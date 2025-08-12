from flask import Flask, jsonify, request
import paho.mqtt.client as mqtt
import json
import random
import time
import threading
from datetime import datetime, timezone

app = Flask(__name__)

# MQTT config
MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/input"  # passa pelo middleware

# Estado do publisher
state = {
    "mode": "none",              # none | intermittent | complete | overload
    "intermittent_rate": 0.3,    # 30%
    "overload_mps": 1000,        # msgs/seg para overload
    "base_mps": 1,               # msgs/seg no modo normal
    "complete_until": 0.0,       # epoch (segundos); >now => falha total ativa
    "running": True,
    "seq": 0
}

# métricas simples do sender
stats = {
    "published": 0,
    "last_error": "",
    "last_publish_time": None
}

# MQTT client
client = mqtt.Client(client_id="sender_app")
client_connected = threading.Event()

def on_connect(c, userdata, flags, rc):
    if rc == 0:
        client_connected.set()
        print("[Sender] MQTT connected")
    else:
        print(f"[Sender] MQTT connect failed rc={rc}")

client.on_connect = on_connect

def mqtt_connect_forever():
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            print(f"[Sender] MQTT connection error: {e}")
            client_connected.clear()
            time.sleep(2)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def generate_sensor_data(seq: int):
    return {
        "seq": seq,
        "device_id": f"device_{random.randint(1,100)}",
        "timestamp": now_iso(),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(40.0, 80.0), 2),
        "status": random.choice(["normal", "warning", "error"])
    }

def should_force_error():
    if time.time() < state["complete_until"]:
        return True
    if state["mode"] == "intermittent":
        return random.random() < state["intermittent_rate"]
    return False

def current_rate_mps():
    if time.time() < state["complete_until"]:
        return state["overload_mps"] if state["mode"] == "overload" else state["base_mps"]
    if state["mode"] == "overload":
        return state["overload_mps"]
    return state["base_mps"]

def publisher_loop():
    while not client_connected.is_set():
        time.sleep(0.2)

    next_due = time.perf_counter()
    while True:
        if not state["running"]:
            time.sleep(0.1)
            continue

        mps = max(0.0001, current_rate_mps())
        interval = 1.0 / mps
        now = time.perf_counter()
        if now < next_due:
            time.sleep(min(0.05, next_due - now))
            continue

        state["seq"] += 1
        data = generate_sensor_data(state["seq"])
        if should_force_error():
            data["status"] = "forced_error"

        payload = json.dumps(data, separators=(",", ":"))

        try:
            result = client.publish(MQTT_TOPIC, payload=payload, qos=1)
            result.wait_for_publish()
            stats["published"] += 1
            stats["last_publish_time"] = now_iso()
            print(f"[Sender] Published (mode={state['mode']}, mps={mps:.1f}): {payload}")
        except Exception as e:
            stats["last_error"] = str(e)
            print(f"[Sender] Publish error: {e}")

        next_due += interval

########################
# REST endpoints
########################

@app.route("/send-one")
def send_one():
    state["seq"] += 1
    data = generate_sensor_data(state["seq"])
    if should_force_error():
        data["status"] = "forced_error"
    try:
        payload = json.dumps(data, separators=(",", ":"))
        result = client.publish(MQTT_TOPIC, payload=payload, qos=1)
        result.wait_for_publish()
        stats["published"] += 1
        stats["last_publish_time"] = now_iso()
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        stats["last_error"] = str(e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/status")
def status():
    return jsonify({
        "mode": state["mode"],
        "intermittent_rate": state["intermittent_rate"],
        "overload_mps": state["overload_mps"],
        "base_mps": state["base_mps"],
        "complete_active": time.time() < state["complete_until"],
        "published": stats["published"],
        "last_publish_time": stats["last_publish_time"],
        "last_error": stats["last_error"],
        "running": state["running"]
    })

@app.route("/start", methods=["POST", "GET"])
def start():
    state["running"] = True
    return jsonify({"status": "ok", "running": True})

@app.route("/stop", methods=["POST", "GET"])
def stop():
    state["running"] = False
    return jsonify({"status": "ok", "running": False})

@app.route("/set-failure-mode/<mode>")
def set_failure_mode(mode):
    valid = ["none", "intermittent", "complete", "overload"]
    if mode not in valid:
        return jsonify({"status": "error", "message": f"invalid mode {mode}"}), 400
    state["mode"] = mode
    if mode != "complete":
        state["complete_until"] = 0.0
    return jsonify({"status": "ok", "mode": mode})

@app.route("/set-rate/<float:mps>")
def set_rate(mps: float):
    if mps <= 0:
        return jsonify({"status": "error", "message": "rate must be > 0"}), 400
    state["base_mps"] = mps
    return jsonify({"status": "ok", "base_mps": state["base_mps"]})

@app.route("/set-intermittent/<float:rate>")
def set_intermittent(rate: float):
    if not (0.0 <= rate <= 1.0):
        return jsonify({"status": "error", "message": "rate must be in [0,1]"}), 400
    state["intermittent_rate"] = rate
    return jsonify({"status": "ok", "intermittent_rate": state["intermittent_rate"]})

@app.route("/set-overload/<int:mps>")
def set_overload(mps: int):
    if mps < 1:
        return jsonify({"status": "error", "message": "mps must be >= 1"}), 400
    state["overload_mps"] = mps
    return jsonify({"status": "ok", "overload_mps": state["overload_mps"]})

@app.route("/scenario/<name>")
def run_scenario(name):
    secs = float(request.args.get("seconds", 60))
    if secs <= 0:
        return jsonify({"status": "error", "message": "seconds must be > 0"}), 400

    if name == "intermittent":
        rate = float(request.args.get("rate", 0.3))
        if not (0.0 <= rate <= 1.0):
            return jsonify({"status": "error", "message": "rate must be in [0,1]"}), 400
        state["mode"] = "intermittent"
        state["intermittent_rate"] = rate
        state["complete_until"] = 0.0
        return jsonify({"status": "ok", "scenario": "intermittent", "rate": rate, "seconds": secs})

    elif name == "complete":
        state["mode"] = "complete"
        state["complete_until"] = time.time() + secs
        return jsonify({"status": "ok", "scenario": "complete", "until": state["complete_until"]})

    elif name == "overload":
        mps = int(request.args.get("mps", 1000))
        if mps < 1:
            return jsonify({"status": "error", "message": "mps must be >= 1"}), 400
        state["mode"] = "overload"
        state["overload_mps"] = mps
        state["complete_until"] = 0.0
        return jsonify({"status": "ok", "scenario": "overload", "mps": mps, "seconds": secs})

    else:
        return jsonify({"status": "error", "message": f"unknown scenario {name}"}), 400

@app.route("/reset-stats", methods=["POST", "GET"])
def reset_stats():
    stats["published"] = 0
    stats["last_error"] = ""
    stats["last_publish_time"] = None
    return jsonify({"status": "ok"})

# >>> NOVO ENDPOINT PARA O COLLECTOR <<<
@app.route("/metrics")
def metrics():
    """
    Endpoint para o collector.py pegar dados do sender.
    Retorna contadores de mensagens e outras métricas.
    """
    return jsonify({
        "total_messages": stats["published"],  # usado pelo collector para delivery_rate
        "sent_messages": stats["published"],   # redundante para compatibilidade
        "last_publish_time": stats["last_publish_time"],
        "last_error": stats["last_error"]
    })

if __name__ == "__main__":
    threading.Thread(target=mqtt_connect_forever, daemon=True).start()
    threading.Thread(target=publisher_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
