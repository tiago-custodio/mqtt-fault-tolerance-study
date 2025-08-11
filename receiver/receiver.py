from flask import Flask, jsonify, request
import paho.mqtt.client as mqtt
from collections import deque
import threading
import time
import csv
from datetime import datetime, timezone
import json

app = Flask(__name__)

# MQTT
MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/data"

# Armazenamento e métricas
message_log = deque(maxlen=10000)
metrics = {
    "received": 0,
    "failed": 0,
    "start_time": time.time(),
    "last_10_latencies": deque(maxlen=10),
    # para cálculo por janela (timestamps de sucessos)
    "success_timestamps": deque(maxlen=200000)  # suficiente pra overload curto
}

CSV_FILE = "mqtt_metrics.csv"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def init_metrics_csv():
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'timestamp', 'message_count', 'failure_count',
            'throughput_total', 'avg_latency_ms', 'failure_rate',
            'message_status'
        ])

def _safe_latency_ms(msg_ts_iso):
    try:
        # aceitar timestamps com/sem timezone; se faltar, tratamos como UTC
        dt = datetime.fromisoformat(msg_ts_iso.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() * 1000.0
    except Exception:
        return 0.0

def calculate_throughput_total():
    elapsed = max(1e-6, time.time() - metrics["start_time"])
    return metrics["received"] / elapsed

def calculate_avg_latency():
    if not metrics["last_10_latencies"]:
        return 0.0
    return sum(metrics["last_10_latencies"]) / len(metrics["last_10_latencies"])

def calculate_window_delivery(window_sec: float):
    """mensagens entregues nos últimos 'window_sec' segundos"""
    cutoff = time.time() - window_sec
    # remove antigos do deque
    while metrics["success_timestamps"] and metrics["success_timestamps"][0] < cutoff:
        metrics["success_timestamps"].popleft()
    return len(metrics["success_timestamps"])

def log_metrics_row(status):
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        failure_rate = metrics["failed"] / max(1, metrics["received"] + metrics["failed"])
        writer.writerow([
            utc_now_iso(),
            metrics["received"],
            metrics["failed"],
            calculate_throughput_total(),
            calculate_avg_latency(),
            failure_rate,
            status
        ])

def on_connect(client, userdata, flags, rc):
    print(f"[Receiver] Connected rc={rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        # se o sender marcou falha simulada
        if data.get("status") == "forced_error":
            raise ValueError("Forced error from sender")

        message_log.append(data)
        metrics["received"] += 1

        # latência (se tivermos timestamp)
        if "timestamp" in data:
            lat = _safe_latency_ms(data["timestamp"])
            metrics["last_10_latencies"].append(lat)

        metrics["success_timestamps"].append(time.time())

        print(f"[Receiver] OK: {data}")
        log_metrics_row("success")

    except Exception as e:
        metrics["failed"] += 1
        message_log.append({"error": str(e), "raw": msg.payload.decode(errors="ignore")})
        print(f"[Receiver] FAIL: {e}")
        log_metrics_row("failed")

def start_mqtt_client():
    client = mqtt.Client(client_id="receiver_app")
    client.on_connect = on_connect
    client.on_message = on_message
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            print(f"[Receiver] MQTT connection error: {e}")
            time.sleep(5)

########################
# REST endpoints
########################

@app.route('/metrics')
def get_metrics():
    # janela (segundos) p/ taxa de entrega no período
    window = float(request.args.get("window", 60))
    delivered_window = calculate_window_delivery(window)
    failure_rate = metrics["failed"] / max(1, metrics["received"] + metrics["failed"])
    return jsonify({
        "total_messages": metrics["received"] + metrics["failed"],
        "successful": metrics["received"],
        "failed": metrics["failed"],
        "throughput_total": calculate_throughput_total(),  # msgs/s desde o início
        "delivered_in_window": delivered_window,           # msgs entregues na janela
        "window_seconds": window,
        "avg_latency_ms_last10": calculate_avg_latency(),
        "failure_rate": failure_rate
    })

@app.route('/messages')
def get_messages():
    n = int(request.args.get("n", 10))
    return jsonify({
        "last_messages": list(message_log)[-n:],
        "total_count": len(message_log)
    })

@app.route('/reset', methods=["POST", "GET"])
def reset():
    message_log.clear()
    metrics["received"] = 0
    metrics["failed"] = 0
    metrics["start_time"] = time.time()
    metrics["last_10_latencies"].clear()
    metrics["success_timestamps"].clear()
    init_metrics_csv()
    return jsonify({"status": "ok", "reset": True})

if __name__ == '__main__':
    init_metrics_csv()
    threading.Thread(target=start_mqtt_client, daemon=True).start()
    app.run(host='0.0.0.0', port=5001)
