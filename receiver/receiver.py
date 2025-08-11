from flask import Flask, jsonify
import paho.mqtt.client as mqtt
from collections import deque
import threading
import time
import csv
from datetime import datetime

app = Flask(__name__)

# MQTT Configuration
MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/data"

# Metrics storage
message_log = deque(maxlen=10000)
metrics = {
    "received": 0,
    "failed": 0,
    "start_time": time.time(),
    "last_10_latencies": deque(maxlen=10)
}

# CSV Setup
def init_metrics_csv():
    with open('mqtt_metrics.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'timestamp', 'message_count', 'failure_count',
            'throughput_1min', 'avg_latency_ms', 'failure_rate',
            'message_status'
        ])

def log_metrics(message, status):
    """Log message and update metrics"""
    timestamp = datetime.now().isoformat()
    metrics["received" if status == "success" else "failed"] += 1
    metrics["last_10_latencies"].append(time.time() - datetime.fromisoformat(message["timestamp"]).timestamp())
    
    with open('mqtt_metrics.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            timestamp,
            metrics["received"],
            metrics["failed"],
            calculate_throughput(),
            calculate_avg_latency(),
            metrics["failed"] / max(1, metrics["received"] + metrics["failed"]),
            status
        ])

def calculate_throughput(window_seconds=60):
    """Calculate messages per second"""
    elapsed = time.time() - metrics["start_time"]
    return metrics["received"] / elapsed if elapsed > 0 else 0

def calculate_avg_latency():
    """Calculate average latency of last 10 messages"""
    if not metrics["last_10_latencies"]:
        return 0
    return sum(metrics["last_10_latencies"]) * 1000 / len(metrics["last_10_latencies"])

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        message = eval(msg.payload.decode())
        if message["status"] == "forced_error":
            raise ValueError("Forced error from sender")
        
        message_log.append(message)
        log_metrics(message, "success")
        print(f"Received valid message: {message}")
        
    except Exception as e:
        message_log.append({"error": str(e), "raw": msg.payload.decode()})
        log_metrics({"timestamp": datetime.now().isoformat()}, "failed")
        print(f"Message processing failed: {e}")

@app.route('/metrics')
def get_metrics():
    """Get current metrics"""
    return jsonify({
        "total_messages": metrics["received"] + metrics["failed"],
        "successful": metrics["received"],
        "failed": metrics["failed"],
        "throughput_1min": calculate_throughput(),
        "avg_latency_ms": calculate_avg_latency(),
        "failure_rate": metrics["failed"] / max(1, metrics["received"] + metrics["failed"])
    })

@app.route('/messages')
def get_messages():
    """Get recent messages"""
    return jsonify({
        "last_10_messages": list(message_log)[-10:],
        "total_count": len(message_log)
    })

def start_mqtt_client():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            print(f"MQTT connection error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    init_metrics_csv()
    # Start MQTT client thread
    threading.Thread(target=start_mqtt_client, daemon=True).start()
    # Start Flask server
    app.run(host='0.0.0.0', port=5001)