from flask import Flask, jsonify
import paho.mqtt.publish as publish
import random
import time
import threading
from datetime import datetime
import json  # <-- Import para serialização JSON

app = Flask(__name__)

# MQTT Configuration
MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/input"  # Alterado para passar pelo middleware

# Failure simulation controls
current_failure_mode = "none"  # none, intermittent, complete, overload
failure_params = {
    "intermittent_rate": 0.3,
    "complete_duration": 60,
    "overload_rate": 0.001  # 1000 msg/s
}

def generate_sensor_data():
    """Generate simulated IoT sensor data"""
    return {
        "device_id": f"device_{random.randint(1, 100)}",
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(40.0, 80.0), 2),
        "status": random.choice(["normal", "warning", "error"])
    }

def controlled_publish():
    """Publish with controlled failure modes"""
    while True:
        try:
            data = generate_sensor_data()
            
            # Apply failure modes
            if current_failure_mode == "intermittent" and random.random() < failure_params["intermittent_rate"]:
                data["status"] = "forced_error"
            
            publish.single(
                MQTT_TOPIC,
                payload=json.dumps(data),  # <-- Envia JSON válido
                hostname=MQTT_BROKER,
                port=MQTT_PORT
            )
            
            print(f"Published: {data}")
            
            # Handle overload mode
            delay = failure_params["overload_rate"] if current_failure_mode == "overload" else 1.0
            time.sleep(delay)
            
        except Exception as e:
            print(f"Publishing error: {e}")
            time.sleep(1)

@app.route('/send-one')
def send_one():
    """Send single message endpoint"""
    data = generate_sensor_data()
    try:
        publish.single(
            MQTT_TOPIC,
            payload=json.dumps(data),  # <-- Envia JSON válido
            hostname=MQTT_BROKER,
            port=MQTT_PORT
        )
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/set-failure-mode/<mode>')
def set_failure_mode(mode):
    """Set failure simulation mode"""
    global current_failure_mode
    valid_modes = ["none", "intermittent", "complete", "overload"]
    
    if mode not in valid_modes:
        return jsonify({"status": "error", "message": "Invalid mode"}), 400
    
    current_failure_mode = mode
    response = {
        "status": "success",
        "new_mode": current_failure_mode,
        "params": failure_params
    }
    
    # Handle complete failure mode
    if mode == "complete":
        response["duration"] = failure_params["complete_duration"]
    
    return jsonify(response)

@app.route('/set-failure-param/<param>/<value>')
def set_failure_param(param, value):
    """Adjust failure parameters"""
    try:
        value = float(value)
        if param in failure_params:
            failure_params[param] = value
            return jsonify({
                "status": "success",
                "parameter": param,
                "new_value": value
            })
        return jsonify({"status": "error", "message": "Invalid parameter"}), 400
    except ValueError:
        return jsonify({"status": "error", "message": "Invalid value"}), 400

if __name__ == '__main__':
    # Start continuous publishing thread
    threading.Thread(target=controlled_publish, daemon=True).start()
    # Start Flask server
    app.run(host='0.0.0.0', port=5000)
