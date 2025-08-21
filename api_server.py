#!/usr/bin/env python3
"""
HTTP API Server for Data Center Sensor Data
Replaces Kafka source for Airbyte Cloud compatibility
"""

from flask import Flask, jsonify
import json
import random
import math
from datetime import datetime, timezone
from kafka import KafkaConsumer
import threading
import time

app = Flask(__name__)

# Store latest sensor data in memory
latest_sensor_data = []
latest_cooling_data = []

class DataCollector:
    def __init__(self):
        self.consumer = None
        self.running = True
        
    def connect_to_kafka(self):
        """Connect to Kafka with retry logic"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries and self.running:
            try:
                self.consumer = KafkaConsumer(
                    'datacenter-sensors',
                    'datacenter-cooling',
                    bootstrap_servers=['localhost:9092'],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    consumer_timeout_ms=1000,
                    auto_offset_reset='latest',
                    group_id='api-server-consumer'
                )
                print("✅ Connected to Kafka successfully")
                return True
            except Exception as e:
                retry_count += 1
                print(f"⚠️  Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(5)
        
        print("❌ Failed to connect to Kafka after multiple attempts")
        return False
        
    def collect_data(self):
        """Collect data from Kafka and store in memory for API"""
        global latest_sensor_data, latest_cooling_data
        
        if not self.connect_to_kafka():
            return
            
        print("🔄 Starting data collection from Kafka...")
        
        while self.running:
            try:
                # Consume messages from Kafka
                for message in self.consumer:
                    if not self.running:
                        break
                        
                    if message.topic == 'datacenter-sensors':
                        latest_sensor_data.append(message.value)
                        # Keep only last 100 readings
                        if len(latest_sensor_data) > 100:
                            latest_sensor_data = latest_sensor_data[-100:]
                        print(f"📊 Collected sensor reading from {message.value.get('device_id')}")
                    
                    elif message.topic == 'datacenter-cooling':
                        latest_cooling_data.append(message.value)
                        # Keep only last 50 readings
                        if len(latest_cooling_data) > 50:
                            latest_cooling_data = latest_cooling_data[-50:]
                        print(f"🏭 Collected cooling data from {message.value.get('system_id')}")
                            
            except Exception as e:
                print(f"Kafka consumer error: {e}")
                time.sleep(5)  # Wait before retrying
                
                # Try to reconnect
                if not self.connect_to_kafka():
                    break

# API Endpoints
@app.route('/')
def home():
    """Home page with API documentation"""
    return jsonify({
        "service": "Data Center API Server",
        "version": "1.0.0",
        "description": "REST API for data center sensor and cooling system data",
        "endpoints": {
            "health": "/health",
            "all_sensors": "/api/v1/sensors",
            "temperature_sensors": "/api/v1/sensors/temperature",
            "humidity_sensors": "/api/v1/sensors/humidity",
            "cooling_systems": "/api/v1/cooling",
            "summary_stats": "/api/v1/summary",
            "airbyte_sensors": "/api/airbyte/sensors",
            "airbyte_cooling": "/api/airbyte/cooling"
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sensor_readings_count": len(latest_sensor_data),
        "cooling_readings_count": len(latest_cooling_data),
        "last_sensor_update": latest_sensor_data[-1].get('timestamp') if latest_sensor_data else None,
        "last_cooling_update": latest_cooling_data[-1].get('timestamp') if latest_cooling_data else None
    })

@app.route('/api/v1/sensors')
def get_sensors():
    """Get all sensor readings"""
    return jsonify({
        "data": latest_sensor_data,
        "count": len(latest_sensor_data),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/api/v1/sensors/temperature')
def get_temperature_sensors():
    """Get only temperature sensor readings"""
    temp_data = [
        reading for reading in latest_sensor_data 
        if reading.get('measurement_type') == 'temperature'
    ]
    return jsonify({
        "data": temp_data,
        "count": len(temp_data),
        "measurement_type": "temperature",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/api/v1/sensors/humidity')
def get_humidity_sensors():
    """Get only humidity sensor readings"""
    humidity_data = [
        reading for reading in latest_sensor_data 
        if reading.get('measurement_type') == 'humidity'
    ]
    return jsonify({
        "data": humidity_data,
        "count": len(humidity_data),
        "measurement_type": "humidity",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/api/v1/cooling')
def get_cooling():
    """Get cooling system data"""
    return jsonify({
        "data": latest_cooling_data,
        "count": len(latest_cooling_data),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/api/v1/summary')
def get_summary():
    """Get summary statistics"""
    # Calculate averages for latest readings
    recent_temps = [
        r['value'] for r in latest_sensor_data[-20:] 
        if r.get('measurement_type') == 'temperature'
    ]
    recent_humidity = [
        r['value'] for r in latest_sensor_data[-20:] 
        if r.get('measurement_type') == 'humidity'
    ]
    recent_cooling_efficiency = [
        r['efficiency_percent'] for r in latest_cooling_data[-10:]
    ]
    
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "statistics": {
            "average_temperature": round(sum(recent_temps) / len(recent_temps), 2) if recent_temps else None,
            "min_temperature": round(min(recent_temps), 2) if recent_temps else None,
            "max_temperature": round(max(recent_temps), 2) if recent_temps else None,
            "average_humidity": round(sum(recent_humidity) / len(recent_humidity), 2) if recent_humidity else None,
            "average_cooling_efficiency": round(sum(recent_cooling_efficiency) / len(recent_cooling_efficiency), 2) if recent_cooling_efficiency else None
        },
        "counts": {
            "total_sensor_readings": len(latest_sensor_data),
            "total_cooling_readings": len(latest_cooling_data),
            "temperature_readings": len(recent_temps),
            "humidity_readings": len(recent_humidity)
        },
        "data_quality": {
            "sensors_active": len(set(r.get('device_id') for r in latest_sensor_data[-20:])) if latest_sensor_data else 0,
            "cooling_systems_active": len(set(r.get('system_id') for r in latest_cooling_data[-10:])) if latest_cooling_data else 0,
            "last_update": max([r.get('timestamp') for r in (latest_sensor_data[-5:] + latest_cooling_data[-5:])] or [None])
        }
    }
    
    return jsonify(summary)

# Airbyte-compatible endpoints (flat structure for easier consumption)
@app.route('/api/airbyte/sensors')
def get_sensors_airbyte():
    """Airbyte-compatible endpoint with flat structure"""
    # Flatten the data structure for easier Airbyte consumption
    flattened_data = []
    
    for reading in latest_sensor_data:
        flattened_data.append({
            "id": f"{reading.get('device_id')}_{reading.get('timestamp')}".replace(':', '_').replace('+', '_'),
            "timestamp": reading.get('timestamp'),
            "device_id": reading.get('device_id'),
            "device_type": reading.get('device_type'),
            "location": reading.get('location'),
            "rack_id": reading.get('rack_id'),
            "measurement_type": reading.get('measurement_type'),
            "value": reading.get('value'),
            "unit": reading.get('unit'),
            "status": reading.get('status'),
            "data_source": "datacenter_sensors"
        })
    
    return jsonify(flattened_data)

@app.route('/api/airbyte/cooling')
def get_cooling_airbyte():
    """Airbyte-compatible cooling data endpoint"""
    flattened_data = []
    
    for reading in latest_cooling_data:
        flattened_data.append({
            "id": f"{reading.get('system_id')}_{reading.get('timestamp')}".replace(':', '_').replace('+', '_'),
            "timestamp": reading.get('timestamp'),
            "system_id": reading.get('system_id'),
            "system_type": reading.get('system_type'),
            "location": reading.get('location'),
            "temperature_setpoint": reading.get('temperature_setpoint'),
            "actual_temperature": reading.get('actual_temperature'),
            "efficiency_percent": reading.get('efficiency_percent'),
            "power_consumption_watts": reading.get('power_consumption_watts'),
            "load_factor": reading.get('load_factor'),
            "status": reading.get('status'),
            "data_source": "cooling_systems"
        })
    
    return jsonify(flattened_data)

# Combined endpoint for Airbyte (all data in one call)
@app.route('/api/airbyte/all')
def get_all_airbyte():
    """Combined endpoint with all data for Airbyte"""
    # Get sensor data
    sensor_data = []
    for reading in latest_sensor_data:
        sensor_data.append({
            "id": f"sensor_{reading.get('device_id')}_{reading.get('timestamp')}".replace(':', '_').replace('+', '_'),
            "timestamp": reading.get('timestamp'),
            "source_type": "sensor",
            "device_id": reading.get('device_id'),
            "device_type": reading.get('device_type'),
            "location": reading.get('location'),
            "rack_id": reading.get('rack_id'),
            "measurement_type": reading.get('measurement_type'),
            "value": reading.get('value'),
            "unit": reading.get('unit'),
            "status": reading.get('status')
        })
    
    # Get cooling data
    cooling_data = []
    for reading in latest_cooling_data:
        cooling_data.append({
            "id": f"cooling_{reading.get('system_id')}_{reading.get('timestamp')}".replace(':', '_').replace('+', '_'),
            "timestamp": reading.get('timestamp'),
            "source_type": "cooling",
            "system_id": reading.get('system_id'),
            "system_type": reading.get('system_type'),
            "location": reading.get('location'),
            "temperature_setpoint": reading.get('temperature_setpoint'),
            "actual_temperature": reading.get('actual_temperature'),
            "efficiency_percent": reading.get('efficiency_percent'),
            "power_consumption_watts": reading.get('power_consumption_watts'),
            "status": reading.get('status')
        })
    
    # Combine all data
    all_data = sensor_data + cooling_data
    
    return jsonify(all_data)

if __name__ == '__main__':
    print("🚀 Starting Data Center API Server...")
    print("📊 Endpoints available:")
    print("   Home: http://localhost:5001/")
    print("   Health: http://localhost:5001/health")
    print("   Sensors: http://localhost:5001/api/v1/sensors")
    print("   Temperature: http://localhost:5001/api/v1/sensors/temperature")
    print("   Humidity: http://localhost:5001/api/v1/sensors/humidity")
    print("   Cooling: http://localhost:5001/api/v1/cooling")
    print("   Summary: http://localhost:5001/api/v1/summary")
    print("   Airbyte Sensors: http://localhost:5001/api/airbyte/sensors")
    print("   Airbyte Cooling: http://localhost:5001/api/airbyte/cooling")
    print("   Airbyte All: http://localhost:5001/api/airbyte/all")
    print("")
    
    # Start data collector in background thread
    collector = DataCollector()
    collector_thread = threading.Thread(target=collector.collect_data)
    collector_thread.daemon = True
    collector_thread.start()
    
    print("🔄 Starting background data collection from Kafka...")
    print("💡 Use Ctrl+C to stop the server")
    print("")
    
    # Run Flask app
    try:
        app.run(host='0.0.0.0', port=5001, debug=False)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down server...")
        collector.running = False