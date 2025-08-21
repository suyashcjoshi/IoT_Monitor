#!/usr/bin/env python3
"""
Data Center Environmental Simulator
Generates realistic sensor data for temperature, humidity, and cooling systems
"""

import json
import time
import random
import math
from datetime import datetime, timezone
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCenterSimulator:
    def __init__(self, kafka_servers=['kafka:29092']):
        # Initialize Kafka producer with retry logic
        self.producer = None
        self.kafka_servers = kafka_servers
        self.connect_to_kafka()
        
        # Data center configuration
        self.server_rooms = {
            'Server Room A': {
                'base_temp': 23.0,
                'base_humidity': 45.0,
                'racks': ['rack-a1', 'rack-a2', 'rack-a3', 'rack-a4']
            },
            'Server Room B': {
                'base_temp': 22.5,
                'base_humidity': 42.0,
                'racks': ['rack-b1', 'rack-b2', 'rack-b3']
            }
        }
        
        self.cooling_systems = [
            {
                'id': 'hvac_primary_a',
                'type': 'primary_cooling',
                'location': 'Server Room A',
                'target_temp': 22.0,
                'max_power': 20000,
                'efficiency_range': (85, 95)
            },
            {
                'id': 'hvac_secondary_a', 
                'type': 'secondary_cooling',
                'location': 'Server Room A',
                'target_temp': 22.5,
                'max_power': 15000,
                'efficiency_range': (80, 90)
            },
            {
                'id': 'hvac_primary_b',
                'type': 'primary_cooling', 
                'location': 'Server Room B',
                'target_temp': 21.5,
                'max_power': 18000,
                'efficiency_range': (87, 93)
            }
        ]
        
        # Simulation state
        self.time_offset = 0
        
    def connect_to_kafka(self):
        """Connect to Kafka with retry logic"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks=1,
                    retries=3
                )
                logger.info("✅ Connected to Kafka successfully")
                return
            except Exception as e:
                retry_count += 1
                logger.warning(f"⚠️  Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(5)
        
        raise Exception("Failed to connect to Kafka after multiple attempts")
    
    def generate_temperature_reading(self, room, rack_id):
        """Generate realistic temperature readings with some variation"""
        base_temp = self.server_rooms[room]['base_temp']
        
        # Add some realistic variation
        daily_cycle = 2.0 * math.sin(2 * math.pi * self.time_offset / (24 * 3600))  # Daily temperature cycle
        random_variation = random.gauss(0, 0.8)  # Random noise
        
        # Some racks run hotter (higher density)
        rack_factor = 1.5 if 'a1' in rack_id or 'b1' in rack_id else 0
        
        temperature = base_temp + daily_cycle + random_variation + rack_factor
        temperature = max(18.0, min(35.0, temperature))  # Realistic bounds
        
        return round(temperature, 2)
    
    def generate_humidity_reading(self, room):
        """Generate realistic humidity readings"""
        base_humidity = self.server_rooms[room]['base_humidity']
        
        # Humidity varies less than temperature
        variation = random.gauss(0, 3.0)
        humidity = base_humidity + variation
        humidity = max(20.0, min(80.0, humidity))  # Realistic bounds
        
        return round(humidity, 1)
    
    def generate_cooling_system_data(self, system):
        """Generate cooling system performance data"""
        # Calculate load based on "outside temperature" simulation
        outside_temp_factor = 15 + 10 * math.sin(2 * math.pi * self.time_offset / (24 * 3600))
        load_factor = min(1.0, (outside_temp_factor - 10) / 20)
        
        # Efficiency decreases with higher load
        base_efficiency = random.uniform(*system['efficiency_range'])
        efficiency = base_efficiency - (load_factor * 5)  # 5% efficiency loss at max load
        
        # Power consumption increases with load
        power_consumption = system['max_power'] * (0.3 + 0.7 * load_factor)  # 30% minimum power
        
        # Actual temperature vs setpoint
        temp_deviation = random.gauss(0, 0.5)
        actual_temp = system['target_temp'] + temp_deviation
        
        return {
            'system_id': system['id'],
            'system_type': system['type'],
            'location': system['location'],
            'temperature_setpoint': system['target_temp'],
            'actual_temperature': round(actual_temp, 2),
            'efficiency_percent': round(efficiency, 1),
            'power_consumption_watts': round(power_consumption, 0),
            'load_factor': round(load_factor, 2),
            'status': 'running'
        }
    
    def send_sensor_data(self):
        """Generate and send sensor readings to Kafka"""
        timestamp = datetime.now(timezone.utc).isoformat()
        
        # Generate temperature and humidity readings for each rack
        for room, config in self.server_rooms.items():
            for rack_id in config['racks']:
                # Temperature sensor
                temp_data = {
                    'timestamp': timestamp,
                    'device_id': f'temp_{rack_id}',
                    'device_type': 'temperature_sensor',
                    'location': room,
                    'rack_id': rack_id,
                    'measurement_type': 'temperature',
                    'value': self.generate_temperature_reading(room, rack_id),
                    'unit': 'celsius',
                    'status': 'active'
                }
                
                # Send to Kafka
                self.producer.send('datacenter-sensors', key=temp_data['device_id'], value=temp_data)
                
            # Humidity sensor (one per room)
            humidity_data = {
                'timestamp': timestamp,
                'device_id': f'humidity_{room.lower().replace(" ", "_")}',
                'device_type': 'humidity_sensor', 
                'location': room,
                'rack_id': None,
                'measurement_type': 'humidity',
                'value': self.generate_humidity_reading(room),
                'unit': 'percent',
                'status': 'active'
            }
            
            self.producer.send('datacenter-sensors', key=humidity_data['device_id'], value=humidity_data)
    
    def send_cooling_data(self):
        """Generate and send cooling system data to Kafka"""
        timestamp = datetime.now(timezone.utc).isoformat()
        
        for system in self.cooling_systems:
            cooling_data = self.generate_cooling_system_data(system)
            cooling_data['timestamp'] = timestamp
            
            # Send to Kafka
            self.producer.send('datacenter-cooling', key=cooling_data['system_id'], value=cooling_data)
    
    def run(self):
        """Main simulation loop"""
        logger.info("🏢 Starting Data Center Environmental Simulator...")
        logger.info(f"📊 Monitoring {len(self.cooling_systems)} cooling systems")
        
        total_racks = sum(len(config['racks']) for config in self.server_rooms.values())
        logger.info(f"🌡️  Simulating {total_racks} temperature sensors")
        logger.info(f"💧 Simulating {len(self.server_rooms)} humidity sensors")
        
        try:
            while True:
                start_time = time.time()
                
                # Generate and send data
                self.send_sensor_data()
                self.send_cooling_data()
                
                # Flush producer to ensure messages are sent
                self.producer.flush(timeout=5)
                
                # Update simulation time
                self.time_offset += 30  # 30 seconds per iteration
                
                # Log progress
                if self.time_offset % 300 == 0:  # Every 5 minutes of sim time
                    logger.info(f"📈 Generated data for simulation time +{self.time_offset//60} minutes")
                
                # Wait for next cycle (30 seconds real time)
                elapsed = time.time() - start_time
                sleep_time = max(0, 30 - elapsed)
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("🛑 Simulator stopped by user")
        except Exception as e:
            logger.error(f"❌ Simulator error: {e}")
        finally:
            if self.producer:
                self.producer.close()

if __name__ == "__main__":
    simulator = DataCenterSimulator()
    simulator.run()
