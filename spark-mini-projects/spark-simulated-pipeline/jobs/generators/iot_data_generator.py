#!/usr/bin/env python3
"""
IoT Data Generator - Simulates realistic IoT device data streams
Generates temperature, humidity, pressure, and other sensor readings
"""

import json
import time
import random
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any
import uuid
from kafka import KafkaProducer
from dataclasses import dataclass
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class DeviceConfig:
    device_id: str
    device_type: str
    location: str
    base_temperature: float
    base_humidity: float
    base_pressure: float
    battery_level: int
    signal_strength: int

class IoTDeviceSimulator:
    def __init__(self, config: DeviceConfig):
        self.config = config
        self.is_running = False
        self.anomaly_probability = 0.05  # 5% chance of anomaly
        
    def generate_sensor_reading(self) -> Dict[str, Any]:
        """Generate a realistic sensor reading with potential anomalies"""
        timestamp = datetime.utcnow().isoformat()
        
        # Add some realistic variation and potential anomalies
        temp_variation = random.gauss(0, 2)  # Normal variation ±2°C
        humidity_variation = random.gauss(0, 5)  # Normal variation ±5%
        pressure_variation = random.gauss(0, 10)  # Normal variation ±10 hPa
        
        # Introduce anomalies occasionally
        if random.random() < self.anomaly_probability:
            temp_variation += random.choice([-15, 15])  # Temperature spike/drop
            
        # Calculate values with bounds checking
        temperature = max(-40, min(80, self.config.base_temperature + temp_variation))
        humidity = max(0, min(100, self.config.base_humidity + humidity_variation))
        pressure = max(800, min(1200, self.config.base_pressure + pressure_variation))
        
        # Battery degradation simulation
        if random.random() < 0.01:  # 1% chance of battery drain
            self.config.battery_level = max(0, self.config.battery_level - 1)
            
        # Signal strength variation
        signal_variation = random.randint(-5, 5)
        signal_strength = max(0, min(100, self.config.signal_strength + signal_variation))
        
        reading = {
            "device_id": self.config.device_id,
            "device_type": self.config.device_type,
            "location": self.config.location,
            "timestamp": timestamp,
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2),
            "pressure": round(pressure, 2),
            "battery_level": self.config.battery_level,
            "signal_strength": signal_strength,
            "message_id": str(uuid.uuid4())
        }
        
        return reading

class IoTDataGenerator:
    def __init__(self, kafka_brokers: str = "localhost:9092"):
        self.kafka_brokers = kafka_brokers
        self.producer = None
        self.devices: List[IoTDeviceSimulator] = []
        self.is_running = False
        
    def initialize_kafka_producer(self):
        """Initialize Kafka producer with error handling"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=3,
                batch_size=16384,
                linger_ms=100,
                buffer_memory=33554432
            )
            print(f"✅ Kafka producer initialized: {self.kafka_brokers}")
        except Exception as e:
            print(f"❌ Failed to initialize Kafka producer: {e}")
            raise
            
    def create_devices(self, count: int = 100) -> List[DeviceConfig]:
        """Create simulated IoT devices with realistic configurations"""
        device_types = [
            "temperature_sensor", "humidity_sensor", "pressure_sensor", 
            "multi_sensor", "environmental_sensor", "weather_station"
        ]
        
        locations = [
            "Building A - Floor 1", "Building A - Floor 2", "Building A - Floor 3",
            "Building B - Floor 1", "Building B - Floor 2", "Building B - Basement",
            "Warehouse Section 1", "Warehouse Section 2", "Warehouse Section 3",
            "Outdoor Station 1", "Outdoor Station 2", "Parking Lot A", "Parking Lot B"
        ]
        
        devices = []
        for i in range(count):
            device_config = DeviceConfig(
                device_id=f"device_{i+1:03d}",
                device_type=random.choice(device_types),
                location=random.choice(locations),
                base_temperature=random.uniform(18, 25),  # Base temp 18-25°C
                base_humidity=random.uniform(40, 70),     # Base humidity 40-70%
                base_pressure=random.uniform(1000, 1020), # Base pressure 1000-1020 hPa
                battery_level=random.randint(70, 100),    # Battery 70-100%
                signal_strength=random.randint(60, 95)    # Signal 60-95%
            )
            devices.append(device_config)
            
        return devices
        
    def setup_devices(self, device_count: int = 100):
        """Setup all IoT device simulators"""
        print(f"🔧 Setting up {device_count} IoT devices...")
        device_configs = self.create_devices(device_count)
        
        for config in device_configs:
            simulator = IoTDeviceSimulator(config)
            self.devices.append(simulator)
            
        print(f"✅ Created {len(self.devices)} IoT device simulators")
        
    def simulate_device_data(self, device: IoTDeviceSimulator, topic: str, interval: float):
        """Simulate data for a single device"""
        while self.is_running:
            try:
                reading = device.generate_sensor_reading()
                
                # Send to Kafka
                if self.producer:
                    future = self.producer.send(
                        topic, 
                        key=device.config.device_id,
                        value=reading
                    )
                    # Optional: wait for confirmation
                    # future.get(timeout=10)
                    
                # Print occasional updates
                if random.random() < 0.01:  # 1% of messages
                    print(f"📊 {device.config.device_id}: T={reading['temperature']}°C, "
                          f"H={reading['humidity']}%, P={reading['pressure']}hPa")
                    
            except Exception as e:
                print(f"❌ Error generating data for {device.config.device_id}: {e}")
                
            time.sleep(interval)
            
    def start_simulation(self, topic: str = "iot-sensors", 
                        interval: float = 1.0, device_count: int = 100):
        """Start the IoT data simulation"""
        print(f"🚀 Starting IoT data simulation...")
        print(f"   📡 Topic: {topic}")
        print(f"   ⏱️  Interval: {interval}s")
        print(f"   📱 Devices: {device_count}")
        
        # Initialize components
        self.initialize_kafka_producer()
        self.setup_devices(device_count)
        
        self.is_running = True
        threads = []
        
        # Start a thread for each device
        for device in self.devices:
            thread = threading.Thread(
                target=self.simulate_device_data,
                args=(device, topic, interval),
                daemon=True
            )
            thread.start()
            threads.append(thread)
            
        print(f"✅ Started {len(threads)} device simulation threads")
        
        try:
            # Keep main thread alive
            while self.is_running:
                time.sleep(5)
                # Print status every 30 seconds
                if int(time.time()) % 30 == 0:
                    print(f"📈 Simulation running... {len(self.devices)} devices active")
                    
        except KeyboardInterrupt:
            print("\n🛑 Stopping simulation...")
            self.stop_simulation()
            
    def stop_simulation(self):
        """Stop the IoT data simulation"""
        self.is_running = False
        if self.producer:
            self.producer.flush()
            self.producer.close()
        print("✅ IoT simulation stopped")
        
    def generate_batch_data(self, topic: str = "iot-sensors", 
                           count: int = 1000, device_count: int = 50):
        """Generate a batch of historical data"""
        print(f"📦 Generating batch data: {count} records from {device_count} devices")
        
        self.initialize_kafka_producer()
        self.setup_devices(device_count)
        
        start_time = datetime.utcnow() - timedelta(hours=24)  # Last 24 hours
        
        for i in range(count):
            device = random.choice(self.devices)
            
            # Generate timestamp in the past 24 hours
            timestamp_offset = random.uniform(0, 24 * 3600)  # Random time in last 24h
            timestamp = start_time + timedelta(seconds=timestamp_offset)
            
            reading = device.generate_sensor_reading()
            reading['timestamp'] = timestamp.isoformat()
            
            # Send to Kafka
            self.producer.send(
                topic,
                key=device.config.device_id,
                value=reading
            )
            
            if (i + 1) % 100 == 0:
                print(f"📊 Generated {i + 1}/{count} records")
                
        self.producer.flush()
        print(f"✅ Batch data generation complete: {count} records sent")

def main():
    """Main function to run the IoT data generator"""
    # Load configuration
    kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')
    device_count = int(os.getenv('IOT_DEVICES_COUNT', '100'))
    interval = float(os.getenv('SIMULATION_INTERVAL_MS', '1000')) / 1000.0
    
    # Create generator
    generator = IoTDataGenerator(kafka_brokers)
    
    # Check if we should generate batch data or start streaming
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--batch':
        batch_count = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
        generator.generate_batch_data(count=batch_count, device_count=device_count)
    else:
        # Start streaming simulation
        generator.start_simulation(
            topic="iot-sensors",
            interval=interval,
            device_count=device_count
        )

if __name__ == "__main__":
    main()
