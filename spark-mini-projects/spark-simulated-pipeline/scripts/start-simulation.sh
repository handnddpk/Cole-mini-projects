#!/bin/bash
"""
Start IoT Data Simulation
Generates continuous simulated IoT sensor data
"""

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Load environment variables
if [ -f .env ]; then
    source .env
fi

# Default values
DEVICE_COUNT=${IOT_DEVICES_COUNT:-100}
INTERVAL_MS=${SIMULATION_INTERVAL_MS:-1000}
KAFKA_TOPIC=${KAFKA_TOPIC:-iot-sensors}

print_status "Starting IoT data simulation..."
print_status "Device count: $DEVICE_COUNT"
print_status "Interval: ${INTERVAL_MS}ms"
print_status "Kafka topic: $KAFKA_TOPIC"

# Check if Kafka is running
if ! docker ps | grep -q kafka; then
    print_warning "Kafka container is not running. Starting infrastructure..."
    docker-compose up -d zookeeper kafka postgres
    ./scripts/wait-for-services.sh
fi

# Check if virtual environment exists
if [ -d "venv" ]; then
    print_status "Activating virtual environment..."
    source venv/bin/activate
fi

# Create Kafka topic if it doesn't exist
print_status "Creating Kafka topic: $KAFKA_TOPIC"
docker exec -it $(docker ps -qf "name=kafka") kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic $KAFKA_TOPIC \
    --partitions 4 \
    --replication-factor 1

print_success "Kafka topic ready"

# Start the data generator
print_status "Starting IoT data generator..."
print_status "Press Ctrl+C to stop the simulation"

cd jobs/generators
python3 iot_data_generator.py

print_success "IoT data simulation stopped"
