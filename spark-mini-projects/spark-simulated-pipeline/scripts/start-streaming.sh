#!/bin/bash
"""
Start IoT Streaming Pipeline
Starts the real-time stream processing with Spark
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

print_status "Starting IoT streaming pipeline..."

# Check if Spark is running
if ! docker ps | grep -q spark-master; then
    print_warning "Spark is not running. Starting services..."
    docker-compose up -d spark-master spark-worker
    sleep 30
fi

# Check if Kafka is running
if ! docker ps | grep -q kafka; then
    print_warning "Kafka is not running. Starting Kafka..."
    docker-compose up -d zookeeper kafka
    ./scripts/wait-for-services.sh
fi

# Check if PostgreSQL is running
if ! docker ps | grep -q postgres; then
    print_warning "PostgreSQL is not running. Starting PostgreSQL..."
    docker-compose up -d postgres
    sleep 10
fi

print_success "All services are running"

# Submit Spark streaming job
print_status "Submitting IoT streaming job to Spark..."

docker exec -it $(docker ps -qf "name=spark-master") spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 2 \
    --num-executors 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    /opt/spark/jobs/streaming/iot_stream_processor.py

print_success "IoT streaming pipeline completed"
