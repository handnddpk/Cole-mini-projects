#!/bin/bash

# Start Data Ingestion Script
# Starts the data ingestion process from external APIs

set -e

echo "📡 Starting data ingestion process..."

# Check if Kafka is running
if ! docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "❌ Kafka is not running. Please start the services first."
    exit 1
fi

# Check if required environment variables are set
if [ ! -f .env ]; then
    echo "❌ .env file not found. Please run setup.sh first."
    exit 1
fi

# Source environment variables
set -a
source .env
set +a

# Start data ingestion in background
echo "🔄 Starting continuous data ingestion..."
docker-compose exec -d jupyter python /home/jovyan/jobs/ingestion/data_collector.py

# Start one-time data collection for immediate results
echo "📥 Running initial data collection..."
docker-compose exec jupyter python /home/jovyan/jobs/ingestion/data_collector.py --once

echo "✅ Data ingestion started successfully!"
echo ""
echo "📊 Monitor ingestion:"
echo "  - Check Kafka topics: docker-compose exec kafka kafka-console-consumer --topic twitter_stream --bootstrap-server localhost:9092"
echo "  - View logs: docker-compose logs jupyter"
echo ""
echo "🛑 To stop ingestion, run: docker-compose exec jupyter pkill -f data_collector.py"
