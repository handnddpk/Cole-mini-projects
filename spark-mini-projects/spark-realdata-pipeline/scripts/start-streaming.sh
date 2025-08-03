#!/bin/bash

# Start Streaming Jobs Script
# Starts all Spark streaming jobs for real-time data processing

set -e

echo "🚀 Starting Spark streaming jobs..."

# Check if Spark master is running
if ! curl -s http://localhost:8080 > /dev/null; then
    echo "❌ Spark master is not running. Please start the services first with docker-compose up -d"
    exit 1
fi

# Submit streaming jobs to Spark cluster
echo "📡 Starting Social Media streaming job..."
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0,io.delta:delta-core_2.12:2.4.0 \
    --py-files /opt/bitnami/spark/jobs/streaming/__init__.py \
    /opt/bitnami/spark/jobs/streaming/social_media_stream.py &

echo "💰 Starting Financial streaming job..."
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0,io.delta:delta-core_2.12:2.4.0 \
    --py-files /opt/bitnami/spark/jobs/streaming/__init__.py \
    /opt/bitnami/spark/jobs/streaming/financial_stream.py &

echo "⏳ Waiting for streaming jobs to initialize..."
sleep 30

# Check if jobs are running
echo "🔍 Checking streaming job status..."
curl -s http://localhost:8080/api/v1/applications | jq -r '.[] | select(.name | contains("Stream")) | "\(.name): \(.state)"' || echo "Jobs status check failed - jobs may still be starting"

echo "✅ Streaming jobs started successfully!"
echo ""
echo "📊 Monitor jobs at:"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Spark Applications: http://localhost:8080/api/v1/applications"
echo ""
echo "🛑 To stop streaming jobs, run: ./scripts/stop-streaming.sh"
