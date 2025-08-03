#!/bin/bash

# Run ML Pipeline Script
# Executes the machine learning pipeline for model training and inference

set -e

echo "🤖 Running ML Pipeline..."

# Check if Spark is running
if ! curl -s http://localhost:8080 > /dev/null; then
    echo "❌ Spark master is not running. Please start the services first."
    exit 1
fi

# Parse command line arguments
RETRAIN=false
MODEL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --retrain)
            RETRAIN=true
            shift
            ;;
        --model)
            MODEL="$2"
            shift 2
            ;;
        *)
            echo "Unknown option $1"
            echo "Usage: $0 [--retrain] [--model sentiment|trend|clustering|anomaly]"
            exit 1
            ;;
    esac
done

# Build spark-submit command
SPARK_SUBMIT_CMD="spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    --packages org.postgresql:postgresql:42.7.0,io.delta:delta-core_2.12:2.4.0 \
    /opt/bitnami/spark/jobs/ml/ml_pipeline.py"

# Add arguments
if [ "$RETRAIN" = true ]; then
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --retrain"
fi

if [ -n "$MODEL" ]; then
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --model $MODEL"
fi

echo "🔧 Executing ML pipeline with Spark..."
docker-compose exec spark-master bash -c "$SPARK_SUBMIT_CMD"

echo "✅ ML Pipeline completed successfully!"
echo ""
echo "📊 Check results:"
echo "  - Model artifacts: /tmp/models/"
echo "  - ML insights: Check PostgreSQL ml_insights table"
echo "  - Anomalies: Check PostgreSQL ml_detected_anomalies table"
