#!/bin/bash
"""
Run IoT ML Pipeline
Executes machine learning training and inference
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

# Parse command line arguments
MODE=${1:-train}
DAYS_BACK=${2:-30}

print_status "Running IoT ML Pipeline..."
print_status "Mode: $MODE"
print_status "Training data: last $DAYS_BACK days"

# Check if Spark is running
if ! docker ps | grep -q spark-master; then
    print_warning "Spark is not running. Starting services..."
    docker-compose up -d spark-master spark-worker
    sleep 30
fi

# Check if PostgreSQL is running
if ! docker ps | grep -q postgres; then
    print_warning "PostgreSQL is not running. Starting PostgreSQL..."
    docker-compose up -d postgres
    sleep 10
fi

print_success "All services are running"

# Submit ML pipeline job
print_status "Submitting ML pipeline job to Spark..."

docker exec -it $(docker ps -qf "name=spark-master") spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    --packages org.postgresql:postgresql:42.6.0 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.driver.maxResultSize=1g \
    /opt/spark/jobs/ml/iot_ml_pipeline.py $MODE $DAYS_BACK

print_success "ML pipeline completed"

# Display ML results summary
if [ "$MODE" = "train" ]; then
    print_status "Checking ML training results..."
    
    docker exec -i $(docker ps -qf "name=postgres") psql -U postgres -d iot_analytics -c "
    SELECT 
        'ML Predictions' as table_name,
        prediction_type,
        COUNT(*) as prediction_count,
        AVG(confidence) as avg_confidence,
        MAX(created_at) as last_updated
    FROM ml_predictions
    WHERE created_at >= NOW() - INTERVAL '1 hour'
    GROUP BY prediction_type
    
    UNION ALL
    
    SELECT 
        'Anomaly Detections' as table_name,
        anomaly_type as prediction_type,
        COUNT(*) as prediction_count,
        AVG(anomaly_score) as avg_confidence,
        MAX(created_at) as last_updated
    FROM anomaly_detections
    WHERE created_at >= NOW() - INTERVAL '1 hour'
    GROUP BY anomaly_type;
    "
    
    print_success "ML models trained and predictions generated! 🤖"
    print_status "View ML analytics in Grafana: http://localhost:3000"
    print_status "Dashboard: IoT ML Analytics Dashboard"
    
elif [ "$MODE" = "inference" ]; then
    print_status "Inference mode completed"
    print_success "Real-time predictions generated! 🔮"
fi

echo ""
print_status "Available ML models:"
echo "   • Anomaly Detection (Gaussian Mixture Model)"
echo "   • Predictive Maintenance (Random Forest Regression)"
echo "   • Device Clustering (K-Means)"
echo "   • Failure Prediction (Random Forest Classification)"

echo ""
print_status "To run different modes:"
echo "   ./scripts/run-ml-pipeline.sh train 30     # Train models with 30 days data"
echo "   ./scripts/run-ml-pipeline.sh inference    # Run inference on recent data"

print_success "IoT ML Pipeline completed! 🎉"
