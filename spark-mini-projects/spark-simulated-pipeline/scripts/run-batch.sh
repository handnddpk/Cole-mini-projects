#!/bin/bash
"""
Run IoT Batch Processing
Executes daily batch analytics and aggregations
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
TARGET_DATE=${1:-$(date -d 'yesterday' +%Y-%m-%d)}
MODE=${2:-daily}

print_status "Running IoT batch processing..."
print_status "Target date: $TARGET_DATE"
print_status "Mode: $MODE"

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

# Submit batch processing job
print_status "Submitting batch processing job to Spark..."

if [ "$MODE" = "historical" ]; then
    DAYS_BACK=${3:-7}
    print_status "Running historical analysis for last $DAYS_BACK days"
    
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
        /opt/spark/jobs/batch/iot_batch_processor.py historical $DAYS_BACK
else
    print_status "Running daily batch processing for $TARGET_DATE"
    
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
        /opt/spark/jobs/batch/iot_batch_processor.py daily $TARGET_DATE
fi

print_success "Batch processing completed"

# Display results summary
print_status "Checking results..."

# Query the database for summary
docker exec -i $(docker ps -qf "name=postgres") psql -U postgres -d iot_analytics -c "
SELECT 
    'Daily Aggregates' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM daily_aggregates
WHERE date = '$TARGET_DATE'

UNION ALL

SELECT 
    'Anomaly Detections' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM anomaly_detections
WHERE DATE(detected_at) = '$TARGET_DATE';
"

print_success "IoT batch processing pipeline completed! 🎉"
print_status "View results in Grafana: http://localhost:3000"
