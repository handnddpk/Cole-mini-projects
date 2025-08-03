#!/bin/bash
"""
Setup script for IoT Simulated Pipeline
Initializes the complete development environment
"""

set -e

echo "🚀 Setting up IoT Simulated Pipeline..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

print_success "Docker is running"

# Check if Docker Compose is available
if ! docker-compose --version > /dev/null 2>&1; then
    if ! docker compose version > /dev/null 2>&1; then
        print_error "Docker Compose is not available. Please install Docker Compose."
        exit 1
    fi
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

print_success "Docker Compose is available"

# Create necessary directories
print_status "Creating necessary directories..."
mkdir -p logs
mkdir -p data/spark-warehouse
mkdir -p data/postgres-data
mkdir -p data/kafka-data
mkdir -p data/zookeeper-data
mkdir -p data/airflow-logs

print_success "Directories created"

# Set permissions
print_status "Setting permissions..."
chmod +x scripts/*.sh
chmod -R 755 jobs/

print_success "Permissions set"

# Load environment variables
if [ -f .env ]; then
    print_status "Loading environment variables from .env"
    source .env
else
    print_warning ".env file not found, using defaults"
fi

# Pull Docker images
print_status "Pulling Docker images (this may take a while)..."
$DOCKER_COMPOSE pull

print_success "Docker images pulled"

# Start infrastructure services
print_status "Starting infrastructure services..."
$DOCKER_COMPOSE up -d zookeeper kafka postgres

# Wait for services to be ready
print_status "Waiting for services to be ready..."
./scripts/wait-for-services.sh

print_success "Infrastructure services are ready"

# Initialize database
print_status "Initializing PostgreSQL database..."
docker exec -i $(docker ps -qf "name=postgres") psql -U postgres -d iot_analytics < sql/init.sql

print_success "Database initialized"

# Start Spark services
print_status "Starting Spark services..."
$DOCKER_COMPOSE up -d spark-master spark-worker

# Wait for Spark to be ready
sleep 30

print_success "Spark services started"

# Start monitoring services
print_status "Starting monitoring services..."
$DOCKER_COMPOSE up -d grafana prometheus

print_success "Monitoring services started"

# Install Python dependencies in a virtual environment (optional)
if command -v python3 > /dev/null 2>&1; then
    print_status "Setting up Python virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    print_success "Python environment set up"
else
    print_warning "Python3 not found, skipping virtual environment setup"
fi

# Generate initial test data
print_status "Generating initial test data..."
python3 jobs/generators/iot_data_generator.py --batch 1000 &
GENERATOR_PID=$!

# Wait a bit for data generation
sleep 10

# Stop the generator
kill $GENERATOR_PID 2>/dev/null || true

print_success "Initial test data generated"

# Start Airflow (optional)
if [ "${START_AIRFLOW:-false}" = "true" ]; then
    print_status "Starting Airflow..."
    $DOCKER_COMPOSE up -d airflow-webserver airflow-scheduler
    print_success "Airflow started"
fi

# Display service URLs
print_success "Setup completed! Services are available at:"
echo ""
echo "🌐 Service URLs:"
echo "   • Spark Master UI:    http://localhost:8080"
echo "   • Kafka UI:           http://localhost:8081"  
echo "   • Grafana:            http://localhost:3000 (admin/admin)"
echo "   • PostgreSQL:         localhost:5432 (postgres/postgres)"
echo "   • Prometheus:         http://localhost:9090"
if [ "${START_AIRFLOW:-false}" = "true" ]; then
    echo "   • Airflow:            http://localhost:8082 (admin/admin)"
fi

echo ""
echo "📊 Database: iot_analytics"
echo "📁 Data Directory: ./data/"
echo "📝 Logs Directory: ./logs/"

echo ""
echo "🚀 To start the complete pipeline:"
echo "   ./scripts/start-streaming.sh    # Start streaming pipeline"
echo "   ./scripts/start-simulation.sh   # Start data simulation"
echo "   ./scripts/run-batch.sh          # Run batch processing"
echo "   ./scripts/run-ml-pipeline.sh    # Run ML pipeline"

echo ""
echo "🛑 To stop all services:"
echo "   $DOCKER_COMPOSE down"

echo ""
print_success "IoT Simulated Pipeline setup complete! 🎉"
