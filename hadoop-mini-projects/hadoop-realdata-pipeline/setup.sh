#!/bin/bash
set -e

echo "🚀 Setting up Hadoop Real-Data Pipeline Environment"
echo "=================================================="

# Check prerequisites
echo "📋 Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8+ first."
    exit 1
fi

echo "✅ All prerequisites met"

# Check for .env file
if [ ! -f .env ]; then
    echo "⚠️  No .env file found. Creating from template..."
    cp .env.example .env
    echo "📝 Please edit .env file with your API keys before continuing."
    echo "   Required: ALPHA_VANTAGE_API_KEY, OPENWEATHER_API_KEY, etc."
    read -p "Press Enter when you've configured your API keys..."
fi

# Create Python virtual environment
echo "🐍 Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Python environment ready"

# Create data directories
echo "📁 Creating data directories..."
mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/archive
mkdir -p data/logs
mkdir -p data/checkpoints

echo "✅ Directory structure created"

# Start Docker services
echo "🐳 Starting Docker services..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."

# Wait for Kafka
echo "🔄 Waiting for Kafka to be ready..."
max_retries=60
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    if docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        echo "✅ Kafka is ready"
        break
    fi
    echo "⏳ Waiting for Kafka... ($((retry_count + 1))/$max_retries)"
    sleep 2
    retry_count=$((retry_count + 1))
done

if [ $retry_count -eq $max_retries ]; then
    echo "❌ Kafka failed to start within expected time"
    exit 1
fi

# Wait for Hadoop services
echo "🔄 Waiting for Hadoop services to be ready..."
sleep 45

# Check if NameNode is responsive
max_retries=30
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    if curl -f http://localhost:9870 > /dev/null 2>&1; then
        echo "✅ Hadoop NameNode is ready"
        break
    fi
    echo "⏳ Waiting for NameNode... ($((retry_count + 1))/$max_retries)"
    sleep 10
    retry_count=$((retry_count + 1))
done

if [ $retry_count -eq $max_retries ]; then
    echo "❌ Hadoop NameNode failed to start within expected time"
    exit 1
fi

# Wait for Airflow
echo "🔄 Waiting for Airflow to be ready..."
max_retries=30
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    if curl -f http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ Airflow is ready"
        break
    fi
    echo "⏳ Waiting for Airflow... ($((retry_count + 1))/$max_retries)"
    sleep 10
    retry_count=$((retry_count + 1))
done

# Create Kafka topics
echo "📨 Creating Kafka topics..."
docker-compose exec -T kafka kafka-topics --create --bootstrap-server localhost:9092 --topic financial-data --partitions 3 --replication-factor 1 || true
docker-compose exec -T kafka kafka-topics --create --bootstrap-server localhost:9092 --topic weather-data --partitions 3 --replication-factor 1 || true
docker-compose exec -T kafka kafka-topics --create --bootstrap-server localhost:9092 --topic social-data --partitions 3 --replication-factor 1 || true
docker-compose exec -T kafka kafka-topics --create --bootstrap-server localhost:9092 --topic iot-data --partitions 3 --replication-factor 1 || true

echo "✅ Kafka topics created"

# Initialize HDFS directories
echo "📁 Creating HDFS directories..."
docker-compose exec -T namenode hdfs dfs -mkdir -p /raw-data/financial || true
docker-compose exec -T namenode hdfs dfs -mkdir -p /raw-data/weather || true
docker-compose exec -T namenode hdfs dfs -mkdir -p /raw-data/social || true
docker-compose exec -T namenode hdfs dfs -mkdir -p /raw-data/iot || true
docker-compose exec -T namenode hdfs dfs -mkdir -p /processed-data || true

echo "✅ HDFS directories created"

# Display service endpoints
echo ""
echo "🎉 Setup Complete! Service endpoints:"
echo "======================================"
echo "🌐 Hadoop NameNode UI:     http://localhost:9870"
echo "🌐 Hadoop DataNode UI:     http://localhost:9864"
echo "🌐 Resource Manager UI:    http://localhost:8088"
echo "🌐 Airflow UI:            http://localhost:8080 (admin/admin)"
echo "🌐 Kafka UI:              http://localhost:8081"
echo "🌐 Grafana Dashboard:     http://localhost:3000 (admin/admin)"
echo "🌐 Prometheus Metrics:    http://localhost:9090"
echo "🌐 Elasticsearch:         http://localhost:9200"
echo ""
echo "📚 Next steps:"
echo "1. Start data ingestion: python3 scripts/ingest_all_data.py"
echo "2. Monitor workflows in Airflow UI: http://localhost:8080"
echo "3. View real-time metrics in Grafana: http://localhost:3000"
echo ""
echo "🚀 Happy real-time data processing!"
