#!/bin/bash
set -e

echo "🚀 Setting up Hadoop Simulated Pipeline Environment"
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
mkdir -p data/input
mkdir -p data/output
mkdir -p data/logs

echo "✅ Directory structure created"

# Start Docker services
echo "🐳 Starting Docker services..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."

# Wait for MySQL
echo "🔄 Waiting for MySQL to be ready..."
while ! docker-compose exec -T mysql-source mysqladmin ping -h"localhost" --silent; do
    sleep 2
done
echo "✅ MySQL is ready"

# Wait for Hadoop services
echo "🔄 Waiting for Hadoop services to be ready..."
sleep 30

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
    echo "🔍 Check logs with: docker-compose logs namenode"
    exit 1
fi

# Generate sample data
echo "🎲 Generating sample data..."
python3 scripts/generate_multi_domain_data.py

echo "✅ Sample data generated"

# Display service endpoints
echo ""
echo "🎉 Setup Complete! Service endpoints:"
echo "======================================"
echo "🌐 Hadoop NameNode UI:     http://localhost:9870"
echo "🌐 Hadoop DataNode UI:     http://localhost:9864"
echo "🌐 Resource Manager UI:    http://localhost:8088"
echo "🌐 Hive Web UI:           http://localhost:10002"
echo "🌐 MySQL Database:        localhost:3306 (user: root, password: hadoop)"
echo ""
echo "📚 Next steps:"
echo "1. Run the comprehensive pipeline: ./scripts/run_comprehensive_pipeline.sh"
echo "2. Access Hadoop UI to monitor jobs: http://localhost:9870"
echo "3. Query data with Hive: docker-compose exec hive-server2 beeline -u jdbc:hive2://localhost:10000"
echo ""
echo "🚀 Happy Hadoop learning!"
