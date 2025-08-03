#!/bin/bash

# Real Data Pipeline Setup Script
# This script sets up the complete real data processing pipeline

set -e

echo "🚀 Setting up Real Data Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed. Please install it first."
    exit 1
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data/{bronze,silver,gold}
mkdir -p data/checkpoints
mkdir -p notebooks
mkdir -p airflow/{logs,plugins}

# Set permissions
chmod -R 777 data/
chmod -R 777 airflow/

# Copy environment variables
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cp .env.example .env
    echo "⚠️  Please edit .env file with your API keys before running the pipeline"
fi

# Pull Docker images
echo "🐳 Pulling Docker images..."
docker-compose pull

# Start core services first
echo "🔧 Starting core services..."
docker-compose up -d postgres redis kafka

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Initialize database
echo "🗄️ Initializing database..."
docker-compose exec -T postgres psql -U postgres -d postgres -f /docker-entrypoint-initdb.d/init.sql

# Create Kafka topics
echo "📡 Creating Kafka topics..."
docker-compose exec kafka kafka-topics --create --topic twitter_stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true
docker-compose exec kafka kafka-topics --create --topic reddit_stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true
docker-compose exec kafka kafka-topics --create --topic news_stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true
docker-compose exec kafka kafka-topics --create --topic financial_stocks --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true
docker-compose exec kafka kafka-topics --create --topic financial_crypto --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true
docker-compose exec kafka kafka-topics --create --topic financial_market_news --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true

# Start remaining services
echo "🚀 Starting all services..."
docker-compose up -d

# Wait for all services
echo "⏳ Waiting for all services to be ready..."
sleep 60

# Install Python dependencies in Jupyter container
echo "📦 Installing Python dependencies..."
docker-compose exec jupyter pip install --quiet \
    tweepy praw requests kafka-python textblob yfinance schedule \
    scikit-learn pandas numpy matplotlib seaborn plotly dash

echo "✅ Setup completed successfully!"
echo ""
echo "🌐 Services are available at:"
echo "  - Jupyter Lab: http://localhost:8888 (token: spark-analytics)"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Airflow: http://localhost:8081 (admin/admin)"
echo "  - Grafana: http://localhost:3000 (admin/admin)"
echo ""
echo "📋 Next steps:"
echo "  1. Edit .env file with your API keys"
echo "  2. Run ./scripts/start-streaming.sh to start streaming jobs"
echo "  3. Run ./scripts/start-ingestion.sh to start data ingestion"
echo "  4. Access Jupyter Lab to explore the data and run analytics"
echo ""
echo "📚 For more information, check the README.md file"
