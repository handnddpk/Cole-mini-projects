#!/bin/bash
set -e

echo "🚀 Setting up Hadoop Deep Technical Project Environment"
echo "======================================================="

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

# Check Java
if ! command -v java &> /dev/null; then
    echo "❌ Java is not installed. Please install Java 11+ first."
    exit 1
fi

# Check Maven
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven is not installed. Please install Maven first."
    exit 1
fi

echo "✅ All prerequisites met"

# Build the project
echo "🔨 Building Java project..."
mvn clean compile package -DskipTests

echo "✅ Java project built successfully"

# Create data directories
echo "📁 Creating data directories..."
mkdir -p data/minio
mkdir -p data/postgres
mkdir -p data/redis
mkdir -p logs

echo "✅ Directory structure created"

# Start infrastructure services
echo "🐳 Starting infrastructure services..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."

# Wait for MinIO
echo "🔄 Waiting for MinIO to be ready..."
max_retries=30
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    if curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        echo "✅ MinIO is ready"
        break
    fi
    echo "⏳ Waiting for MinIO... ($((retry_count + 1))/$max_retries)"
    sleep 5
    retry_count=$((retry_count + 1))
done

if [ $retry_count -eq $max_retries ]; then
    echo "❌ MinIO failed to start within expected time"
    exit 1
fi

# Wait for PostgreSQL
echo "🔄 Waiting for PostgreSQL to be ready..."
max_retries=30
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    if docker-compose exec -T postgres pg_isready -U hdfs_user > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready"
        break
    fi
    echo "⏳ Waiting for PostgreSQL... ($((retry_count + 1))/$max_retries)"
    sleep 3
    retry_count=$((retry_count + 1))
done

if [ $retry_count -eq $max_retries ]; then
    echo "❌ PostgreSQL failed to start within expected time"
    exit 1
fi

# Wait for Redis
echo "🔄 Waiting for Redis to be ready..."
max_retries=20
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    if docker-compose exec -T redis redis-cli ping | grep -q PONG; then
        echo "✅ Redis is ready"
        break
    fi
    echo "⏳ Waiting for Redis... ($((retry_count + 1))/$max_retries)"
    sleep 2
    retry_count=$((retry_count + 1))
done

if [ $retry_count -eq $max_retries ]; then
    echo "❌ Redis failed to start within expected time"
    exit 1
fi

# Initialize MinIO buckets
echo "🪣 Creating MinIO buckets..."
docker-compose exec -T minio1 mc alias set local http://localhost:9000 minioadmin minioadmin123 || true
docker-compose exec -T minio1 mc mb local/hdfs-data || true
docker-compose exec -T minio1 mc mb local/hdfs-metadata || true

echo "✅ MinIO buckets created"

# Initialize database schema
echo "🗄️  Initializing database schema..."
docker-compose exec -T postgres psql -U hdfs_user -d hdfs_metadata -c "
CREATE TABLE IF NOT EXISTS namespace_nodes (
    id BIGSERIAL PRIMARY KEY,
    path VARCHAR(4096) NOT NULL UNIQUE,
    parent_path VARCHAR(4096),
    node_type VARCHAR(20) NOT NULL,
    owner VARCHAR(100) NOT NULL,
    group_name VARCHAR(100) NOT NULL,
    permissions SMALLINT NOT NULL,
    modification_time BIGINT NOT NULL,
    access_time BIGINT NOT NULL,
    block_size BIGINT,
    replication SMALLINT,
    file_size BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_namespace_parent_path ON namespace_nodes(parent_path);
CREATE INDEX IF NOT EXISTS idx_namespace_path ON namespace_nodes(path);

CREATE TABLE IF NOT EXISTS file_blocks (
    id BIGSERIAL PRIMARY KEY,
    file_path VARCHAR(4096) NOT NULL,
    block_id VARCHAR(256) NOT NULL,
    block_index INTEGER NOT NULL,
    block_size BIGINT NOT NULL,
    object_key VARCHAR(512) NOT NULL,
    checksum VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_file_blocks_path ON file_blocks(file_path);
CREATE INDEX IF NOT EXISTS idx_file_blocks_block_id ON file_blocks(block_id);
" || true

echo "✅ Database schema initialized"

# Start the HDFS metadata service
echo "🚀 Starting HDFS Metadata Service..."
nohup java -jar target/hdfs-metadata-on-minio-1.0-SNAPSHOT.jar > logs/hdfs-service.log 2>&1 &
HDFS_SERVICE_PID=$!
echo $HDFS_SERVICE_PID > hdfs-service.pid

echo "⏳ Waiting for HDFS service to be ready..."
max_retries=30
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    if curl -f http://localhost:8080/api/hdfs/health > /dev/null 2>&1; then
        echo "✅ HDFS Metadata Service is ready"
        break
    fi
    echo "⏳ Waiting for HDFS service... ($((retry_count + 1))/$max_retries)"
    sleep 3
    retry_count=$((retry_count + 1))
done

if [ $retry_count -eq $max_retries ]; then
    echo "❌ HDFS Metadata Service failed to start within expected time"
    echo "🔍 Check logs: tail -f logs/hdfs-service.log"
    exit 1
fi

# Run basic tests
echo "🧪 Running basic functionality tests..."
./scripts/test-basic-operations.sh

echo "✅ Basic tests passed"

# Display service endpoints
echo ""
echo "🎉 Setup Complete! Service endpoints:"
echo "======================================"
echo "🌐 HDFS Metadata API:     http://localhost:8080"
echo "🌐 MinIO Console:         http://localhost:9001 (minioadmin/minioadmin123)"
echo "🌐 PostgreSQL:           localhost:5432 (hdfs_user/hdfs_pass)"
echo "🌐 Redis:                localhost:6379"
echo "🌐 Prometheus Metrics:   http://localhost:8080/actuator/prometheus"
echo ""
echo "📚 API Documentation:"
echo "  • Health Check:         GET  http://localhost:8080/api/hdfs/health"
echo "  • Create File:         POST http://localhost:8080/api/hdfs/create"
echo "  • Read File:           GET  http://localhost:8080/api/hdfs/open"
echo "  • Delete File:         DELETE http://localhost:8080/api/hdfs/delete"
echo "  • List Directory:      GET  http://localhost:8080/api/hdfs/list"
echo "  • Rename File:         POST http://localhost:8080/api/hdfs/rename"
echo ""
echo "📚 Next steps:"
echo "1. Run comprehensive tests: ./scripts/test-basic-operations.sh"
echo "2. Run performance benchmarks: ./scripts/benchmark-operations.sh"
echo "3. View service logs: tail -f logs/hdfs-service.log"
echo "4. Stop service: kill \$(cat hdfs-service.pid)"
echo ""
echo "🚀 Happy distributed systems engineering!"
