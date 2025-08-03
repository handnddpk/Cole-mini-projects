#!/bin/bash

# Wait for all services to be ready
echo "🚀 Starting Spark IoT Pipeline Services..."
echo "=========================================="

# Wait for Spark Master
echo "Waiting for Spark Master..."
while ! curl -f http://localhost:8080 > /dev/null 2>&1; do
    sleep 3
    echo "  Still waiting for Spark Master..."
done
echo "✅ Spark Master is ready!"

# Wait for Kafka
echo "Waiting for Kafka..."
while ! nc -z localhost 9092; do
    sleep 3
    echo "  Still waiting for Kafka..."
done
echo "✅ Kafka is ready!"

# Wait for Schema Registry
echo "Waiting for Schema Registry..."
while ! curl -f http://localhost:8081 > /dev/null 2>&1; do
    sleep 3
    echo "  Still waiting for Schema Registry..."
done
echo "✅ Schema Registry is ready!"

# Wait for MinIO
echo "Waiting for MinIO..."
while ! curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    sleep 3
    echo "  Still waiting for MinIO..."
done
echo "✅ MinIO is ready!"

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
while ! pg_isready -h localhost -p 5432 -U spark_user > /dev/null 2>&1; do
    sleep 3
    echo "  Still waiting for PostgreSQL..."
done
echo "✅ PostgreSQL is ready!"

# Wait for Redis
echo "Waiting for Redis..."
while ! redis-cli -h localhost -p 6379 ping > /dev/null 2>&1; do
    sleep 3
    echo "  Still waiting for Redis..."
done
echo "✅ Redis is ready!"

echo ""
echo "🎉 All services are ready!"
echo ""
echo "🔗 Service URLs:"
echo "  • Spark Master UI:     http://localhost:8080"
echo "  • Spark Applications:  http://localhost:4040"
echo "  • Kafka Control Center: http://localhost:9021"
echo "  • MinIO Console:       http://localhost:9001 (admin/minioadmin123)"
echo "  • Grafana Dashboard:   http://localhost:3000 (admin/admin123)"
echo "  • PostgreSQL:          localhost:5432 (spark_user/spark_password)"
echo ""
echo "🚀 Ready to start IoT data simulation and Spark streaming!"
