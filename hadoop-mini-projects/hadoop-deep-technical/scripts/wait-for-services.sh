#!/bin/bash

# Wait for all services to be ready
# This script polls health endpoints until all services are responsive

echo "Waiting for services to start..."

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
while ! pg_isready -h localhost -p 5432 -U hdfs_admin; do
    sleep 2
done
echo "PostgreSQL is ready!"

# Wait for Redis
echo "Waiting for Redis..."
while ! redis-cli -h localhost -p 6379 ping > /dev/null 2>&1; do
    sleep 2
done
echo "Redis is ready!"

# Wait for MinIO
echo "Waiting for MinIO..."
while ! curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    sleep 2
done
echo "MinIO is ready!"

# Wait for metadata service
echo "Waiting for Metadata Service..."
while ! curl -f http://localhost:8080/api/v1/health > /dev/null 2>&1; do
    sleep 2
done
echo "Metadata Service is ready!"

echo "All services are ready! 🎉"
echo ""
echo "Service URLs:"
echo "- Metadata Service API: http://localhost:8080"
echo "- HDFS WebHDFS API: http://localhost:8080/webhdfs/v1"
echo "- MinIO Console: http://localhost:9001 (admin/minioadmin123)"
echo "- Grafana Dashboard: http://localhost:3000 (admin/admin123)"
echo "- PostgreSQL: localhost:5432 (hdfs_admin/hdfs_password)"
echo "- Redis: localhost:6379"
