# Hadoop Mini Projects - Troubleshooting Guide

## Common Issues and Solutions

### 🐳 Docker Related Issues

#### Issue: "Cannot connect to the Docker daemon"
```bash
# Solution: Start Docker service
sudo systemctl start docker  # Linux
# or restart Docker Desktop on macOS/Windows
```

#### Issue: "Port already in use"
```bash
# Solution: Find and kill processes using the port
lsof -ti:9870 | xargs kill -9  # Replace 9870 with the conflicting port
# or change ports in docker-compose.yml
```

#### Issue: "Not enough memory for containers"
```bash
# Solution: Increase Docker memory allocation
# Docker Desktop: Settings > Resources > Advanced > Memory (set to 8GB+)
# Linux: Ensure sufficient system memory
```

### 🚀 Hadoop Simulated Pipeline Issues

#### Issue: "MySQL connection refused"
```bash
# Solution: Wait for MySQL to be fully ready
docker-compose logs mysql-source
# Wait until you see "MySQL init process done. Ready for start up."
```

#### Issue: "NameNode not responsive"
```bash
# Solution: Check NameNode logs and restart if needed
docker-compose logs namenode
docker-compose restart namenode
```

#### Issue: "Hive connection timeout"
```bash
# Solution: Ensure Hive depends on Hadoop and wait for initialization
docker-compose exec hive-server2 beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;"
```

#### Issue: "Python dependencies missing"
```bash
# Solution: Install requirements in virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 🌐 Real-Data Pipeline Issues

#### Issue: "API rate limits exceeded"
```bash
# Solution: Configure proper rate limiting in .env
RATE_LIMIT_REQUESTS_PER_MINUTE=5
RATE_LIMIT_BURST=10
```

#### Issue: "Kafka broker not available"
```bash
# Solution: Check Kafka startup sequence
docker-compose logs kafka
# Ensure Zookeeper is running first
docker-compose up -d zookeeper
sleep 10
docker-compose up -d kafka
```

#### Issue: "Airflow webserver not starting"
```bash
# Solution: Initialize Airflow database
docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create \
    --username admin --password admin --firstname Admin \
    --lastname User --role Admin --email admin@example.com
```

#### Issue: "API keys not working"
```bash
# Solution: Verify API key format in .env file
# Remove quotes and extra spaces
ALPHA_VANTAGE_API_KEY=YOUR_KEY_HERE
# not: ALPHA_VANTAGE_API_KEY="YOUR_KEY_HERE"
```

### ⚡ Deep Technical Project Issues

#### Issue: "Maven build failures"
```bash
# Solution: Update Maven dependencies
mvn clean install -U
# If still failing, check Java version
java -version  # Should be 11+
```

#### Issue: "MinIO cluster not forming"
```bash
# Solution: Check network connectivity between MinIO nodes
docker-compose logs minio1
docker-compose logs minio2
# Restart with clean volumes if needed
docker-compose down -v
docker-compose up -d
```

#### Issue: "PostgreSQL connection errors"
```bash
# Solution: Check PostgreSQL initialization
docker-compose exec postgres pg_isready -U hdfs_user
# Reset database if needed
docker-compose down postgres
docker volume rm hadoop-deep-technical_postgres_data
docker-compose up -d postgres
```

#### Issue: "HDFS service startup failures"
```bash
# Solution: Check service logs and dependencies
tail -f logs/hdfs-service.log
# Ensure all dependencies are ready
curl http://localhost:5432  # PostgreSQL
curl http://localhost:6379  # Redis
curl http://localhost:9000  # MinIO
```

### 📊 Performance Issues

#### Issue: "Slow query performance"
```bash
# Solution: Optimize Hive queries
# Use partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

# Enable vectorization
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
```

#### Issue: "Memory errors in Spark jobs"
```bash
# Solution: Adjust Spark memory settings in docker-compose.yml
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g
SPARK_EXECUTOR_CORES=2
```

#### Issue: "HDFS out of space"
```bash
# Solution: Clean up old data and increase disk space
docker-compose exec namenode hdfs dfs -du -h /
docker-compose exec namenode hdfs dfs -rm -r /tmp/*
# Increase volume size in docker-compose.yml
```

### 🔧 Development Issues

#### Issue: "IDE cannot resolve Hadoop classes"
```bash
# Solution: Add Hadoop libraries to classpath
# For IntelliJ IDEA:
# File > Project Structure > Libraries > Add Hadoop JARs
# For Eclipse:
# Project Properties > Java Build Path > Libraries > Add External JARs
```

#### Issue: "Unit tests failing"
```bash
# Solution: Use TestContainers for integration tests
# Check test configuration in pom.xml
mvn test -Dtest=MetadataServiceTest
```

### 🔍 Debugging Tips

#### Enable Debug Logging
```bash
# Hadoop
export HADOOP_LOGLEVEL=DEBUG

# Hive
SET hive.root.logger=DEBUG,console;

# Spark
--conf spark.eventLog.enabled=true
--conf spark.eventLog.dir=hdfs://namenode:9000/spark-logs
```

#### Monitor Resource Usage
```bash
# Container resource usage
docker stats

# HDFS usage
docker-compose exec namenode hdfs dfsadmin -report

# System resources
htop
df -h
free -h
```

#### Check Service Health
```bash
# Hadoop cluster health
docker-compose exec namenode hdfs dfsadmin -report

# Hive connection
docker-compose exec hive-server2 beeline -u jdbc:hive2://localhost:10000 -e "SELECT 1;"

# Kafka topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 📞 Getting Help

#### Log Locations
- Docker logs: `docker-compose logs [service-name]`
- Hadoop logs: `/opt/hadoop/logs/` inside containers
- Hive logs: `/tmp/hive/` inside containers
- Application logs: `logs/` directory in each project

#### Useful Commands
```bash
# View all running containers
docker ps

# Enter container for debugging
docker-compose exec [service-name] bash

# Check network connectivity
docker-compose exec [service-name] ping [other-service]

# View container resource limits
docker inspect [container-name] | grep -i memory
```

#### Community Resources
- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [Apache Hive Wiki](https://cwiki.apache.org/confluence/display/Hive/)
- [Docker Compose Troubleshooting](https://docs.docker.com/compose/troubleshooting/)
- [Stack Overflow - Hadoop Tags](https://stackoverflow.com/questions/tagged/hadoop)

### 🚨 Emergency Recovery

#### Complete Reset
```bash
# Stop all services
docker-compose down

# Remove all volumes (WARNING: This deletes all data)
docker-compose down -v

# Remove all images (optional)
docker system prune -a

# Start fresh
docker-compose up -d
```

#### Backup Important Data
```bash
# Backup HDFS data
docker-compose exec namenode hdfs dfs -get / /backup/hdfs-backup

# Backup database
docker-compose exec postgres pg_dump -U hdfs_user hdfs_metadata > backup.sql

# Backup configuration
cp -r conf/ backup/conf-backup/
```
