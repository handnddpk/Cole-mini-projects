# Big Data Processing Mini Projects

This repository contains comprehensive projects for **Apache Hadoop HDFS**, **Apache Spark**, and **Apache Kafka & Debezium** ecosystems, designed to demonstrate different aspects of big data processing, distributed storage systems, modern analytics platforms, and real-time event streaming.

## 📁 Project Collections

### 🗃️ Hadoop HDFS Projects
Traditional distributed storage and MapReduce processing

### ⚡ Apache Spark Projects  
Modern unified analytics engine for large-scale data processing

### 🌊 Apache Kafka & Debezium Projects
Event streaming, change data capture, and real-time integration

---

## 🗃️ Hadoop HDFS Projects Overview

### 1. 🔄 Hadoop HDFS Simulated Data Pipeline
**Location**: `hadoop-simulated-pipeline/`
- **Focus**: Learning HDFS fundamentals with synthetic data
- **Key Features**: E-commerce transaction simulation, MapReduce processing
- **Complexity**: Beginner to Intermediate
- **Setup Time**: ~15 minutes

### 2. 🌐 Hadoop HDFS Real Data Pipeline  
**Location**: `hadoop-realdata-pipeline/`
- **Focus**: Production-ready pipeline with real APIs
- **Key Features**: Multi-source data ingestion, Apache Spark, Airflow orchestration
- **Complexity**: Intermediate to Advanced
- **Setup Time**: ~30 minutes

### 3. ⚡ HDFS Metadata Management on MinIO
**Location**: `hadoop-deep-technical/`
- **Focus**: HDFS metadata simulation on object storage
- **Key Features**: MinIO integration, PostgreSQL metadata, consistency challenges
- **Complexity**: Advanced to Expert
- **Setup Time**: ~45 minutes

---

## ⚡ Apache Spark Projects Overview

### 1. 🔄 Spark Simulated IoT Data Pipeline
**Location**: `spark-simulated-pipeline/`
- **Focus**: Learning Spark fundamentals with synthetic streaming data
- **Key Features**: IoT sensor simulation, real-time analytics, Delta Lake
- **Complexity**: Beginner to Intermediate
- **Setup Time**: ~20 minutes

### 2. 🌐 Spark Real-Time Analytics Pipeline  
**Location**: `spark-realdata-pipeline/`
- **Focus**: Production-ready streaming analytics with real APIs
- **Key Features**: Multi-source streaming, ML inference, real-time dashboards
- **Complexity**: Intermediate to Advanced
- **Setup Time**: ~35 minutes

### 3. ⚡ Spark YAML/JSON SDK - Declarative Pipeline Generator
**Location**: `spark-yaml-sdk/`
- **Focus**: Enterprise self-service data pipeline platform
- **Key Features**: YAML/JSON to Spark job generation, Git integration, UI builder
- **Complexity**: Advanced to Expert
- **Setup Time**: ~50 minutes

---

## 🌊 Apache Kafka & Debezium Projects Overview

### 1. 🔄 Kafka Event-Driven Microservices Pipeline
**Location**: `kafka-simulated-pipeline/`
- **Focus**: Learning Kafka fundamentals with event-driven architecture
- **Key Features**: E-commerce microservices simulation, event sourcing, CQRS patterns
- **Complexity**: Beginner to Intermediate
- **Setup Time**: ~25 minutes

### 2. 🌐 Kafka Multi-Source Real-Time Integration  
**Location**: `kafka-realdata-pipeline/`
- **Focus**: Production-ready multi-source streaming with Kafka Connect
- **Key Features**: Database CDC, API streaming, real-time analytics, schema evolution
- **Complexity**: Intermediate to Advanced
- **Setup Time**: ~40 minutes

### 3. ⚡ Debezium Direct Streaming Engine - Kafka-Free CDC
**Location**: `debezium-direct-streaming/`
- **Focus**: Custom Debezium implementation for direct database-to-target streaming
- **Key Features**: MySQL to ClickHouse direct streaming, ultra-low latency, custom connectors
- **Complexity**: Advanced to Expert
- **Setup Time**: ~60 minutes

## Quick Start Guide

Each project contains:
- Comprehensive README with detailed setup instructions
- Docker Compose configuration for easy deployment
- Working source code with basic functionality
- Performance benchmarking tools
- Monitoring and observability setup

### Prerequisites
- Docker and Docker Compose
- At least 4GB RAM (8GB+ recommended for deep-technical)
- Basic knowledge of Hadoop ecosystem

### Choose Your Project

**New to Hadoop?** Start with `hadoop-simulated-pipeline`
```bash
cd hadoop-simulated-pipeline
docker-compose up -d
./scripts/run_pipeline.sh
```

**Want Real Data?** Try `hadoop-realdata-pipeline`
```bash
cd hadoop-realdata-pipeline
cp .env.example .env  # Add your API keys
docker-compose up -d
```

**Advanced User?** Dive into `hadoop-deep-technical`
```bash
cd hadoop-deep-technical
mvn clean package
docker-compose up -d
./scripts/performance_benchmark.sh
```

**New to Kafka?** Start with `kafka-simulated-pipeline`
```bash
cd kafka-simulated-pipeline
docker-compose up -d
./scripts/wait-for-services.sh
./scripts/start-microservices-demo.sh
```

**Want Multi-Source Integration?** Try `kafka-realdata-pipeline`
```bash
cd kafka-realdata-pipeline
cp .env.example .env  # Configure API keys
docker-compose up -d
./scripts/deploy-connectors.sh
```

**Advanced CDC Engineer?** Explore `debezium-direct-streaming`
```bash
cd debezium-direct-streaming
./scripts/setup-databases.sh
mvn clean package
./scripts/start-direct-streaming.sh
```

## Learning Path

### Hadoop HDFS Path
1. **Start Simple**: Run the simulated pipeline to understand HDFS basics
2. **Add Complexity**: Move to real data pipeline for production concepts
3. **Go Deep**: Explore the technical implementation for optimization techniques

### Apache Spark Path
1. **Foundation**: Master Spark fundamentals with simulated streaming data
2. **Real-World**: Build production streaming analytics with live data sources
3. **Enterprise**: Create self-service platform for democratizing data pipelines

### Kafka & Debezium Path
1. **Event-Driven**: Master Kafka with event-driven microservices patterns
2. **Integration**: Build production streaming with multiple data sources
3. **Innovation**: Create custom CDC solutions bypassing traditional Kafka topics

## Architecture Comparison

| Feature | Simulated | Real Data | Deep Technical |
|---------|-----------|-----------|----------------|
| Data Sources | Synthetic | APIs | Benchmarks |
| Processing | MapReduce | Spark + MR | Custom |
| Monitoring | Basic | Advanced | Expert |
| Scalability | Single Node | Multi-Node | Optimized |
| Learning Goal | Fundamentals | Production | Internals |

## Support

Each project includes:
- ✅ Complete documentation
- ✅ Working code examples  
- ✅ Troubleshooting guides
- ✅ Performance metrics
- ✅ Future enhancement ideas

## Contributing

Feel free to enhance any project with:
- Additional data sources
- New processing algorithms
- Performance optimizations
- Better monitoring
- Bug fixes and improvements

## Resources

- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [HDFS Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [MapReduce Tutorial](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)

---

**Happy Learning! 🎓**
