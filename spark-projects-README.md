# Apache Spark Mini Projects

This repository contains 3 comprehensive Apache Spark projects designed to demonstrate different aspects of distributed data processing, real-time analytics, and enterprise data pipeline automation.

## Projects Overview

### 1. 🔄 Spark Simulated Data Pipeline
**Location**: `spark-simulated-pipeline/`
- **Focus**: Learning Spark fundamentals with synthetic streaming data
- **Key Features**: IoT sensor simulation, real-time analytics, batch processing
- **Complexity**: Beginner to Intermediate
- **Setup Time**: ~20 minutes

### 2. 🌐 Spark Real-Time Analytics Pipeline  
**Location**: `spark-realdata-pipeline/`
- **Focus**: Production-ready streaming analytics with real APIs
- **Key Features**: Multi-source streaming, ML inference, Delta Lake, real-time dashboards
- **Complexity**: Intermediate to Advanced
- **Setup Time**: ~35 minutes

### 3. ⚡ Spark YAML/JSON SDK - Declarative Pipeline Generator
**Location**: `spark-yaml-sdk/`
- **Focus**: Enterprise self-service data pipeline platform
- **Key Features**: YAML/JSON to Spark job generation, Git integration, UI builder, auto-deployment
- **Complexity**: Advanced to Expert
- **Setup Time**: ~50 minutes

## Quick Start Guide

Each project contains:
- Comprehensive README with detailed setup instructions
- Docker Compose configuration for easy deployment
- Working source code with production-ready patterns
- Performance optimization examples
- Real-time monitoring and alerting setup

### Prerequisites
- Docker and Docker Compose
- At least 6GB RAM (12GB+ recommended for YAML SDK)
- Basic knowledge of Apache Spark ecosystem
- Python 3.8+ and Java 11+ (for local development)

### Choose Your Project

**New to Spark?** Start with `spark-simulated-pipeline`
```bash
cd spark-simulated-pipeline
docker-compose up -d
./scripts/start-streaming-pipeline.sh
```

**Want Real-Time Analytics?** Try `spark-realdata-pipeline`
```bash
cd spark-realdata-pipeline
cp .env.example .env  # Add your API keys
docker-compose up -d
./scripts/start-realtime-analytics.sh
```

**Enterprise Platform Builder?** Dive into `spark-yaml-sdk`
```bash
cd spark-yaml-sdk
./scripts/setup-development.sh
docker-compose up -d
./scripts/deploy-sample-pipelines.sh
```

## Learning Path

1. **Foundation**: Master Spark fundamentals with simulated streaming data
2. **Real-World**: Build production streaming analytics with live data sources
3. **Enterprise**: Create self-service platform for democratizing data pipeline creation

## Architecture Comparison

| Feature | Simulated | Real-Time Analytics | YAML SDK Platform |
|---------|-----------|---------------------|-------------------|
| Data Sources | IoT Simulation | Live APIs + Kafka | Git Repos + UI |
| Processing | Batch + Streaming | Real-time ML | Dynamic Generation |
| Storage | Parquet + Delta | Delta Lake + Redis | Multi-format Support |
| Monitoring | Spark UI | Grafana + Alerts | Enterprise Dashboard |
| Scalability | Single Cluster | Multi-cluster | Auto-scaling |
| User Experience | Developer | Data Analyst | Business User |
| Learning Goal | Spark Basics | Production Streaming | Platform Engineering |

## Key Technologies

### Spark Simulated Pipeline
- **Apache Spark 3.5** (Structured Streaming)
- **Apache Kafka** (Event streaming)
- **Delta Lake** (ACID transactions)
- **MinIO** (Object storage)
- **PostgreSQL** (Metadata)

### Spark Real-Time Analytics
- **Spark Streaming** + **MLlib**
- **Apache Kafka** + **Schema Registry**
- **Delta Lake** + **Redis**
- **Prometheus** + **Grafana**
- **Jupyter** (Interactive analysis)

### Spark YAML SDK Platform
- **Custom Spark Job Generator**
- **GitOps** integration
- **React.js** UI builder
- **Kubernetes** orchestration
- **Apache Airflow** scheduling

## Use Cases by Project

### 🔄 Simulated Pipeline
- IoT data processing patterns
- Stream-batch architecture
- Data quality frameworks
- Performance optimization

### 🌐 Real-Time Analytics  
- Fraud detection systems
- Recommendation engines
- Operational monitoring
- Customer analytics

### ⚡ YAML SDK Platform
- Enterprise data democratization
- Self-service analytics
- Compliance and governance
- Cost optimization

## Support

Each project includes:
- ✅ Complete documentation with architecture diagrams
- ✅ Production-ready code examples  
- ✅ Comprehensive troubleshooting guides
- ✅ Performance tuning recommendations
- ✅ Extensive future enhancement roadmaps

## Contributing

Contribute by enhancing:
- Additional data source connectors
- New transformation algorithms
- ML model integration patterns
- UI/UX improvements
- Performance optimizations

## Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Spark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [Delta Lake Documentation](https://docs.delta.io/)

---

**Master Spark at Scale! 🚀**
