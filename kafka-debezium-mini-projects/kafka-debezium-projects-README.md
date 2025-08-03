# Apache Kafka & Debezium Mini Projects

This repository contains 3 comprehensive Apache Kafka and Debezium projects designed to demonstrate different aspects of event streaming, change data capture (CDC), and real-time data integration patterns.

## Projects Overview

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
- **Key Features**: MySQL to ClickHouse direct streaming, custom connectors, performance optimization
- **Complexity**: Advanced to Expert
- **Setup Time**: ~60 minutes

## Quick Start Guide

Each project contains:
- Comprehensive README with detailed setup instructions
- Docker Compose configuration for complete ecosystem
- Production-ready source code with error handling
- Performance benchmarking and optimization guides
- Monitoring and observability setup with metrics

### Prerequisites
- Docker and Docker Compose
- At least 8GB RAM (16GB+ recommended for Debezium Direct)
- Basic knowledge of Apache Kafka ecosystem
- Java 11+ and Python 3.8+ (for local development)
- MySQL and ClickHouse familiarity (for direct streaming project)

### Choose Your Project

**New to Kafka?** Start with `kafka-simulated-pipeline`
```bash
cd kafka-simulated-pipeline
docker-compose up -d
./scripts/start-microservices-demo.sh
```

**Want Multi-Source Integration?** Try `kafka-realdata-pipeline`
```bash
cd kafka-realdata-pipeline
cp .env.example .env  # Add your database credentials
docker-compose up -d
./scripts/deploy-connectors.sh
```

**Advanced CDC Engineer?** Dive into `debezium-direct-streaming`
```bash
cd debezium-direct-streaming
./scripts/setup-databases.sh
mvn clean package
docker-compose up -d
./scripts/start-direct-streaming.sh
```

## Learning Path

1. **Foundation**: Master Kafka with event-driven microservices patterns
2. **Integration**: Build production streaming with multiple data sources
3. **Innovation**: Create custom CDC solutions bypassing traditional Kafka topics

## Architecture Comparison

| Feature | Simulated Microservices | Multi-Source Integration | Direct CDC Streaming |
|---------|------------------------|--------------------------|---------------------|
| Data Sources | Event Simulation | Databases + APIs | MySQL Binlog |
| Processing | Event Sourcing | Stream Processing | Custom CDC Engine |
| Storage | Kafka Topics | Multiple Sinks | Direct ClickHouse |
| Monitoring | Kafka UI | Confluent Control Center | Custom Metrics |
| Scalability | Multi-broker | Connect Clusters | Direct Parallelism |
| User Experience | Developer | Data Engineer | Platform Engineer |
| Learning Goal | Kafka Basics | Production Integration | Custom CDC Innovation |

## Key Technologies

### Kafka Simulated Pipeline
- **Apache Kafka 3.7** (Event streaming)
- **Kafka Streams** (Stream processing)
- **Schema Registry** (Schema management)
- **Spring Boot** (Microservices)
- **PostgreSQL** (Event store)

### Kafka Multi-Source Integration
- **Kafka Connect** + **Connectors**
- **Debezium CDC** connectors
- **Elasticsearch** + **MongoDB**
- **Prometheus** + **Grafana**
- **Apache Avro** (Schema evolution)

### Debezium Direct Streaming
- **Custom Debezium Engine**
- **MySQL Binlog Reader**
- **ClickHouse Native Protocol**
- **Java NIO** (High performance)
- **Micrometer** (Metrics)

## Use Cases by Project

### 🔄 Simulated Pipeline
- Event-driven architecture patterns
- Microservices communication
- Event sourcing implementation
- CQRS pattern demonstration

### 🌐 Multi-Source Integration  
- Real-time data warehouse loading
- Multi-database synchronization
- API-to-database streaming
- Schema evolution handling

### ⚡ Direct CDC Streaming
- Ultra-low latency CDC
- Kafka-free data streaming
- Custom connector development
- High-performance data transfer

## Performance Benchmarks

### Expected Throughput
- **Simulated Pipeline**: 50K events/sec
- **Multi-Source Integration**: 100K records/sec
- **Direct Streaming**: 500K+ records/sec (no Kafka overhead)

### Resource Requirements
- **Simulated**: 4GB RAM, 2 CPU cores
- **Multi-Source**: 8GB RAM, 4 CPU cores  
- **Direct Streaming**: 16GB RAM, 8 CPU cores

## Support

Each project includes:
- ✅ Complete documentation with architecture diagrams
- ✅ Production-ready code with comprehensive error handling
- ✅ Performance tuning guides and benchmarking tools
- ✅ Troubleshooting guides for common issues
- ✅ Extensive future enhancement roadmaps

## Contributing

Contribute by enhancing:
- Additional source/sink connectors
- New event processing patterns
- Performance optimization techniques
- Monitoring and alerting improvements
- Security and authentication features

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Debezium Documentation](https://debezium.io/documentation/)
- [Kafka Connect Guide](https://docs.confluent.io/platform/current/connect/index.html)
- [Event Streaming Patterns](https://www.confluent.io/blog/event-streaming-patterns/)

---

**Master Event Streaming at Scale! 🚀**
