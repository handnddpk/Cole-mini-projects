# Kafka Multi-Source Real-Time Integration Pipeline

## Project Description

This project demonstrates a production-ready multi-source data integration platform using Apache Kafka Connect, Debezium CDC connectors, and various sink connectors. It showcases real-time data streaming from multiple heterogeneous sources (databases, APIs, files) into various target systems (data warehouses, search engines, caches) with schema evolution, data transformation, and comprehensive monitoring.

## Data Sources & Integration

- **Database CDC**: MySQL, PostgreSQL, MongoDB change data capture via Debezium
- **API Streaming**: REST APIs with custom connectors (financial data, social media, IoT)
- **File Sources**: S3, HDFS, local filesystem with various formats (JSON, CSV, Avro, Parquet)
- **Message Systems**: JMS, RabbitMQ bridge connectors
- **Real-time Sources**: Twitter API, stock market feeds, weather APIs, e-commerce events

## Target Systems

- **Data Warehouses**: ClickHouse, BigQuery, Snowflake
- **Search & Analytics**: Elasticsearch, Apache Solr
- **Caches**: Redis, Hazelcast
- **Databases**: PostgreSQL, MongoDB, Cassandra
- **Object Storage**: S3, MinIO
- **Time-series**: InfluxDB, TimescaleDB

## Tech Stack

- **Streaming Platform**: Apache Kafka 3.7 with Kafka Connect
- **CDC Connectors**: Debezium (MySQL, PostgreSQL, MongoDB)
- **Schema Management**: Confluent Schema Registry with Avro/JSON Schema
- **Data Processing**: Kafka Streams, Apache Flink (optional)
- **Monitoring**: Confluent Control Center, Prometheus, Grafana
- **Container Platform**: Docker & Docker Compose
- **API Integration**: Custom REST source connectors
- **Data Validation**: Great Expectations integration

## Project Purpose

- Master Kafka Connect ecosystem and connector development
- Learn Change Data Capture (CDC) patterns with Debezium
- Understand multi-source data integration architectures
- Practice schema evolution and data compatibility
- Explore real-time ETL/ELT patterns
- Implement data quality validation and monitoring
- Learn connector configuration and management
- Practice troubleshooting distributed streaming systems

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available for containers
- Java 11+ and Python 3.8+ (for custom connectors)
- Access to external APIs (optional, demo data provided)
- Basic understanding of Kafka Connect concepts

### Quick Start

```bash
# Clone and navigate to project
cd kafka-realdata-pipeline

# Configure environment variables
cp .env.example .env
# Edit .env with your API keys and database credentials

# Start the complete ecosystem
docker-compose up -d

# Wait for all services to be ready
./scripts/wait-for-services.sh

# Deploy CDC connectors
./scripts/deploy-cdc-connectors.sh

# Deploy sink connectors
./scripts/deploy-sink-connectors.sh

# Start real-time data ingestion
./scripts/start-realtime-ingestion.sh

# Access the services
open http://localhost:9021    # Confluent Control Center
open http://localhost:8083    # Kafka Connect REST API
open http://localhost:3000    # Grafana Dashboard
```

### Service Endpoints

- **Confluent Control Center**: `http://localhost:9021`
- **Kafka Connect REST API**: `http://localhost:8083`
- **Schema Registry**: `http://localhost:8081`
- **Elasticsearch**: `http://localhost:9200`
- **Kibana**: `http://localhost:5601`
- **ClickHouse**: `http://localhost:8123`
- **Redis**: `localhost:6379`
- **Grafana Dashboard**: `http://localhost:3000` (admin/admin123)
- **Source MySQL**: `localhost:3306` (root/debezium)
- **Source PostgreSQL**: `localhost:5432` (postgres/postgres)

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                Multi-Source Real-Time Integration Architecture           │
├─────────────────────────────────────────────────────────────────────────┤
│  Source Systems        │  Kafka Connect        │  Target Systems        │
│  ├─ MySQL (CDC)        │  ├─ Debezium MySQL    │  ├─ ClickHouse         │
│  ├─ PostgreSQL (CDC)   │  ├─ Debezium Postgres │  ├─ Elasticsearch      │
│  ├─ MongoDB (CDC)      │  ├─ MongoDB CDC       │  ├─ Redis Cache        │
│  ├─ REST APIs          │  ├─ HTTP Connector     │  ├─ S3/MinIO           │
│  ├─ File Systems       │  ├─ File Connectors    │  └─ Time-series DB     │
│  └─ Message Queues     │  └─ Custom Connectors  │                        │
├─────────────────────────────────────────────────────────────────────────┤
│  Stream Processing     │  Schema Management     │  Monitoring            │
│  ├─ Kafka Streams      │  ├─ Schema Registry    │  ├─ Control Center     │
│  ├─ Data Validation    │  ├─ Avro Schemas       │  ├─ Prometheus         │
│  ├─ Transformations    │  ├─ JSON Schema        │  ├─ Grafana            │
│  └─ Enrichment         │  └─ Schema Evolution   │  └─ JMX Metrics        │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Examples

### 1. Database CDC Pipeline
```text
MySQL binlog → Debezium → Kafka Topic → ClickHouse Sink → Analytics Dashboard
     ↓            ↓          ↓             ↓                ↓
 Table Changes  CDC Events  Partitioned   Columnar     Real-time Queries
              (Avro)      Topics        Storage      & Visualizations
```

### 2. API Streaming Pipeline
```text
REST APIs → HTTP Source → Schema Registry → Stream Processing → Multiple Sinks
    ↓          ↓              ↓                ↓                   ↓
Stock/Social  JSON Events  Schema Validation  Enrichment      ES + Redis + S3
```

### 3. Multi-Sink Fan-out
```text
Single Kafka Topic → Multiple Sink Connectors
       ↓                        ↓
   Customer Data    ├─ Elasticsearch (Search)
                   ├─ Redis (Cache)
                   ├─ ClickHouse (Analytics)
                   └─ S3 (Archival)
```

## Connector Configurations

### Debezium MySQL CDC Connector
```json
{
  "name": "mysql-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql-source",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "database.server.name": "ecommerce",
    "table.include.list": "inventory.products,orders.orders,users.customers",
    "database.history.kafka.bootstrap.servers": "kafka:29092",
    "database.history.kafka.topic": "schema-changes.ecommerce",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

### ClickHouse Sink Connector
```json
{
  "name": "clickhouse-sink-connector",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "3",
    "topics": "ecommerce.inventory.products,ecommerce.orders.orders",
    "clickhouse.server.url": "http://clickhouse:8123",
    "clickhouse.server.database": "kafka_connect",
    "clickhouse.server.username": "default",
    "clickhouse.server.password": "",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

### Elasticsearch Sink Connector
```json
{
  "name": "elasticsearch-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "2",
    "topics": "ecommerce.orders.orders,api.social.tweets",
    "connection.url": "http://elasticsearch:9200",
    "type.name": "_doc",
    "key.ignore": "false",
    "schema.ignore": "false",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

## Real Data Sources Integration

### Financial Data (Alpha Vantage API)
- Stock prices, forex rates, cryptocurrency data
- Real-time and historical data ingestion
- Schema evolution for new financial instruments

### Social Media (Twitter API v2)
- Tweet streaming with hashtag filtering
- Sentiment analysis integration
- Real-time trend detection

### Weather Data (OpenWeatherMap API)
- Current weather and forecasts
- Historical weather data
- IoT sensor integration

### E-commerce APIs
- Product catalog updates
- Order status changes
- Customer behavior tracking

## Data Transformation Examples

### Single Message Transforms (SMT)
```json
{
  "transforms": "unwrap,addTimestamp",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.addTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.addTimestamp.timestamp.field": "processed_at"
}
```

### Custom Stream Processing
```java
KStream<String, OrderEvent> orders = builder.stream("ecommerce.orders.orders");
KStream<String, EnrichedOrder> enrichedOrders = orders
    .selectKey((key, order) -> order.getCustomerId())
    .join(customerTable, this::enrichWithCustomerData)
    .mapValues(this::calculateOrderMetrics);
```

## Schema Evolution Strategies

### Backward Compatibility
- Adding optional fields to existing schemas
- Removing fields (with default values)
- Changing field documentation

### Forward Compatibility
- Reading data with newer schemas
- Ignoring unknown fields
- Default value handling

### Full Compatibility
- Supporting both forward and backward compatibility
- Careful schema design principles
- Version management strategies

## Performance Optimization

### Connector Tuning
```json
{
  "tasks.max": "4",
  "batch.size": "2000",
  "linger.ms": "100",
  "buffer.memory": "67108864",
  "max.poll.records": "1000",
  "consumer.max.poll.interval.ms": "300000"
}
```

### Kafka Configuration
- Partition strategy optimization
- Replication factor tuning
- Log retention policies  
- Compression settings

### Target System Optimization
- Batch insert strategies
- Connection pooling
- Index optimization
- Partitioning strategies

## Monitoring & Observability

### Connector Metrics
- Processing throughput (records/second)
- Lag monitoring (source to sink delay)
- Error rates and failure analysis
- Schema registry usage statistics

### Data Quality Monitoring
- Record count validation
- Schema compliance checking
- Data freshness monitoring
- Duplicate detection

### Infrastructure Metrics
- Kafka broker health
- Connect worker status
- Target system performance
- Network and disk I/O

## Error Handling & Recovery

### Dead Letter Queues
```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "connect-dlq-topic",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

### Retry Strategies
- Exponential backoff configuration
- Maximum retry attempts
- Retry delay patterns
- Circuit breaker implementation

### Data Recovery
- Topic replay mechanisms
- Checkpoint management
- State store recovery
- Manual intervention procedures

## Security Configuration

### Authentication & Authorization
- SASL/SCRAM authentication
- SSL/TLS encryption
- Schema Registry security
- Connector authentication

### Data Protection
- Field-level encryption
- PII data handling
- GDPR compliance patterns
- Audit logging

## Future Expansion Directions

### 1. Advanced CDC Patterns
- **Multi-Master Replication**: Bidirectional CDC between databases
- **Cross-Region CDC**: Global data synchronization patterns
- **Schema Registry Federation**: Multi-cluster schema management
- **Conflict Resolution**: Handle concurrent updates across systems

### 2. Real-time Analytics Enhancement
- **Apache Flink Integration**: Complex event processing
- **Stream ML**: Real-time machine learning inference
- **Anomaly Detection**: Automated data quality monitoring
- **Time-series Analytics**: Advanced temporal data processing

### 3. Data Governance & Lineage
- **Data Lineage Tracking**: End-to-end data flow visualization
- **Data Catalog Integration**: Apache Atlas, DataHub integration
- **Policy Enforcement**: Automated compliance checking
- **Data Classification**: Sensitive data identification and handling

### 4. Performance & Scalability
- **Auto-scaling Connectors**: Dynamic resource allocation
- **Partition Management**: Automatic partition rebalancing
- **Compression Optimization**: Advanced compression strategies
- **Caching Strategies**: Multi-level caching implementation

### 5. Advanced Connectors
- **Cloud Native Connectors**: Kubernetes-native deployment
- **Custom Protocol Support**: WebSocket, gRPC, MQTT connectors
- **Legacy System Integration**: Mainframe, SAP connectors
- **Real-time APIs**: GraphQL subscription connectors

### 6. Operational Excellence
- **GitOps Integration**: Configuration as code
- **Blue-Green Deployments**: Zero-downtime connector updates
- **Chaos Engineering**: Fault injection testing
- **Performance Benchmarking**: Automated performance testing

### 7. Multi-Cloud & Hybrid
- **Cloud Provider Integration**: AWS MSK, Confluent Cloud
- **Hybrid Deployment**: On-premises to cloud streaming
- **Multi-Cloud Replication**: Cross-cloud data synchronization
- **Edge Computing**: IoT edge to cloud streaming

## Learning Resources

### Books
- "Kafka Connect - Building Real-Time Data Pipelines" by Robin Moffatt
- "Streaming Data" by Andrew Psaltis
- "Building Data Streaming Applications" by Valliappa Lakshmanan

### Online Courses
- [Confluent Developer Certification](https://developer.confluent.io/learn/)
- [Debezium Tutorial](https://debezium.io/documentation/tutorial/)
- [Kafka Connect Deep Dive](https://www.confluent.io/kafka-connect/)

### Documentation & Tutorials
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
- [Debezium Documentation](https://debezium.io/documentation/)
- [Confluent Platform Documentation](https://docs.confluent.io/)

---

**Master real-time data integration at enterprise scale! 🌊**
