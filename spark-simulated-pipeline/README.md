# Spark Simulated IoT Data Pipeline

## Project Description

This project demonstrates a comprehensive Apache Spark ecosystem for processing simulated IoT sensor data streams. It showcases real-time data ingestion, stream processing, batch analytics, and machine learning inference using synthetic but realistic sensor data from smart city infrastructure, industrial equipment, and environmental monitoring systems.

## Data Source & Simulation

- **Type**: Multi-domain IoT Sensor Simulation
- **Domains**: Smart city (traffic, air quality), Industrial IoT (machinery, temperature), Environmental (weather, energy)
- **Format**: JSON streaming via Kafka, Parquet batch files, Delta Lake tables
- **Volume**: 100K+ events/minute, 1M+ daily sensor readings
- **Schema**: 
  - **Traffic Sensors**: sensor_id, location, vehicle_count, avg_speed, timestamp, congestion_level
  - **Air Quality**: sensor_id, pm2_5, pm10, no2, o3, co, temperature, humidity, timestamp, location
  - **Industrial**: equipment_id, temperature, pressure, vibration, power_consumption, timestamp, status
  - **Weather**: station_id, temperature, humidity, wind_speed, precipitation, timestamp, coordinates
  - **Energy**: meter_id, consumption_kwh, voltage, current, power_factor, timestamp, building_id

## Tech Stack

- **Processing Engine**: Apache Spark 3.5.1 (Structured Streaming + Batch)
- **Streaming Platform**: Apache Kafka 3.6 with Schema Registry
- **Storage Layer**: Delta Lake 3.1, MinIO (S3-compatible)
- **Databases**: PostgreSQL (metadata), Redis (real-time cache)
- **Container Platform**: Docker & Docker Compose
- **Data Generation**: Python with realistic IoT patterns
- **Monitoring**: Spark UI, Kafka Manager, Grafana dashboards
- **Languages**: Scala (Spark jobs), Python (data generation), SQL (analytics)

## Project Purpose

- Master Apache Spark Structured Streaming fundamentals
- Learn real-time data processing patterns and best practices
- Understand stream-batch lambda architecture implementation
- Practice data quality validation and anomaly detection
- Explore machine learning on streaming data (online learning)
- Implement exactly-once processing guarantees
- Learn performance tuning for high-throughput streaming
- Understand checkpoint management and failure recovery

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- At least 6GB RAM available for containers
- Java 11+ and Python 3.8+ (for local development)
- Basic understanding of Apache Spark concepts

### Quick Start

```bash
# Clone and navigate to project
cd spark-simulated-pipeline

# Start the complete ecosystem
docker-compose up -d

# Wait for all services to be ready
./scripts/wait-for-services.sh

# Start IoT data simulation
./scripts/start-data-simulation.sh

# Deploy Spark streaming jobs
./scripts/deploy-streaming-jobs.sh

# Start batch processing jobs
./scripts/start-batch-processing.sh

# Access the services
open http://localhost:4040    # Spark UI
open http://localhost:9021    # Kafka Control Center
open http://localhost:3000    # Grafana Dashboard
```

### Service Endpoints

- **Spark Master UI**: `http://localhost:8080`
- **Spark Application UI**: `http://localhost:4040`
- **Kafka Control Center**: `http://localhost:9021`
- **MinIO Console**: `http://localhost:9001` (admin/minioadmin123)
- **Grafana Dashboard**: `http://localhost:3000` (admin/admin123)
- **PostgreSQL**: `localhost:5432` (spark_user/spark_password)
- **Redis**: `localhost:6379`

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│                    Spark IoT Streaming Architecture                  │
├─────────────────────────────────────────────────────────────────────┤
│  Data Sources          │  Stream Processing     │  Storage Layer     │
│  ├─ IoT Simulators     │  ├─ Spark Streaming    │  ├─ Delta Lake     │
│  ├─ Kafka Topics       │  ├─ Real-time ML       │  ├─ MinIO S3       │
│  ├─ Schema Registry    │  ├─ Aggregations       │  ├─ PostgreSQL     │
│  └─ Event Generation   │  └─ Quality Checks     │  └─ Redis Cache    │
├─────────────────────────────────────────────────────────────────────┤
│  Batch Processing      │  Analytics Layer       │  Monitoring        │
│  ├─ Scheduled Jobs     │  ├─ SQL Analytics      │  ├─ Spark UI       │
│  ├─ Data Validation    │  ├─ ML Training        │  ├─ Kafka Manager  │
│  ├─ ETL Pipelines      │  ├─ Reporting          │  ├─ Grafana        │
│  └─ Model Training     │  └─ Dashboards         │  └─ Prometheus     │
└─────────────────────────────────────────────────────────────────────┘
```

## Core Learning Exercises

### Exercise 1: Real-Time Anomaly Detection
Implement streaming anomaly detection for sensor data:
```bash
# Start the anomaly detection job
./scripts/run-anomaly-detection.sh

# Monitor alerts in real-time
./scripts/monitor-anomalies.sh

# Analyze anomaly patterns
./scripts/analyze-anomaly-patterns.sh
```

### Exercise 2: Stream-Batch Integration
Practice lambda architecture patterns:
```bash
# Run speed layer (streaming)
./scripts/run-speed-layer.sh

# Run batch layer processing
./scripts/run-batch-layer.sh

# Merge results in serving layer
./scripts/run-serving-layer.sh
```

### Exercise 3: Performance Optimization
Optimize Spark streaming performance:
```bash
# Baseline performance test
./scripts/benchmark-baseline.sh

# Apply optimizations
./scripts/apply-optimizations.sh

# Compare performance improvements
./scripts/compare-performance.sh
```

### Exercise 4: Exactly-Once Processing
Implement exactly-once semantics:
```bash
# Test normal processing
./scripts/test-normal-processing.sh

# Simulate failures
./scripts/simulate-failures.sh

# Verify exactly-once guarantees
./scripts/verify-exactly-once.sh
```

## Advanced Features

### Machine Learning Integration
- **Online Learning**: Update ML models with streaming data
- **Feature Engineering**: Real-time feature computation
- **Model Serving**: Low-latency prediction serving
- **A/B Testing**: Compare model performance in real-time

### Data Quality & Monitoring
- **Schema Evolution**: Handle changing data schemas
- **Data Validation**: Real-time quality checks
- **Drift Detection**: Monitor data and model drift
- **Alerting**: Automated anomaly and failure alerts

### Performance & Scalability
- **Dynamic Scaling**: Auto-scale based on throughput
- **Resource Optimization**: Memory and CPU tuning
- **Checkpointing**: Optimize checkpoint intervals
- **Backpressure**: Handle varying data rates

## Sample Analytics Queries

### Real-Time Dashboard Queries
```sql
-- Traffic congestion by location (last 5 minutes)
SELECT location, AVG(congestion_level) as avg_congestion
FROM traffic_stream 
WHERE timestamp > current_timestamp - INTERVAL 5 MINUTES
GROUP BY location
ORDER BY avg_congestion DESC;

-- Air quality alerts (exceeding thresholds)
SELECT sensor_id, location, pm2_5, timestamp
FROM air_quality_stream
WHERE pm2_5 > 35 OR pm10 > 150
ORDER BY timestamp DESC;

-- Industrial equipment anomalies
SELECT equipment_id, temperature, pressure, 
       anomaly_score, timestamp
FROM industrial_anomalies
WHERE anomaly_score > 0.8
ORDER BY timestamp DESC;
```

### Batch Analytics Examples
```sql
-- Daily energy consumption patterns
SELECT building_id, 
       DATE(timestamp) as date,
       AVG(consumption_kwh) as avg_consumption,
       MAX(consumption_kwh) as peak_consumption
FROM energy_readings
GROUP BY building_id, DATE(timestamp)
ORDER BY date DESC, avg_consumption DESC;

-- Weather correlation analysis
SELECT DATE(timestamp) as date,
       CORR(temperature, energy_consumption) as temp_energy_corr,
       CORR(humidity, air_quality_pm2_5) as humidity_air_corr
FROM combined_environmental_data
GROUP BY DATE(timestamp)
ORDER BY date DESC;
```

## Future Expansion Directions with Curated Resources

### 1. Advanced Stream Processing Patterns

**Learning Resources:**
- [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/): Comprehensive streaming processing guide
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html): Official documentation
- [Event-Driven Architecture Patterns](https://www.oreilly.com/library/view/building-event-driven/9781492057888/): Event processing patterns
- [Kafka Streams in Action](https://www.manning.com/books/kafka-streams-in-action): Advanced Kafka streaming patterns

**Implementation Projects:**
- Complex event processing (CEP) for IoT pattern recognition
- Multi-stream joins with event-time processing
- Late-arriving data handling with watermarks
- Stream-stream and stream-table joins optimization

### 2. Real-Time Machine Learning and MLOps

**ML Resources:**
- [Stream Processing with Apache Spark](https://www.oreilly.com/library/view/stream-processing-with/9781491944233/): ML on streaming data
- [MLOps Engineering at Scale](https://www.manning.com/books/mlops-engineering-at-scale): Production ML systems
- [Online Machine Learning](https://www.cambridge.org/core/books/online-machine-learning/): Streaming ML algorithms
- [Apache Spark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html): Spark ML documentation

**ML Enhancement Projects:**
- Online learning algorithms for concept drift adaptation
- Real-time feature stores with streaming feature engineering
- Model serving with sub-second latency requirements
- A/B testing frameworks for model comparison in production

### 3. Advanced Data Lake Architecture with Delta Lake

**Data Lake Resources:**
- [Delta Lake: The Definitive Guide](https://www.oreilly.com/library/view/delta-lake-the/9781098104825/): Complete Delta Lake implementation
- [Lakehouse Architecture](https://databricks.com/research/lakehouse): Next-generation data architecture
- [Apache Iceberg Documentation](https://iceberg.apache.org/): Alternative table format
- [Data Mesh Principles](https://martinfowler.com/articles/data-mesh-principles.html): Distributed data architecture

**Data Architecture Projects:**
- Multi-table ACID transactions across streaming updates
- Time travel and data versioning for IoT historical analysis
- Efficient small file handling for high-frequency sensor data
- Cross-region data replication and disaster recovery strategies

### 4. Enterprise-Grade Monitoring and Observability

**Observability Resources:**
- [Observability Engineering](https://www.oreilly.com/library/view/observability-engineering/9781492076438/): Modern observability practices
- [Prometheus Monitoring Guide](https://prometheus.io/docs/): Metrics collection and alerting
- [Distributed Tracing in Practice](https://www.oreilly.com/library/view/distributed-tracing-in/9781492056621/): Request tracing patterns
- [SRE Workbook](https://sre.google/workbook/table-of-contents/): Site reliability engineering

**Monitoring Enhancement Projects:**
- Custom Spark metrics collection and visualization
- Distributed tracing across streaming pipeline stages
- Intelligent alerting with machine learning-based anomaly detection
- Performance profiling and optimization recommendation systems

### 5. Cloud-Native Deployment and Kubernetes Integration

**Cloud-Native Resources:**
- [Kubernetes Operators](https://www.oreilly.com/library/view/kubernetes-operators/9781492048039/): Operator pattern implementation
- [Spark on Kubernetes Guide](https://spark.apache.org/docs/latest/running-on-kubernetes.html): Official Kubernetes integration
- [Cloud Native Patterns](https://www.manning.com/books/cloud-native-patterns): Cloud-native design patterns
- [GitOps Principles](https://www.gitops.tech/): GitOps deployment strategies

**Cloud Deployment Projects:**
- Kubernetes-native Spark operator development
- Auto-scaling streaming applications based on Kafka lag
- Multi-cloud deployment with disaster recovery capabilities
- Cost optimization strategies for cloud-based streaming workloads

### 6. Advanced Data Security and Compliance

**Security Resources:**
- [Data Security in Modern Architectures](https://www.oreilly.com/library/view/data-security-in/9781492067142/): Data protection strategies
- [Apache Ranger Documentation](https://ranger.apache.org/): Fine-grained access control
- [Zero Trust Data Architecture](https://www.nist.gov/publications/zero-trust-architecture): NIST security framework
- [GDPR Compliance Guide](https://gdpr.eu/): Privacy regulation compliance

**Security Implementation Projects:**
- End-to-end encryption for streaming data pipelines
- Fine-grained access control with attribute-based policies
- Data anonymization and pseudonymization for IoT data
- Audit logging and compliance reporting automation

### 7. Edge Computing and IoT Integration

**Edge Computing Resources:**
- [Edge Computing Patterns](https://www.oreilly.com/library/view/learning-iot/9781491934135/): IoT and edge architectures
- [Apache EdgeX Foundry](https://www.edgexfoundry.org/): Edge computing framework
- [Spark on Edge Devices](https://spark.apache.org/docs/latest/): Lightweight Spark deployment
- [Industrial IoT Architectures](https://www.microsoft.com/en-us/internet-of-things/): Enterprise IoT patterns

**Edge Integration Projects:**
- Lightweight Spark deployment on edge devices
- Hierarchical data processing (edge → fog → cloud)
- Offline-capable streaming with eventual consistency
- Edge-to-cloud data synchronization strategies

### 8. Time Series Analytics and Forecasting

**Time Series Resources:**
- [Time Series Analysis and Forecasting](https://otexts.com/fpp3/): Statistical forecasting methods
- [Prophet Forecasting](https://facebook.github.io/prophet/): Automated time series forecasting
- [Apache Spark Time Series](https://github.com/twosigma/flint): Time series library for Spark
- [InfluxDB Time Series Guide](https://docs.influxdata.com/): Time series database patterns

**Time Series Projects:**
- Real-time forecasting for IoT sensor values
- Seasonal decomposition and trend analysis
- Multi-variate time series analysis with cross-correlation
- Automated anomaly detection using statistical methods

### 9. Data Visualization and Business Intelligence

**Visualization Resources:**
- [Apache Superset Documentation](https://superset.apache.org/): Open-source BI platform
- [Real-time Dashboard Design](https://www.tableau.com/learn/articles/dashboard-design-best-practices): Dashboard best practices
- [D3.js for Real-time Visualization](https://observablehq.com/@d3/): Interactive visualizations
- [Streaming Analytics Dashboards](https://grafana.com/): Real-time monitoring dashboards

**Visualization Projects:**
- Real-time IoT dashboard with WebSocket updates
- Interactive time series exploration interfaces
- Geospatial visualization for location-based sensor data
- Custom alerting and notification systems

### 10. Performance Engineering and Cost Optimization

**Performance Resources:**
- [Spark Performance Tuning Guide](https://spark.apache.org/docs/latest/tuning.html): Official performance optimization
- [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/): Advanced performance techniques
- [Cost Optimization Strategies](https://aws.amazon.com/blogs/big-data/): Cloud cost management
- [Streaming Performance Patterns](https://www.confluent.io/blog/): Kafka and streaming optimization

**Performance Projects:**
- Intelligent resource allocation based on workload characteristics
- Cost-aware scheduling with spot instances and preemptible VMs
- Performance regression testing for streaming applications
- Automated performance tuning using machine learning

## Data Pipeline Stages

1. **Data Generation**: Realistic IoT sensor simulation with configurable patterns
2. **Stream Ingestion**: High-throughput Kafka ingestion with schema validation
3. **Real-time Processing**: Spark Structured Streaming with exactly-once semantics
4. **Batch Processing**: Scheduled ETL jobs for historical analysis
5. **Storage**: Multi-tier storage strategy (hot/warm/cold data)
6. **Analytics**: SQL analytics and machine learning inference
7. **Visualization**: Real-time dashboards and alerting systems

## Performance Metrics

- **Streaming Throughput**: 100K+ events/second processing capability
- **End-to-End Latency**: Sub-second processing for real-time alerts
- **Exactly-Once Guarantee**: Zero data loss or duplication
- **Availability**: 99.9%+ uptime with automatic failure recovery
- **Storage Efficiency**: 80%+ compression with Delta Lake optimization

## Learning Outcomes

- **Spark Streaming Mastery**: Structured Streaming, watermarks, triggers
- **Lambda Architecture**: Speed/batch/serving layer implementation
- **Data Quality**: Validation, monitoring, and alerting patterns
- **Performance Tuning**: Memory management, partitioning, caching
- **MLOps Integration**: Streaming ML pipelines and model serving
- **Production Patterns**: Monitoring, alerting, and failure recovery
