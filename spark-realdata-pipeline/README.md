# Spark Real-Time Analytics Pipeline

## Project Description

This project demonstrates an enterprise-grade real-time analytics platform using Apache Spark with live data sources. It showcases advanced streaming analytics, machine learning inference, real-time feature engineering, and interactive dashboards using real-world APIs including financial markets, social media, weather data, and e-commerce platforms.

## Data Sources & Integration

- **Type**: Live Production APIs and Streaming Services
- **Sources**: 
  - **Financial**: Yahoo Finance, Alpha Vantage (stock prices, forex, crypto)
  - **Social Media**: Twitter API v2, Reddit API (sentiment analysis)
  - **Weather**: OpenWeatherMap, Weather.gov (environmental data)
  - **E-commerce**: Shopify, WooCommerce webhooks (transaction streams)
  - **IoT**: Public sensor APIs (air quality, traffic, energy)
- **Format**: JSON streaming, REST APIs, WebSockets, Kafka events
- **Volume**: 50K+ events/second peak, 5M+ daily events
- **Processing**: Real-time feature engineering, ML inference, anomaly detection

## Tech Stack

- **Processing Engine**: Apache Spark 3.5.1 (Structured Streaming + MLlib)
- **Streaming Platform**: Apache Kafka 3.6 + Kafka Connect + KSQL
- **Machine Learning**: Spark MLlib, TensorFlow Serving, MLflow
- **Storage**: Delta Lake 3.1, Redis Streams, Apache Cassandra
- **APIs**: FastAPI (Python), Spring Boot (Java) microservices
- **Container Platform**: Docker Compose + Kubernetes deployment
- **Monitoring**: Prometheus, Grafana, Jaeger, ELK Stack
- **Notebooks**: JupyterLab with Spark integration
- **Languages**: Scala (Spark), Python (ML/APIs), SQL (analytics)

## Project Purpose

- Build production-ready streaming analytics platform
- Master real-time machine learning inference and online learning
- Implement complex event processing and stream joins
- Learn advanced Kafka ecosystem (Connect, KSQL, Schema Registry)
- Practice microservices architecture for data platforms
- Understand real-time feature stores and model serving
- Implement comprehensive monitoring and alerting
- Learn cost optimization for cloud streaming workloads

## Setup Instructions

### Prerequisites

- Docker and Docker Compose (Docker Desktop recommended)
- At least 8GB RAM available for containers
- API keys for external services (provided in `.env.example`)
- Basic understanding of streaming analytics and ML concepts

### Quick Start

```bash
# Clone and navigate to project
cd spark-realdata-pipeline

# Setup environment variables
cp .env.example .env
# Edit .env with your API keys

# Start the complete platform
docker-compose up -d

# Wait for all services to be ready
./scripts/wait-for-services.sh

# Initialize data sources and schema
./scripts/setup-data-sources.sh

# Deploy streaming analytics jobs
./scripts/deploy-analytics-jobs.sh

# Start ML model serving
./scripts/start-model-serving.sh

# Access the platform
open http://localhost:8888    # JupyterLab
open http://localhost:3000    # Grafana Dashboard
open http://localhost:8080    # Spark UI
```

### Service Endpoints

- **JupyterLab**: `http://localhost:8888` (token: spark-analytics)
- **Spark Master UI**: `http://localhost:8080`
- **Kafka Control Center**: `http://localhost:9021`
- **Grafana Dashboards**: `http://localhost:3000` (admin/admin123)
- **MLflow UI**: `http://localhost:5000`
- **FastAPI Documentation**: `http://localhost:8000/docs`
- **Cassandra**: `localhost:9042`
- **Redis**: `localhost:6379`

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│                Real-Time Analytics Platform Architecture             │
├─────────────────────────────────────────────────────────────────────┤
│  External APIs        │  Ingestion Layer      │  Stream Processing   │
│  ├─ Financial APIs    │  ├─ Kafka Connect     │  ├─ Spark Streaming  │
│  ├─ Social Media      │  ├─ API Gateways      │  ├─ KSQL Processing  │
│  ├─ Weather Services  │  ├─ WebSocket Streams │  ├─ Complex Events   │
│  └─ IoT Sensors       │  └─ Schema Registry   │  └─ ML Inference     │
├─────────────────────────────────────────────────────────────────────┤
│  Feature Store        │  Model Serving        │  Analytics Layer     │
│  ├─ Real-time Features│  ├─ TensorFlow Serve  │  ├─ Interactive SQL  │
│  ├─ Historical Store  │  ├─ MLflow Models     │  ├─ Real-time Dash   │
│  ├─ Feature Pipeline  │  ├─ A/B Testing      │  ├─ Alerting Engine  │
│  └─ Delta Lake Store  │  └─ Model Monitoring  │  └─ Business Reports │
└─────────────────────────────────────────────────────────────────────┘
```

## Core Use Cases & Analytics

### Use Case 1: Real-Time Financial Analytics
- **Stock price momentum detection**
- **Forex arbitrage opportunity identification**
- **Crypto market sentiment correlation**
- **Risk management and position sizing**

### Use Case 2: Social Media Sentiment Analysis
- **Brand sentiment tracking across platforms**
- **Viral content early detection**
- **Influencer impact measurement**
- **Crisis communication response**

### Use Case 3: Weather-Driven Business Intelligence
- **Retail demand forecasting based on weather**
- **Energy consumption prediction**
- **Agricultural yield optimization**
- **Supply chain weather impact analysis**

### Use Case 4: E-commerce Real-Time Personalization
- **Dynamic pricing optimization**
- **Inventory level alerting**
- **Customer behavior pattern recognition**
- **Fraud detection and prevention**

## Advanced Analytics Examples

### Real-Time Feature Engineering
```python
# Real-time moving averages for stock prices
stock_features = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-prices") \
    .load() \
    .select(
        col("key").cast("string").alias("symbol"),
        from_json(col("value").cast("string"), stock_schema).alias("data"),
        col("timestamp")
    ) \
    .select("symbol", "data.*", "timestamp") \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("symbol")
    ) \
    .agg(
        avg("price").alias("avg_price_5min"),
        stddev("price").alias("price_volatility"),
        max("volume").alias("max_volume"),
        count("*").alias("trade_count")
    )
```

### Machine Learning Pipeline
```python
# Real-time sentiment classification
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

# Build ML pipeline
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)

pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

# Apply to streaming data
sentiment_stream = social_media_stream.select("text", "platform", "timestamp")
predictions = pipeline.fit(training_data).transform(sentiment_stream)
```

### Complex Event Processing
```scala
// Detect trading patterns using CEP
val pattern = Pattern.begin[StockEvent]("start")
  .where(_.eventType == "BUY")
  .next("middle")
  .where(_.price > _.previous.price * 1.05)
  .followedBy("end")
  .where(_.eventType == "SELL")
  .within(Time.minutes(15))

val patternStream = CEP.pattern(stockStream, pattern)
val alerts = patternStream.select("start", "middle", "end") { pattern =>
  TradingAlert(
    symbol = pattern.get("start").symbol,
    pattern = "MOMENTUM_REVERSAL",
    confidence = calculateConfidence(pattern),
    timestamp = System.currentTimeMillis()
  )
}
```

## Machine Learning Integration

### Model Types & Applications
- **Time Series Forecasting**: ARIMA, Prophet, LSTM for price prediction
- **Classification**: Sentiment analysis, fraud detection, customer segmentation
- **Anomaly Detection**: Isolation Forest, AutoEncoders for outlier detection
- **Recommendation Systems**: Collaborative filtering, matrix factorization
- **Clustering**: K-means, DBSCAN for customer/content grouping

### MLOps Workflow
1. **Feature Engineering**: Real-time feature computation and storage
2. **Model Training**: Automated retraining with new data
3. **Model Validation**: A/B testing and performance monitoring
4. **Model Deployment**: Canary releases with rollback capabilities
5. **Model Monitoring**: Drift detection and performance alerts

## Performance Optimization Features

### Streaming Optimizations
- **Adaptive Query Execution**: Dynamic partition coalescing
- **Watermark Management**: Optimal late data handling
- **Checkpointing**: Tuned checkpoint intervals for performance
- **Resource Management**: Dynamic allocation based on throughput

### Storage Optimizations
- **Delta Lake**: ACID transactions, time travel, schema evolution
- **Partitioning**: Intelligent partitioning strategies
- **Compression**: Optimized compression algorithms
- **Caching**: Multi-level caching (Spark, Redis, application)

## Monitoring & Observability

### Key Metrics Dashboard
- **Throughput**: Events/second, latency percentiles
- **Resource Utilization**: CPU, memory, network usage
- **Data Quality**: Schema validation, null rates, outliers
- **Model Performance**: Accuracy, drift detection, prediction latency
- **Business KPIs**: Revenue impact, user engagement, operational efficiency

### Alerting Rules
```yaml
# Example Prometheus alerting rules
groups:
  - name: spark-streaming-alerts
    rules:
      - alert: HighStreamingLatency
        expr: spark_streaming_batch_processing_time > 30000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High streaming latency detected"
          
      - alert: ModelDriftDetected
        expr: model_drift_score > 0.8
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Model performance drift detected"
```

## Future Expansion Directions with Curated Resources

### 1. Advanced MLOps and Model Lifecycle Management

**MLOps Resources:**
- [MLOps Engineering at Scale](https://www.manning.com/books/mlops-engineering-at-scale): Production ML systems
- [Machine Learning Design Patterns](https://www.oreilly.com/library/view/machine-learning-design/9781098115777/): ML engineering patterns
- [Building Machine Learning Powered Applications](https://www.oreilly.com/library/view/building-machine-learning/9781492045106/): End-to-end ML applications
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html): Complete MLflow implementation guide

**MLOps Enhancement Projects:**
- Automated model retraining pipelines with concept drift detection
- Multi-armed bandit testing for model selection in production
- Federated learning for privacy-preserving model training
- Model interpretability and explainability dashboards for business users

### 2. Real-Time Feature Stores and Data Mesh Architecture

**Feature Store Resources:**
- [Feature Store for ML](https://www.oreilly.com/library/view/feature-store-for/9781492052593/): Comprehensive feature store guide
- [Data Mesh Principles](https://martinfowler.com/articles/data-mesh-principles.html): Decentralized data architecture
- [Feast Feature Store](https://feast.dev/): Open-source feature store implementation
- [Tecton Feature Platform](https://www.tecton.ai/blog/what-is-a-feature-store/): Enterprise feature store patterns

**Architecture Enhancement Projects:**
- Domain-oriented feature ownership with self-serve feature discovery
- Real-time feature serving with sub-millisecond latency requirements
- Feature lineage tracking and impact analysis across ML models
- Cross-team feature sharing with governance and access control policies

### 3. Advanced Stream Processing and Complex Event Processing

**Stream Processing Resources:**
- [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/): Advanced streaming concepts
- [Apache Flink Documentation](https://flink.apache.org/): Alternative streaming engine
- [Complex Event Processing](https://www.oreilly.com/library/view/complex-event-processing/9780596516215/): CEP patterns and implementation
- [Kafka Streams in Action](https://www.manning.com/books/kafka-streams-in-action): Advanced Kafka streaming

**Stream Processing Projects:**
- Multi-stream temporal joins with event-time processing
- Stateful stream processing with exactly-once semantics
- Session-based analytics with dynamic session windows
- Stream-to-stream and stream-to-table joins optimization

### 4. Cloud-Native Deployment and Kubernetes Orchestration

**Cloud-Native Resources:**
- [Kubernetes in Action](https://www.manning.com/books/kubernetes-in-action-second-edition): Complete Kubernetes guide
- [Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html): Official Kubernetes integration
- [Cloud Native Patterns](https://www.manning.com/books/cloud-native-patterns): Cloud-native design patterns
- [Istio Service Mesh](https://istio.io/latest/docs/): Microservices networking and security

**Cloud Deployment Projects:**
- Kubernetes-native Spark operator with custom resource definitions
- Auto-scaling streaming applications based on Kafka consumer lag
- Multi-region deployment with disaster recovery capabilities
- Service mesh integration for secure microservices communication

### 5. Advanced Analytics and Business Intelligence Integration

**Analytics Resources:**
- [Apache Superset](https://superset.apache.org/): Modern data visualization platform
- [dbt (data build tool)](https://docs.getdbt.com/): Analytics engineering workflows
- [Metabase](https://www.metabase.com/): Open-source business intelligence
- [Modern Data Stack](https://www.getdbt.com/analytics-engineering/): Analytics engineering best practices

**BI Enhancement Projects:**
- Self-service analytics platform with drag-and-drop interface
- Automated insight generation using NLP and statistical analysis
- Real-time collaborative dashboards with commenting and alerting
- Embedded analytics SDK for external applications and customer portals

### 6. Data Security, Privacy, and Compliance

**Security Resources:**
- [Data Security in the Cloud](https://www.oreilly.com/library/view/data-security-in/9781492067142/): Cloud data protection
- [Differential Privacy](https://programming-dp.com/): Privacy-preserving analytics
- [Apache Ranger](https://ranger.apache.org/): Data governance and security
- [GDPR Compliance Guide](https://gdpr.eu/): Privacy regulation implementation

**Security Implementation Projects:**
- End-to-end encryption for streaming data with key rotation
- Differential privacy implementation for sensitive analytics
- Fine-grained access control with attribute-based policies
- Data masking and anonymization for non-production environments

### 7. Cost Optimization and Resource Management

**Cost Optimization Resources:**
- [Cloud FinOps](https://www.finops.org/): Financial operations for cloud computing
- [Spot Instance Best Practices](https://aws.amazon.com/ec2/spot/getting-started/): Cost-effective compute strategies
- [Kubernetes Resource Management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/): Resource optimization
- [Apache Spark Cost Optimization](https://spark.apache.org/docs/latest/tuning.html): Spark performance tuning

**Cost Management Projects:**
- Intelligent workload scheduling with spot instance integration
- Resource rightsizing based on historical usage patterns
- Cost attribution and chargeback systems for data consumers
- Automated resource cleanup and lifecycle management policies

### 8. Edge Computing and IoT Integration

**Edge Computing Resources:**
- [Edge AI and IoT](https://www.oreilly.com/library/view/learning-iot/9781491934135/): Edge computing architectures
- [Apache EdgeX Foundry](https://www.edgexfoundry.org/): Industrial IoT edge platform
- [KubeEdge](https://kubeedge.io/): Kubernetes-based edge computing
- [Azure IoT Edge](https://docs.microsoft.com/en-us/azure/iot-edge/): Cloud-to-edge deployment

**Edge Integration Projects:**
- Hierarchical stream processing from edge to cloud
- Offline-capable analytics with eventual consistency
- Edge-based model inference with cloud model management
- Device fleet management and remote deployment capabilities

### 9. Graph Analytics and Network Analysis

**Graph Analytics Resources:**
- [Graph Analytics for Big Data](https://www.manning.com/books/graph-analytics-for-big-data): Graph processing patterns
- [Apache Spark GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html): Graph processing with Spark
- [Neo4j Graph Database](https://neo4j.com/docs/): Native graph database integration
- [Network Analysis and Mining](https://www.cambridge.org/core/books/networks-crowds-and-markets/): Social network analysis

**Graph Analytics Projects:**
- Social network influence analysis and community detection
- Financial transaction network fraud detection
- Supply chain risk analysis using graph algorithms
- Knowledge graph construction from streaming text data

### 10. Advanced Time Series Analytics and Forecasting

**Time Series Resources:**
- [Time Series Analysis and Forecasting](https://otexts.com/fpp3/): Statistical forecasting methods
- [Prophet Documentation](https://facebook.github.io/prophet/): Automated forecasting tool
- [Apache Spark Time Series](https://github.com/twosigma/flint): Time series analytics library
- [InfluxDB Time Series Platform](https://docs.influxdata.com/): Time series database integration

**Time Series Projects:**
- Multi-variate time series forecasting with external regressors
- Seasonal decomposition and trend analysis for business metrics
- Anomaly detection in time series using statistical and ML methods
- Real-time forecasting with automated model selection and hyperparameter tuning

## Real-Time Use Case Examples

### Financial Trading Analytics
```sql
-- Real-time stock momentum detection
SELECT 
    symbol,
    timestamp,
    price,
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price,
    (price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp)) / 
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) * 100 as price_change_pct,
    AVG(price) OVER (PARTITION BY symbol ORDER BY timestamp 
                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
    volume,
    CASE 
        WHEN price > AVG(price) OVER (PARTITION BY symbol ORDER BY timestamp 
                                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) 
        THEN 'BUY_SIGNAL'
        ELSE 'HOLD'
    END as trading_signal
FROM stock_stream
WHERE timestamp > current_timestamp - INTERVAL 1 HOUR
```

### Social Media Sentiment Correlation
```sql
-- Correlate social sentiment with stock price movements
SELECT 
    s.symbol,
    s.timestamp,
    s.price_change_pct,
    sm.sentiment_score,
    sm.mention_count,
    CORR(s.price_change_pct, sm.sentiment_score) OVER (
        PARTITION BY s.symbol 
        ORDER BY s.timestamp 
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) as sentiment_price_correlation
FROM stock_stream s
JOIN social_sentiment_stream sm 
    ON s.symbol = sm.symbol 
    AND s.timestamp BETWEEN sm.timestamp - INTERVAL 5 MINUTES 
                        AND sm.timestamp + INTERVAL 5 MINUTES
```

## Performance Targets

- **Stream Processing**: 50K+ events/second sustained throughput
- **End-to-End Latency**: <100ms for real-time alerts
- **ML Inference**: <10ms prediction latency for online models
- **Data Freshness**: <30 seconds from source to dashboard
- **Availability**: 99.95% uptime with automatic failover
- **Cost Efficiency**: 60% cost reduction through optimization

## Learning Outcomes

- **Production Streaming**: Enterprise-grade streaming architecture patterns
- **Real-Time ML**: Online learning, model serving, and MLOps practices
- **Complex Analytics**: Multi-stream joins, CEP, and advanced aggregations
- **Platform Engineering**: Microservices, APIs, and self-service capabilities
- **Observability**: Comprehensive monitoring, alerting, and troubleshooting
- **Cloud Integration**: Multi-cloud deployment and cost optimization strategies
