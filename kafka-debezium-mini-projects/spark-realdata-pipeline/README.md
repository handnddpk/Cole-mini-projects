# Spark Multi-Source Real-Time Analytics Pipeline

## Project Description

This project demonstrates a production-ready big data analytics platform using Apache Spark with Structured Streaming, Delta Lake, and various data sources. It showcases real-time data processing, batch analytics, machine learning pipelines, and advanced data lake architectures with schema evolution, ACID transactions, and comprehensive monitoring.

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                   Spark Real-Time Analytics Platform                    │
├─────────────────────────────────────────────────────────────────────────┤
│  Data Sources          │  Spark Processing     │  Data Lake & Serving   │
│  ├─ Kafka Streams      │  ├─ Structured Stream │  ├─ Delta Lake         │
│  ├─ Database CDC       │  ├─ Batch Processing  │  ├─ Parquet Files      │
│  ├─ REST APIs          │  ├─ ML Pipelines      │  ├─ Feature Store      │
│  ├─ File Systems       │  ├─ Graph Analytics   │  ├─ Data Warehouse     │
│  ├─ Message Queues     │  └─ Stream Analytics  │  └─ Serving Layer      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Sources

- **Streaming Sources**: Kafka, Kinesis, Pulsar, Event Hubs
- **Database Sources**: MySQL, PostgreSQL, MongoDB, Cassandra  
- **File Sources**: S3, HDFS, Azure Blob, GCS (Parquet, Delta, JSON, CSV)
- **API Sources**: REST APIs, GraphQL, WebSocket streams
- **Message Systems**: RabbitMQ, ActiveMQ, Apache Pulsar

## Processing Capabilities

- **Streaming Analytics**: Real-time aggregations, windowing, watermarking
- **Batch Processing**: ETL/ELT, data quality, complex transformations
- **Machine Learning**: MLlib, distributed training, feature engineering
- **Graph Analytics**: GraphX, network analysis, community detection
- **Time Series**: Temporal analytics, forecasting, anomaly detection

## Tech Stack

- **Processing Engine**: Apache Spark 3.5 with Scala/Python
- **Storage Layer**: Delta Lake with ACID transactions
- **Streaming**: Structured Streaming with Kafka integration
- **ML Framework**: MLlib, Spark ML pipelines
- **Orchestration**: Apache Airflow, Kubernetes
- **Monitoring**: Spark UI, Prometheus, Grafana
- **Development**: Jupyter, Databricks, Spark Shell

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- At least 16GB RAM available
- Java 11+ and Scala 2.12+
- Python 3.8+ with PySpark
- Apache Spark 3.5+

### Quick Start

```bash
# Navigate to project
cd spark-realdata-pipeline

# Start the complete ecosystem
docker-compose up -d

# Wait for services to be ready
./scripts/wait-for-services.sh

# Initialize Delta Lake tables
./scripts/setup-delta-tables.sh

# Start streaming jobs
./scripts/start-streaming-jobs.sh

# Launch batch processing
./scripts/run-batch-analytics.sh

# Access UIs
open http://localhost:4040    # Spark UI
open http://localhost:8080    # Spark Master UI
open http://localhost:3000    # Grafana Dashboard
open http://localhost:8888    # Jupyter Notebook
```

### Service Endpoints

- **Spark Master UI**: `http://localhost:8080`
- **Spark Applications**: `http://localhost:4040-4050`
- **Jupyter Notebook**: `http://localhost:8888`
- **MinIO Console**: `http://localhost:9001`
- **Grafana Dashboard**: `http://localhost:3000`
- **Airflow**: `http://localhost:8081`

## Project Structure

```
spark-realdata-pipeline/
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── streaming/           # Structured Streaming jobs
│   │   │   ├── batch/              # Batch processing jobs  
│   │   │   ├── ml/                 # Machine learning pipelines
│   │   │   └── utils/              # Common utilities
│   │   └── python/
│   │       ├── streaming/          # PySpark streaming jobs
│   │       ├── ml/                 # Python ML pipelines
│   │       └── notebooks/          # Jupyter notebooks
├── config/
│   ├── spark-defaults.conf
│   ├── log4j.properties
│   └── delta-lake.conf
├── data/
│   ├── raw/                        # Raw data ingestion
│   ├── bronze/                     # Bronze layer (raw data)
│   ├── silver/                     # Silver layer (cleaned data)
│   └── gold/                       # Gold layer (aggregated data)
├── scripts/
│   ├── setup-cluster.sh
│   ├── submit-jobs.sh
│   └── monitoring.sh
├── docker/
│   ├── spark/
│   ├── jupyter/
│   └── airflow/
└── docker-compose.yml
```

## Data Lake Architecture

### Delta Lake Medallion Architecture

```text
Raw Data → Bronze Layer → Silver Layer → Gold Layer → Serving
   ↓           ↓            ↓            ↓         ↓
File/Stream  Raw Storage  Clean Data   Aggregated  Analytics
Sources     (Delta)      (Delta)      (Delta)     & ML
```

### Bronze Layer
- Raw data ingestion from all sources
- Schema enforcement and evolution
- Data quality checks and bad record handling
- Time travel and versioning capabilities

### Silver Layer  
- Data cleansing and standardization
- Deduplication and data quality improvements
- Schema validation and type enforcement
- Incremental processing with merge operations

### Gold Layer
- Business-ready aggregated data
- Feature engineering for ML
- Dimensional modeling
- Performance optimized for analytics

## Streaming Processing Examples

### Real-time Analytics Pipeline

```scala
// Kafka to Delta Lake streaming
val kafkaStream = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "ecommerce-events")
  .load()

val processedStream = kafkaStream
  .select(from_json(col("value").cast("string"), schema).alias("data"))
  .select("data.*")
  .withColumn("processing_time", current_timestamp())
  .withWatermark("event_time", "5 minutes")
  .groupBy(
    window(col("event_time"), "1 minute"),
    col("product_category")
  )
  .agg(
    sum("amount").alias("total_revenue"),
    count("*").alias("order_count"),
    avg("amount").alias("avg_order_value")
  )

processedStream
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/delta/checkpoints/realtime-analytics")
  .option("path", "/delta/gold/realtime-metrics")
  .trigger(Trigger.ProcessingTime("1 minute"))
  .start()
```

### Complex Event Processing

```scala
// Multi-stream joins and complex aggregations
val ordersStream = spark.readStream.format("delta").load("/delta/bronze/orders")
val paymentsStream = spark.readStream.format("delta").load("/delta/bronze/payments")
val shipmentsStream = spark.readStream.format("delta").load("/delta/bronze/shipments")

val enrichedOrders = ordersStream
  .join(paymentsStream, "order_id")
  .join(shipmentsStream, "order_id")
  .select(
    col("order_id"),
    col("customer_id"),
    col("total_amount"),
    col("payment_status"),
    col("shipping_status"),
    when(col("payment_status") === "completed" && 
         col("shipping_status") === "delivered", "fulfilled")
    .otherwise("pending").alias("fulfillment_status")
  )
```

## Machine Learning Pipelines

### Real-time Feature Engineering

```scala
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Pipeline

val featureEngineering = new Pipeline().setStages(Array(
  new StringIndexer().setInputCol("category").setOutputCol("category_idx"),
  new VectorAssembler()
    .setInputCols(Array("price", "quantity", "category_idx"))
    .setOutputCol("features"),
  new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaled_features")
))

val streamingML = ordersStream
  .transform(featureEngineering.fit(historicalData).transform(_))
  .select("order_id", "scaled_features", "customer_segment")
```

### Online Model Serving

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Load streaming data for real-time inference
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ml-inference-requests") \
    .load()

# Load pre-trained model
model = Pipeline.load("/models/customer-churn-model")

# Apply model for real-time predictions
predictions = model.transform(streaming_df)

predictions.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "ml-predictions") \
    .option("checkpointLocation", "/checkpoints/ml-inference") \
    .start()
```

## Batch Processing Jobs

### Data Quality Framework

```scala
import io.delta.tables.DeltaTable

object DataQualityJob extends SparkJob {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    
    val silverTable = DeltaTable.forPath(spark, "/delta/silver/customers")
    
    // Data quality checks
    val qualityChecks = Seq(
      ("email_format", col("email").rlike("^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$")),
      ("age_range", col("age").between(0, 120)),
      ("required_fields", col("customer_id").isNotNull && col("name").isNotNull)
    )
    
    val qualityResults = qualityChecks.map { case (checkName, condition) =>
      val passCount = silverTable.toDF.filter(condition).count()
      val totalCount = silverTable.toDF.count()
      val passRate = passCount.toDouble / totalCount
      
      QualityMetric(checkName, passRate, passCount, totalCount)
    }
    
    // Store quality metrics
    qualityResults.toDF.write
      .format("delta")
      .mode("append")
      .save("/delta/gold/data-quality-metrics")
  }
}
```

### Incremental ETL with Delta Lake

```scala
import io.delta.tables.DeltaTable

object IncrementalETL extends SparkJob {
  def run(spark: SparkSession): Unit = {
    val bronzeTable = DeltaTable.forPath(spark, "/delta/bronze/raw-events")
    val silverTable = DeltaTable.forPath(spark, "/delta/silver/processed-events")
    
    // Get latest processed timestamp
    val lastProcessed = silverTable.toDF
      .agg(max("processing_timestamp"))
      .collect()(0).getTimestamp(0)
    
    // Process only new records
    val newRecords = bronzeTable.toDF
      .filter(col("ingestion_time") > lit(lastProcessed))
      .transform(cleanAndValidate)
      .transform(enrichWithDimensions)
      .withColumn("processing_timestamp", current_timestamp())
    
    // Upsert into silver table
    silverTable.alias("target")
      .merge(newRecords.alias("source"), "target.event_id = source.event_id")
      .whenMatched.updateAll()
      .whenNotMatched.insertAll()
      .execute()
  }
}
```

## Graph Analytics

### Network Analysis

```scala
import org.apache.spark.graphx._

object NetworkAnalysis extends SparkJob {
  def run(spark: SparkSession): Unit = {
    // Load customer transaction network
    val vertices = spark.read.format("delta")
      .load("/delta/silver/customers")
      .select("customer_id", "customer_segment")
      .rdd.map(row => (row.getLong(0), row.getString(1)))
    
    val edges = spark.read.format("delta")
      .load("/delta/silver/transactions")
      .select("from_customer", "to_customer", "amount")
      .rdd.map(row => Edge(row.getLong(0), row.getLong(1), row.getDouble(2)))
    
    val graph = Graph(vertices, edges)
    
    // Community detection
    val communities = graph.connectedComponents()
    
    // PageRank for customer influence
    val pageRank = graph.pageRank(0.001)
    
    // Save results to Delta Lake
    communities.vertices.toDF("customer_id", "community")
      .write.format("delta").mode("overwrite")
      .save("/delta/gold/customer-communities")
      
    pageRank.vertices.toDF("customer_id", "influence_score")
      .write.format("delta").mode("overwrite")
      .save("/delta/gold/customer-influence")
  }
}
```

## Advanced Analytics

### Time Series Forecasting

```python
from pyspark.sql.functions import *
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

def time_series_forecasting(spark):
    # Load historical sales data
    sales_df = spark.read.format("delta").load("/delta/gold/daily-sales")
    
    # Feature engineering for time series
    features_df = sales_df \
        .withColumn("day_of_week", dayofweek("date")) \
        .withColumn("month", month("date")) \
        .withColumn("lag_1", lag("sales", 1).over(Window.orderBy("date"))) \
        .withColumn("lag_7", lag("sales", 7).over(Window.orderBy("date"))) \
        .withColumn("rolling_avg_7", 
                   avg("sales").over(Window.orderBy("date").rowsBetween(-6, 0)))
    
    # Vector assembly
    assembler = VectorAssembler(
        inputCols=["day_of_week", "month", "lag_1", "lag_7", "rolling_avg_7"],
        outputCol="features"
    )
    
    model_df = assembler.transform(features_df).na.drop()
    
    # Train forecasting model
    lr = LinearRegression(featuresCol="features", labelCol="sales")
    model = lr.fit(model_df)
    
    # Generate predictions
    predictions = model.transform(model_df)
    
    # Save model and predictions
    model.write().overwrite().save("/models/sales-forecasting")
    predictions.write.format("delta").mode("overwrite") \
        .save("/delta/gold/sales-forecasts")
```

### Anomaly Detection

```scala
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.StandardScaler

object AnomalyDetection extends SparkJob {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    
    // Load transaction data
    val transactions = spark.read.format("delta")
      .load("/delta/silver/transactions")
      .select("transaction_id", "amount", "merchant_category", "time_of_day")
    
    // Feature engineering
    val features = new VectorAssembler()
      .setInputCols(Array("amount", "merchant_category_encoded", "time_of_day"))
      .setOutputCol("raw_features")
      .transform(transactions)
    
    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .fit(features)
    
    val scaledData = scaler.transform(features)
    
    // Anomaly detection using K-means clustering
    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("cluster")
      .setK(10)
      .setSeed(42)
    
    val model = kmeans.fit(scaledData)
    val predictions = model.transform(scaledData)
    
    // Calculate distance from cluster centers to identify outliers
    val anomalies = predictions
      .withColumn("distance_to_center", 
        calculateDistance(col("features"), col("cluster")))
      .withColumn("is_anomaly", 
        col("distance_to_center") > percentile_approx(col("distance_to_center"), 0.95))
    
    // Save anomaly results
    anomalies.filter(col("is_anomaly"))
      .write.format("delta").mode("overwrite")
      .save("/delta/gold/transaction-anomalies")
  }
}
```

## Performance Optimization

### Adaptive Query Execution (AQE)

```scala
// Enable AQE for automatic optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

### Delta Lake Optimization

```scala
import io.delta.tables.DeltaTable

// Z-order optimization for better query performance
DeltaTable.forPath(spark, "/delta/silver/transactions")
  .optimize()
  .executeZOrderBy("customer_id", "transaction_date")

// Vacuum old files to save storage
DeltaTable.forPath(spark, "/delta/silver/transactions")
  .vacuum(7) // Keep 7 days of history
```

### Dynamic Partition Pruning

```scala
// Configure dynamic partition pruning
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")

// Partitioned table query
val dailySales = spark.read.format("delta")
  .load("/delta/gold/sales-by-day")
  .filter(col("date") >= "2024-01-01")
  .groupBy("product_category")
  .sum("sales_amount")
```

## Monitoring & Observability

### Custom Metrics Collection

```scala
import org.apache.spark.util.AccumulatorV2

class DataQualityAccumulator extends AccumulatorV2[String, Map[String, Long]] {
  private var _metrics = Map.empty[String, Long]
  
  def isZero: Boolean = _metrics.isEmpty
  def copy(): DataQualityAccumulator = {
    val acc = new DataQualityAccumulator
    acc._metrics = _metrics
    acc
  }
  def reset(): Unit = _metrics = Map.empty
  def add(metric: String): Unit = {
    _metrics = _metrics + (metric -> (_metrics.getOrElse(metric, 0L) + 1))
  }
  def merge(other: AccumulatorV2[String, Map[String, Long]]): Unit = {
    other match {
      case acc: DataQualityAccumulator =>
        acc._metrics.foreach { case (k, v) =>
          _metrics = _metrics + (k -> (_metrics.getOrElse(k, 0L) + v))
        }
    }
  }
  def value: Map[String, Long] = _metrics
}
```

### Structured Streaming Metrics

```scala
// Monitor streaming query progress
val query = processedStream.writeStream
  .format("delta")
  .option("checkpointLocation", "/checkpoints/streaming-metrics")
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    val recordCount = batchDF.count()
    val processingTime = System.currentTimeMillis()
    
    // Log metrics
    logger.info(s"Batch $batchId processed $recordCount records at $processingTime")
    
    // Store metrics in Delta table
    Seq((batchId, recordCount, processingTime))
      .toDF("batch_id", "record_count", "processing_time")
      .write.format("delta").mode("append")
      .save("/delta/monitoring/streaming-metrics")
  }
  .start()

// Query progress monitoring
val progress = query.recentProgress
progress.foreach(p => println(s"Input rate: ${p.inputRowsPerSecond}"))
```

## Testing Framework

### Unit Testing with ScalaTest

```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.test.SharedSparkSession

class DataTransformationsTest extends AnyFlatSpec with SharedSparkSession {
  
  "Data transformation" should "clean invalid records correctly" in {
    import spark.implicits._
    
    val input = Seq(
      ("valid@email.com", 25, "John"),
      ("invalid-email", -5, null),
      ("another@valid.com", 30, "Jane")
    ).toDF("email", "age", "name")
    
    val result = DataTransformations.cleanCustomerData(input)
    val validRecords = result.count()
    
    assert(validRecords == 2)
    assert(result.filter(col("age") < 0).count() == 0)
    assert(result.filter(col("name").isNull).count() == 0)
  }
  
  "Aggregation function" should "calculate metrics correctly" in {
    val input = Seq(
      ("2024-01-01", "A", 100.0),
      ("2024-01-01", "A", 200.0),
      ("2024-01-01", "B", 150.0)
    ).toDF("date", "category", "amount")
    
    val result = DataTransformations.calculateDailyMetrics(input)
    val categoryA = result.filter(col("category") === "A").collect()(0)
    
    assert(categoryA.getDouble(categoryA.fieldIndex("total_amount")) == 300.0)
    assert(categoryA.getLong(categoryA.fieldIndex("count")) == 2)
  }
}
```

## Deployment & Orchestration

### Apache Airflow DAGs

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'retries': 2
}

dag = DAG(
    'spark_analytics_pipeline',
    default_args=default_args,
    description='Daily Spark analytics pipeline',
    schedule_interval='@daily',
    catchup=False
)

# Data ingestion job
ingest_data = SparkSubmitOperator(
    task_id='ingest_raw_data',
    application='/spark-jobs/ingestion/DataIngestionJob.py',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    },
    dag=dag
)

# Data quality job
quality_check = SparkSubmitOperator(
    task_id='data_quality_check',
    application='/spark-jobs/quality/DataQualityJob.scala',
    dag=dag
)

# Feature engineering
feature_engineering = SparkSubmitOperator(
    task_id='feature_engineering',
    application='/spark-jobs/ml/FeatureEngineeringJob.py',
    dag=dag
)

# Model training
model_training = SparkSubmitOperator(
    task_id='model_training',
    application='/spark-jobs/ml/ModelTrainingJob.py',
    dag=dag
)

# Set dependencies
ingest_data >> quality_check >> feature_engineering >> model_training
```

### Kubernetes Deployment

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: realtime-analytics
  namespace: spark-jobs
spec:
  type: Scala
  mode: cluster
  image: "spark-analytics:3.5.0"
  imagePullPolicy: Always
  mainClass: com.analytics.streaming.RealtimeAnalyticsJob
  mainApplicationFile: "s3a://spark-apps/realtime-analytics.jar"
  sparkVersion: "3.5.0"
  driver:
    cores: 2
    memory: "2g"
    serviceAccount: spark-driver
  executor:
    cores: 2
    instances: 4
    memory: "4g"
  dynamicAllocation:
    enabled: true
    initialExecutors: 2
    minExecutors: 2
    maxExecutors: 10
```

## Future Enhancements

### 1. Advanced ML Operations
- **AutoML Integration**: Automated model selection and hyperparameter tuning
- **Model Versioning**: MLflow integration for model lifecycle management  
- **A/B Testing**: Online experimentation framework
- **Real-time Inference**: Low-latency model serving

### 2. Advanced Analytics
- **Deep Learning**: Integration with TensorFlow and PyTorch
- **Graph Neural Networks**: Advanced graph analytics capabilities
- **Geospatial Analytics**: Location-based analytics and processing
- **Computer Vision**: Image and video processing pipelines

### 3. Performance & Scalability  
- **GPU Acceleration**: RAPIDS integration for GPU-accelerated processing
- **Quantum Computing**: Qiskit integration for quantum algorithms
- **Edge Computing**: Spark on edge devices and fog computing
- **Multi-Cloud**: Cross-cloud data processing and analytics

---

**Master big data analytics with enterprise-grade Spark pipelines! ⚡**
