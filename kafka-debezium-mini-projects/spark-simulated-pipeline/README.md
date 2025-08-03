# Spark E-commerce Analytics Pipeline

## Project Description

This project demonstrates a comprehensive e-commerce analytics platform using Apache Spark for real-time and batch processing. It simulates a complete e-commerce data pipeline with order processing, customer analytics, inventory management, and machine learning-driven recommendations using Spark Structured Streaming, MLlib, and Delta Lake.

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                Spark E-commerce Analytics Platform                      │
├─────────────────────────────────────────────────────────────────────────┤
│  Data Generation     │  Spark Processing      │  Analytics & ML         │
│  ├─ Order Events     │  ├─ Streaming ETL      │  ├─ Customer Segments   │
│  ├─ User Behavior    │  ├─ Batch Analytics    │  ├─ Product Recommendations │
│  ├─ Inventory Data   │  ├─ Feature Engineering│  ├─ Demand Forecasting  │
│  ├─ Product Catalog  │  ├─ Data Quality       │  ├─ Fraud Detection     │
│  └─ Payment Events   │  └─ Real-time Metrics  │  └─ Price Optimization  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Business Use Cases

- **Real-time Order Processing**: Stream processing of orders with inventory validation
- **Customer Analytics**: Segmentation, lifetime value, churn prediction
- **Product Recommendations**: Collaborative filtering and content-based recommendations
- **Inventory Management**: Demand forecasting and stock optimization
- **Fraud Detection**: Real-time transaction monitoring and anomaly detection
- **Price Optimization**: Dynamic pricing based on demand and competition

## Tech Stack

- **Processing Engine**: Apache Spark 3.5 with Scala/PySpark
- **Storage**: Delta Lake with ACID transactions
- **Streaming**: Structured Streaming with Kafka
- **ML Framework**: Spark MLlib and ML Pipelines
- **Orchestration**: Apache Airflow
- **Monitoring**: Spark UI, Prometheus, Grafana
- **Development**: Jupyter Notebooks, Spark Shell

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Apache Spark 3.5+
- Scala 2.12+ and Python 3.8+
- At least 12GB RAM available

### Quick Start

```bash
# Navigate to project
cd spark-simulated-pipeline

# Start infrastructure
docker-compose up -d

# Initialize data generation
./scripts/setup-data-generation.sh

# Start streaming jobs
./scripts/start-streaming-jobs.sh

# Run batch analytics
./scripts/run-batch-analytics.sh

# Access UIs
open http://localhost:4040    # Spark UI
open http://localhost:8080    # Spark Master
open http://localhost:8888    # Jupyter Notebook
open http://localhost:3000    # Grafana Dashboard
```

## Project Structure

```
spark-simulated-pipeline/
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── ecommerce/
│   │   │   │   ├── streaming/          # Real-time processing
│   │   │   │   ├── batch/              # Batch jobs
│   │   │   │   ├── ml/                 # Machine learning
│   │   │   │   └── utils/              # Common utilities
│   │   │   └── generators/             # Data generators
│   │   └── python/
│   │       ├── streaming/              # PySpark streaming
│   │       ├── ml/                     # Python ML jobs
│   │       └── notebooks/              # Analysis notebooks
├── data/
│   ├── bronze/                         # Raw streaming data
│   ├── silver/                         # Cleaned data
│   ├── gold/                          # Business aggregates
│   └── models/                        # ML models
├── config/
│   ├── spark-defaults.conf
│   └── streaming-config.conf
├── scripts/
│   ├── data-generation/
│   ├── job-submission/
│   └── monitoring/
└── docker-compose.yml
```

## Data Model

### Core Entities

```scala
case class Order(
  orderId: String,
  customerId: String,
  orderDate: Timestamp,
  totalAmount: Double,
  status: String,
  items: Array[OrderItem]
)

case class OrderItem(
  productId: String,
  quantity: Int,
  price: Double,
  category: String
)

case class Customer(
  customerId: String,
  email: String,
  registrationDate: Timestamp,
  segment: String,
  lifetimeValue: Double
)

case class Product(
  productId: String,
  name: String,
  category: String,
  price: Double,
  inventory: Int,
  rating: Double
)
```

## Real-time Streaming Jobs

### Order Processing Stream

```scala
package ecommerce.streaming

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

object OrderProcessingJob extends SparkJob {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    
    // Read from Kafka orders topic
    val ordersStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ecommerce-orders")
      .load()
      .select(from_json(col("value").cast("string"), orderSchema).as("order"))
      .select("order.*")
      .withWatermark("orderDate", "5 minutes")
    
    // Real-time order validation and enrichment
    val enrichedOrders = ordersStream
      .join(broadcast(productCatalog), "productId")
      .join(broadcast(customerData), "customerId")
      .withColumn("orderValue", col("quantity") * col("price"))
      .withColumn("isHighValue", col("orderValue") > 1000)
      .withColumn("processingTime", current_timestamp())
    
    // Write to Delta Lake bronze layer
    enrichedOrders
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/checkpoints/orders-bronze")
      .option("path", "/data/bronze/orders")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
    
    // Real-time metrics aggregation
    val orderMetrics = enrichedOrders
      .groupBy(
        window(col("orderDate"), "1 minute"),
        col("category")
      )
      .agg(
        sum("orderValue").alias("totalRevenue"),
        count("*").alias("orderCount"),
        avg("orderValue").alias("avgOrderValue"),
        countDistinct("customerId").alias("uniqueCustomers")
      )
    
    // Write metrics to gold layer
    orderMetrics
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "/checkpoints/order-metrics")
      .option("path", "/data/gold/order-metrics")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
  }
}
```

### Fraud Detection Stream

```scala
package ecommerce.streaming

object FraudDetectionJob extends SparkJob {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    
    val paymentsStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "payment-events")
      .load()
      .select(from_json(col("value").cast("string"), paymentSchema).as("payment"))
      .select("payment.*")
    
    // Real-time fraud scoring
    val fraudScores = paymentsStream
      .withColumn("hourOfDay", hour(col("paymentTime")))
      .withColumn("dayOfWeek", dayofweek(col("paymentTime")))
      .withColumn("amountZScore", 
        (col("amount") - col("customerAvgAmount")) / col("customerStdAmount"))
      .withColumn("velocityScore", 
        when(col("paymentsLast1Hour") > 5, 1.0).otherwise(0.0))
      .withColumn("fraudScore", 
        col("amountZScore") * 0.4 + 
        col("velocityScore") * 0.3 + 
        col("locationRiskScore") * 0.3)
      .withColumn("isFraudulent", col("fraudScore") > 0.7)
    
    // Alert on high-risk transactions
    val fraudAlerts = fraudScores
      .filter(col("isFraudulent"))
      .select("paymentId", "customerId", "amount", "fraudScore", "paymentTime")
    
    fraudAlerts
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "fraud-alerts")
      .option("checkpointLocation", "/checkpoints/fraud-alerts")
      .start()
      
    // Store fraud scores for model retraining
    fraudScores
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/checkpoints/fraud-scores")
      .option("path", "/data/silver/fraud-scores")
      .start()
  }
}
```

## Batch Analytics Jobs

### Customer Segmentation

```scala
package ecommerce.batch

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}

object CustomerSegmentationJob extends SparkJob {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    
    // Load customer transaction history
    val customerMetrics = spark.read
      .format("delta")
      .load("/data/silver/customer-transactions")
      .groupBy("customerId")
      .agg(
        sum("amount").alias("totalSpent"),
        count("*").alias("transactionCount"),
        avg("amount").alias("avgTransactionValue"),
        max("transactionDate").alias("lastTransactionDate"),
        min("transactionDate").alias("firstTransactionDate")
      )
      .withColumn("daysSinceLastTransaction", 
        datediff(current_date(), col("lastTransactionDate")))
      .withColumn("customerLifetime", 
        datediff(col("lastTransactionDate"), col("firstTransactionDate")))
      .withColumn("frequency", 
        col("transactionCount") / (col("customerLifetime") + 1))
    
    // Feature engineering for RFM analysis
    val features = Array("totalSpent", "frequency", "daysSinceLastTransaction")
    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("rawFeatures")
    
    val scaler = new StandardScaler()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)
    
    val featureData = scaler.fit(assembler.transform(customerMetrics))
      .transform(assembler.transform(customerMetrics))
    
    // K-means clustering for segmentation
    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("segment")
      .setK(5)
      .setSeed(42)
    
    val model = kmeans.fit(featureData)
    val segmentedCustomers = model.transform(featureData)
    
    // Segment interpretation
    val segmentSummary = segmentedCustomers
      .groupBy("segment")
      .agg(
        count("*").alias("customerCount"),
        avg("totalSpent").alias("avgTotalSpent"),
        avg("frequency").alias("avgFrequency"),
        avg("daysSinceLastTransaction").alias("avgRecency")
      )
      .withColumn("segmentName", 
        when(col("avgTotalSpent") > 1000 && col("avgRecency") < 30, "Champions")
        .when(col("avgTotalSpent") > 500 && col("avgRecency") < 60, "Loyal Customers")
        .when(col("avgRecency") > 180, "At Risk")
        .when(col("avgRecency") > 365, "Lost Customers")
        .otherwise("Potential Loyalists"))
    
    // Save results
    segmentedCustomers.write
      .format("delta")
      .mode("overwrite")
      .save("/data/gold/customer-segments")
      
    segmentSummary.write
      .format("delta")
      .mode("overwrite")
      .save("/data/gold/segment-summary")
  }
}
```

### Product Recommendation Engine

```scala
package ecommerce.ml

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator

object RecommendationEngineJob extends SparkJob {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    
    // Load user-item interaction data
    val ratings = spark.read
      .format("delta")
      .load("/data/silver/product-ratings")
      .select("customerId", "productId", "rating", "timestamp")
    
    // Convert string IDs to numeric for ALS
    val customerIndexer = new StringIndexer()
      .setInputCol("customerId")
      .setOutputCol("customerIndex")
    
    val productIndexer = new StringIndexer()
      .setInputCol("productId")
      .setOutputCol("productIndex")
    
    val indexedRatings = productIndexer.fit(customerIndexer.fit(ratings)
      .transform(ratings)).transform(customerIndexer.fit(ratings).transform(ratings))
      .select(col("customerIndex").cast("int").alias("customer"),
              col("productIndex").cast("int").alias("product"),
              col("rating").cast("float"))
    
    // Split data for training and testing
    val Array(training, test) = indexedRatings.randomSplit(Array(0.8, 0.2), seed = 42)
    
    // Build ALS recommendation model
    val als = new ALS()
      .setMaxIter(10)
      .setRegParam(0.1)
      .setUserCol("customer")
      .setItemCol("product")
      .setRatingCol("rating")
      .setColdStartStrategy("drop")
      .setImplicitPrefs(false)
    
    val model = als.fit(training)
    
    // Generate predictions and evaluate
    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    
    // Generate top 10 recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    val productRecs = model.recommendForAllItems(10)
    
    // Save model and recommendations
    model.write.overwrite().save("/data/models/als-recommendation-model")
    
    userRecs.write
      .format("delta")
      .mode("overwrite")
      .save("/data/gold/user-recommendations")
      
    productRecs.write
      .format("delta")
      .mode("overwrite")
      .save("/data/gold/product-recommendations")
  }
}
```

### Demand Forecasting

```python
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def demand_forecasting_job(spark):
    # Load historical sales data
    sales_df = spark.read.format("delta").load("/data/silver/daily-sales")
    
    # Feature engineering for time series
    window_spec = Window.partitionBy("productId").orderBy("date")
    
    features_df = sales_df \
        .withColumn("day_of_week", dayofweek("date")) \
        .withColumn("month", month("date")) \
        .withColumn("quarter", quarter("date")) \
        .withColumn("lag_1", lag("quantity", 1).over(window_spec)) \
        .withColumn("lag_7", lag("quantity", 7).over(window_spec)) \
        .withColumn("lag_30", lag("quantity", 30).over(window_spec)) \
        .withColumn("rolling_avg_7", 
                   avg("quantity").over(window_spec.rowsBetween(-6, 0))) \
        .withColumn("rolling_avg_30", 
                   avg("quantity").over(window_spec.rowsBetween(-29, 0))) \
        .withColumn("is_weekend", 
                   when(col("day_of_week").isin([1, 7]), 1).otherwise(0))
    
    # Add external factors (holidays, promotions, etc.)
    features_df = features_df \
        .withColumn("is_holiday", 
                   when(col("date").isin(holiday_dates), 1).otherwise(0)) \
        .withColumn("promotion_intensity", 
                   coalesce(col("promotion_discount"), lit(0.0)))
    
    # Category encoding
    category_indexer = StringIndexer(
        inputCol="category", 
        outputCol="category_idx"
    )
    
    # Vector assembly
    feature_cols = [
        "day_of_week", "month", "quarter", "category_idx",
        "lag_1", "lag_7", "lag_30", "rolling_avg_7", "rolling_avg_30",
        "is_weekend", "is_holiday", "promotion_intensity"
    ]
    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    
    # Prepare training data
    model_df = assembler.transform(
        category_indexer.fit(features_df).transform(features_df)
    ).na.drop()
    
    # Split data
    train_df, test_df = model_df.randomSplit([0.8, 0.2], seed=42)
    
    # Train Random Forest model
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="quantity",
        numTrees=100,
        maxDepth=10
    )
    
    model = rf.fit(train_df)
    predictions = model.transform(test_df)
    
    # Evaluate model
    evaluator = RegressionEvaluator(
        labelCol="quantity",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"RMSE for demand forecasting: {rmse}")
    
    # Generate future predictions
    future_dates = spark.range(1, 31) \
        .withColumn("future_date", 
                   date_add(current_date(), col("id").cast("int"))) \
        .crossJoin(sales_df.select("productId", "category").distinct())
    
    # Feature engineering for future dates
    future_features = future_dates \
        .withColumn("day_of_week", dayofweek("future_date")) \
        .withColumn("month", month("future_date")) \
        .withColumn("quarter", quarter("future_date"))
    
    # Apply model for forecasting
    future_predictions = model.transform(
        assembler.transform(
            category_indexer.fit(features_df).transform(future_features)
        )
    )
    
    # Save model and predictions
    model.write().overwrite().save("/data/models/demand-forecasting")
    
    future_predictions.select(
        "productId", "future_date", "prediction"
    ).write.format("delta").mode("overwrite") \
        .save("/data/gold/demand-forecasts")
```

## Data Generation

### E-commerce Data Simulator

```scala
package generators

import org.apache.spark.sql.streaming.DataStreamWriter
import scala.util.Random

object EcommerceDataGenerator {
  
  case class SimulatedOrder(
    orderId: String,
    customerId: String,
    productId: String,
    quantity: Int,
    price: Double,
    timestamp: Timestamp,
    category: String,
    customerSegment: String
  )
  
  def generateOrderStream(spark: SparkSession, ratePerSecond: Int = 10): Unit = {
    import spark.implicits._
    
    val categories = Array("Electronics", "Clothing", "Books", "Home", "Sports")
    val segments = Array("Premium", "Regular", "Budget")
    
    val orderStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", ratePerSecond)
      .load()
      .map { row =>
        val random = new Random()
        SimulatedOrder(
          orderId = s"ORDER-${row.getLong(0)}",
          customerId = s"CUST-${random.nextInt(10000)}",
          productId = s"PROD-${random.nextInt(1000)}",
          quantity = random.nextInt(5) + 1,
          price = 10.0 + random.nextDouble() * 990.0,
          timestamp = new Timestamp(row.getTimestamp(1).getTime),
          category = categories(random.nextInt(categories.length)),
          customerSegment = segments(random.nextInt(segments.length))
        )
      }
    
    // Write to Kafka
    orderStream
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "ecommerce-orders")
      .option("checkpointLocation", "/checkpoints/order-generation")
      .start()
    
    // Also write to Delta Lake
    orderStream
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/checkpoints/orders-raw")
      .option("path", "/data/bronze/orders-raw")
      .start()
  }
  
  def generateUserBehaviorStream(spark: SparkSession): Unit = {
    import spark.implicits._
    
    val actions = Array("view", "add_to_cart", "purchase", "review")
    
    val behaviorStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 50)
      .load()
      .map { row =>
        val random = new Random()
        UserBehavior(
          userId = s"USER-${random.nextInt(5000)}",
          productId = s"PROD-${random.nextInt(1000)}",
          action = actions(random.nextInt(actions.length)),
          timestamp = new Timestamp(row.getTimestamp(1).getTime),
          sessionId = s"SESSION-${random.nextInt(1000)}",
          duration = random.nextInt(300) + 10
        )
      }
    
    behaviorStream
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/checkpoints/user-behavior")
      .option("path", "/data/bronze/user-behavior")
      .start()
  }
}
```

## Performance Optimization

### Adaptive Query Execution

```scala
// Enable AQE optimizations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

// Dynamic partition pruning
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

// Broadcast join optimization
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "20MB")
```

### Delta Lake Optimization

```scala
import io.delta.tables.DeltaTable

// Optimize tables regularly
DeltaTable.forPath(spark, "/data/silver/orders")
  .optimize()
  .executeZOrderBy("customerId", "orderDate")

// Auto-compact small files
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.maxFileSize", "134217728") // 128MB
```

## Monitoring & Alerting

### Custom Metrics

```scala
import org.apache.spark.util.AccumulatorV2

class BusinessMetricsAccumulator extends AccumulatorV2[String, Map[String, Double]] {
  private var _metrics = Map.empty[String, Double]
  
  def isZero: Boolean = _metrics.isEmpty
  def copy(): BusinessMetricsAccumulator = {
    val acc = new BusinessMetricsAccumulator
    acc._metrics = _metrics
    acc
  }
  def reset(): Unit = _metrics = Map.empty
  def add(metric: String, value: Double): Unit = {
    _metrics = _metrics + (metric -> (_metrics.getOrElse(metric, 0.0) + value))
  }
  def merge(other: AccumulatorV2[String, Map[String, Double]]): Unit = {
    other match {
      case acc: BusinessMetricsAccumulator =>
        acc._metrics.foreach { case (k, v) =>
          _metrics = _metrics + (k -> (_metrics.getOrElse(k, 0.0) + v))
        }
    }
  }
  def value: Map[String, Double] = _metrics
}

// Usage in streaming job
val metricsAccumulator = spark.sparkContext.register(new BusinessMetricsAccumulator)

ordersStream.foreachBatch { (batchDF, batchId) =>
  val revenue = batchDF.agg(sum("orderValue")).collect()(0).getDouble(0)
  val orderCount = batchDF.count()
  
  metricsAccumulator.add("total_revenue", revenue)
  metricsAccumulator.add("order_count", orderCount.toDouble)
  
  // Store metrics in monitoring table
  Seq((batchId, revenue, orderCount, System.currentTimeMillis()))
    .toDF("batch_id", "revenue", "order_count", "timestamp")
    .write.format("delta").mode("append")
    .save("/data/monitoring/streaming-metrics")
}
```

## Testing Framework

### Spark Testing Utilities

```scala
import org.scalatest.flatspec.AnyFlatSpec
import com.holdenkarau.spark.testing.DataFrameSuiteBase

class EcommerceAnalyticsTest extends AnyFlatSpec with DataFrameSuiteBase {
  import spark.implicits._
  
  "Customer segmentation" should "correctly identify high-value customers" in {
    val testData = Seq(
      ("CUST1", 5000.0, 50, 10),
      ("CUST2", 100.0, 5, 100),
      ("CUST3", 2000.0, 20, 30)
    ).toDF("customerId", "totalSpent", "transactionCount", "daysSinceLastTransaction")
    
    val result = CustomerSegmentationJob.segmentCustomers(testData)
    val segments = result.select("customerId", "segmentName").collect()
    
    assert(segments.find(_.getString(0) == "CUST1").get.getString(1) == "Champions")
    assert(segments.find(_.getString(0) == "CUST2").get.getString(1) == "At Risk")
  }
  
  "Fraud detection" should "identify suspicious transactions" in {
    val testTransactions = Seq(
      ("TXN1", "CUST1", 50.0, 0.1),
      ("TXN2", "CUST1", 5000.0, 0.8),
      ("TXN3", "CUST2", 100.0, 0.3)
    ).toDF("transactionId", "customerId", "amount", "fraudScore")
    
    val fraudulent = FraudDetectionJob.identifyFraud(testTransactions, 0.7)
    assert(fraudulent.count() == 1)
    assert(fraudulent.collect()(0).getString(0) == "TXN2")
  }
}
```

## Deployment & Orchestration

### Airflow DAG for E-commerce Pipeline

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ecommerce-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'retries': 2
}

dag = DAG(
    'ecommerce_analytics_pipeline',
    default_args=default_args,
    description='E-commerce analytics pipeline',
    schedule_interval='@daily',
    catchup=False
)

# Customer segmentation job
customer_segmentation = SparkSubmitOperator(
    task_id='customer_segmentation',
    application='/spark-jobs/CustomerSegmentationJob.scala',
    name='customer-segmentation',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.dynamicAllocation.enabled': 'true',
        'spark.dynamicAllocation.minExecutors': '2',
        'spark.dynamicAllocation.maxExecutors': '10'
    },
    dag=dag
)

# Product recommendations
product_recommendations = SparkSubmitOperator(
    task_id='product_recommendations',
    application='/spark-jobs/RecommendationEngineJob.scala',
    name='product-recommendations',
    dag=dag
)

# Demand forecasting
demand_forecasting = SparkSubmitOperator(
    task_id='demand_forecasting',
    application='/spark-jobs/DemandForecastingJob.py',
    name='demand-forecasting',
    dag=dag
)

# Inventory optimization
inventory_optimization = SparkSubmitOperator(
    task_id='inventory_optimization',
    application='/spark-jobs/InventoryOptimizationJob.scala',
    name='inventory-optimization',
    dag=dag
)

# Set task dependencies
customer_segmentation >> product_recommendations
demand_forecasting >> inventory_optimization
product_recommendations >> inventory_optimization
```

## Business Intelligence Dashboards

### Real-time Metrics Dashboard

```python
# Grafana dashboard configuration for real-time metrics
import json

dashboard_config = {
    "dashboard": {
        "title": "E-commerce Real-time Analytics",
        "panels": [
            {
                "title": "Orders per Minute",
                "type": "graph",
                "targets": [
                    {
                        "expr": "rate(ecommerce_orders_total[1m])",
                        "legendFormat": "Orders/min"
                    }
                ]
            },
            {
                "title": "Revenue by Category",
                "type": "pie",
                "targets": [
                    {
                        "expr": "sum by (category) (ecommerce_revenue_total)",
                        "legendFormat": "{{category}}"
                    }
                ]
            },
            {
                "title": "Customer Segments Distribution",
                "type": "bar",
                "targets": [
                    {
                        "expr": "count by (segment) (ecommerce_customers_total)",
                        "legendFormat": "{{segment}}"
                    }
                ]
            }
        ]
    }
}
```

## Future Enhancements

### 1. Advanced Analytics
- **Deep Learning**: Implement neural networks for complex pattern recognition
- **Graph Analytics**: Social network analysis for viral marketing
- **Time Series**: Advanced forecasting with LSTM/GRU models
- **Computer Vision**: Image-based product recommendations

### 2. Real-time ML
- **Online Learning**: Continuously updating models with streaming data
- **Feature Stores**: Centralized feature management and serving
- **Model Monitoring**: Drift detection and automated retraining
- **A/B Testing**: Experimentation framework for model comparison

### 3. Advanced Optimization
- **Dynamic Pricing**: Real-time price optimization based on demand
- **Supply Chain**: End-to-end supply chain optimization
- **Marketing Attribution**: Multi-touch attribution modeling
- **Personalization**: Individual customer experience optimization

---

**Build enterprise-scale e-commerce analytics with Spark! 🛒⚡**
