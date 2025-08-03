#!/usr/bin/env python3
"""
IoT Machine Learning Pipeline
Implements predictive maintenance, anomaly detection, and clustering for IoT devices
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans, GaussianMixture
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, ClusteringEvaluator
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import os
import logging
from datetime import datetime, timedelta
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IoTMLPipeline:
    def __init__(self):
        self.spark = None
        self.postgres_url = None
        self.postgres_properties = None
        self.models = {}
        
    def initialize_spark(self):
        """Initialize Spark session with ML libraries"""
        try:
            self.spark = SparkSession.builder \
                .appName("IoTMLPipeline") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
                .getOrCreate()
                
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark ML session initialized")
            
            # Setup PostgreSQL connection
            self.postgres_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:" \
                              f"{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'iot_analytics')}"
            
            self.postgres_properties = {
                "user": os.getenv('POSTGRES_USER', 'postgres'),
                "password": os.getenv('POSTGRES_PASSWORD', 'postgres'),
                "driver": "org.postgresql.Driver"
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark ML: {e}")
            raise
            
    def load_training_data(self, days_back: int = 30):
        """Load and prepare training data from PostgreSQL"""
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            query = f"""(
                SELECT 
                    sr.*,
                    dm.device_type,
                    dm.location,
                    EXTRACT(HOUR FROM sr.timestamp) as hour_of_day,
                    EXTRACT(DOW FROM sr.timestamp) as day_of_week,
                    CASE 
                        WHEN sr.battery_level < 20 THEN 1 
                        ELSE 0 
                    END as low_battery_flag,
                    CASE 
                        WHEN sr.signal_strength < 30 THEN 1 
                        ELSE 0 
                    END as poor_signal_flag
                FROM sensor_readings sr
                JOIN device_metadata dm ON sr.device_id = dm.device_id
                WHERE sr.timestamp >= '{start_date.strftime('%Y-%m-%d')}'
                AND sr.timestamp < '{end_date.strftime('%Y-%m-%d')}'
                ORDER BY sr.timestamp
            ) AS training_data"""
            
            df = self.spark.read.jdbc(
                url=self.postgres_url,
                table=query,
                properties=self.postgres_properties
            )
            
            logger.info(f"📊 Loaded {df.count()} training records")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to load training data: {e}")
            raise
            
    def feature_engineering(self, df):
        """Create features for ML models"""
        try:
            # Add time-based features
            df_features = df \
                .withColumn("temp_humidity_ratio", col("temperature") / col("humidity")) \
                .withColumn("temp_pressure_interaction", col("temperature") * col("pressure") / 1000) \
                .withColumn("battery_signal_score", col("battery_level") * col("signal_strength") / 100) \
                .withColumn("is_weekend", when(col("day_of_week").isin([0, 6]), 1).otherwise(0)) \
                .withColumn("is_night", when((col("hour_of_day") < 6) | (col("hour_of_day") > 22), 1).otherwise(0))
            
            # Add rolling window features (simplified for batch processing)
            window_spec = Window.partitionBy("device_id").orderBy("timestamp").rowsBetween(-5, 0)
            
            df_features = df_features \
                .withColumn("temp_avg_6", avg("temperature").over(window_spec)) \
                .withColumn("temp_std_6", stddev("temperature").over(window_spec)) \
                .withColumn("humidity_trend", col("humidity") - lag("humidity", 5).over(window_spec))
                
            logger.info("✅ Feature engineering completed")
            return df_features
            
        except Exception as e:
            logger.error(f"❌ Feature engineering failed: {e}")
            raise
            
    def train_anomaly_detection_model(self, df):
        """Train unsupervised anomaly detection using clustering"""
        try:
            logger.info("🤖 Training anomaly detection model...")
            
            # Select features for anomaly detection
            feature_cols = ["temperature", "humidity", "pressure", "battery_level", 
                           "signal_strength", "temp_humidity_ratio", "temp_pressure_interaction"]
            
            # Assemble features
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            
            # Use Gaussian Mixture Model for anomaly detection
            gmm = GaussianMixture(featuresCol="scaled_features", k=3, seed=42)
            
            # Create pipeline
            pipeline = Pipeline(stages=[assembler, scaler, gmm])
            
            # Train model
            model = pipeline.fit(df)
            self.models['anomaly_detection'] = model
            
            # Calculate anomaly scores
            predictions = model.transform(df)
            
            # Extract anomaly scores (negative log-likelihood)
            anomaly_df = predictions.select(
                "device_id", "timestamp", "temperature", "humidity", "pressure",
                "prediction", "probability"
            ).withColumn("anomaly_score", 
                        -log(expr("probability[prediction]"))) \
             .filter(col("anomaly_score") > 5.0)  # Threshold for anomalies
            
            logger.info(f"🚨 Detected {anomaly_df.count()} potential anomalies")
            
            # Save anomaly detection results
            self.save_predictions(anomaly_df, "anomaly_detections", "anomaly_detection")
            
            return model
            
        except Exception as e:
            logger.error(f"❌ Anomaly detection training failed: {e}")
            raise
            
    def train_predictive_maintenance_model(self, df):
        """Train predictive maintenance model for battery life prediction"""
        try:
            logger.info("🔋 Training predictive maintenance model...")
            
            # Create target variable (time until battery replacement)
            df_battery = df.filter(col("battery_level").isNotNull()) \
                          .withColumn("days_to_low_battery", 
                                    when(col("battery_level") > 20, 
                                         (col("battery_level") - 20) / 2.0)  # Simplified model
                                    .otherwise(0))
            
            # Select features
            feature_cols = ["temperature", "humidity", "pressure", "signal_strength",
                           "hour_of_day", "day_of_week", "temp_avg_6", "battery_signal_score"]
            
            # Prepare ML pipeline
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="days_to_low_battery", 
                                     numTrees=50, seed=42)
            
            pipeline = Pipeline(stages=[assembler, scaler, rf])
            
            # Split data
            train_df, test_df = df_battery.randomSplit([0.8, 0.2], seed=42)
            
            # Train model
            model = pipeline.fit(train_df)
            self.models['predictive_maintenance'] = model
            
            # Evaluate model
            predictions = model.transform(test_df)
            evaluator = RegressionEvaluator(labelCol="days_to_low_battery", 
                                          predictionCol="prediction", metricName="rmse")
            rmse = evaluator.evaluate(predictions)
            
            logger.info(f"📊 Predictive maintenance model RMSE: {rmse:.2f}")
            
            # Generate predictions for all devices
            all_predictions = model.transform(df_battery)
                
            # Save predictions
            maintenance_predictions = all_predictions.select(
                "device_id", "timestamp", "battery_level", "prediction",
                current_timestamp().alias("created_at")
            ).withColumn("prediction_type", lit("battery_life")) \
             .withColumn("model_version", lit("v1.0")) \
             .withColumnRenamed("prediction", "predicted_value") \
             .withColumn("confidence", lit(0.85))  # Simplified confidence
            
            self.save_predictions(maintenance_predictions, "ml_predictions", "predictive_maintenance")
            
            return model
            
        except Exception as e:
            logger.error(f"❌ Predictive maintenance training failed: {e}")
            raise
            
    def train_device_clustering_model(self, df):
        """Train device clustering model to group similar devices"""
        try:
            logger.info("🎯 Training device clustering model...")
            
            # Aggregate device characteristics
            device_profiles = df.groupBy("device_id", "device_type", "location") \
                .agg(
                    avg("temperature").alias("avg_temperature"),
                    stddev("temperature").alias("std_temperature"),
                    avg("humidity").alias("avg_humidity"),
                    stddev("humidity").alias("std_humidity"),
                    avg("pressure").alias("avg_pressure"),
                    avg("battery_level").alias("avg_battery"),
                    avg("signal_strength").alias("avg_signal"),
                    count("*").alias("reading_count")
                )
            
            # Select features for clustering
            feature_cols = ["avg_temperature", "std_temperature", "avg_humidity", 
                           "std_humidity", "avg_pressure", "avg_battery", "avg_signal"]
            
            # Prepare clustering pipeline
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            kmeans = KMeans(featuresCol="scaled_features", k=5, seed=42)
            
            pipeline = Pipeline(stages=[assembler, scaler, kmeans])
            
            # Train model
            model = pipeline.fit(device_profiles)
            self.models['device_clustering'] = model
            
            # Generate cluster assignments
            clustered_devices = model.transform(device_profiles)
            
            # Evaluate clustering
            evaluator = ClusteringEvaluator(featuresCol="scaled_features")
            silhouette = evaluator.evaluate(clustered_devices)
            logger.info(f"📊 Device clustering silhouette score: {silhouette:.3f}")
            
            # Show cluster summary
            cluster_summary = clustered_devices.groupBy("prediction") \
                .agg(
                    count("*").alias("device_count"),
                    avg("avg_temperature").alias("cluster_avg_temp"),
                    avg("avg_humidity").alias("cluster_avg_humidity"),
                    avg("avg_battery").alias("cluster_avg_battery")
                ) \
                .orderBy("prediction")
                
            logger.info("🎯 Device Cluster Summary:")
            cluster_summary.show()
            
            return model
            
        except Exception as e:
            logger.error(f"❌ Device clustering training failed: {e}")
            raise
            
    def train_failure_prediction_model(self, df):
        """Train binary classification model for device failure prediction"""
        try:
            logger.info("⚠️ Training failure prediction model...")
            
            # Create failure labels based on multiple criteria
            failure_conditions = (
                (col("battery_level") < 10) |
                (col("signal_strength") < 20) |
                (col("temperature") > 60) |
                (col("temperature") < -10)
            )
            
            df_failure = df.withColumn("failure_risk", 
                                     when(failure_conditions, 1).otherwise(0))
            
            # Balance the dataset
            failure_count = df_failure.filter(col("failure_risk") == 1).count()
            normal_count = df_failure.filter(col("failure_risk") == 0).count()
            
            # Sample down majority class if needed
            if normal_count > failure_count * 3:
                normal_sample = df_failure.filter(col("failure_risk") == 0).sample(
                    fraction=min(1.0, (failure_count * 3) / normal_count), seed=42)
                failure_sample = df_failure.filter(col("failure_risk") == 1)
                df_failure = normal_sample.union(failure_sample)
            
            # Select features
            feature_cols = ["temperature", "humidity", "pressure", "battery_level", 
                           "signal_strength", "hour_of_day", "temp_humidity_ratio",
                           "battery_signal_score", "is_weekend", "is_night"]
            
            # Prepare ML pipeline
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            rf_classifier = RandomForestClassifier(featuresCol="scaled_features", 
                                                 labelCol="failure_risk", numTrees=100, seed=42)
            
            pipeline = Pipeline(stages=[assembler, scaler, rf_classifier])
            
            # Split data
            train_df, test_df = df_failure.randomSplit([0.8, 0.2], seed=42)
            
            # Train model
            model = pipeline.fit(train_df)
            self.models['failure_prediction'] = model
            
            # Evaluate model
            predictions = model.transform(test_df)
            
            # Calculate metrics
            tp = predictions.filter((col("failure_risk") == 1) & (col("prediction") == 1)).count()
            fp = predictions.filter((col("failure_risk") == 0) & (col("prediction") == 1)).count()
            tn = predictions.filter((col("failure_risk") == 0) & (col("prediction") == 0)).count()
            fn = predictions.filter((col("failure_risk") == 1) & (col("prediction") == 0)).count()
            
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            
            logger.info(f"📊 Failure Prediction Model Metrics:")
            logger.info(f"   • Precision: {precision:.3f}")
            logger.info(f"   • Recall: {recall:.3f}")
            logger.info(f"   • F1-Score: {f1_score:.3f}")
            
            # Generate high-risk device predictions
            high_risk_predictions = model.transform(df) \
                .filter(col("prediction") == 1) \
                .select("device_id", "timestamp", "probability", 
                       current_timestamp().alias("created_at")) \
                .withColumn("prediction_type", lit("failure_risk")) \
                .withColumn("model_version", lit("v1.0")) \
                .withColumn("predicted_value", expr("probability[1]")) \
                .withColumn("confidence", expr("probability[1]")) \
                .select("device_id", "timestamp", "prediction_type", 
                       "predicted_value", "confidence", "model_version", "created_at")
            
            self.save_predictions(high_risk_predictions, "ml_predictions", "failure_prediction")
            
            return model
            
        except Exception as e:
            logger.error(f"❌ Failure prediction training failed: {e}")
            raise
            
    def save_predictions(self, df, table_name, model_type):
        """Save ML predictions to PostgreSQL"""
        try:
            if df.count() > 0:
                df.write \
                    .mode("append") \
                    .jdbc(
                        url=self.postgres_url,
                        table=table_name,
                        properties=self.postgres_properties
                    )
                logger.info(f"💾 Saved {df.count()} {model_type} predictions to {table_name}")
            else:
                logger.info(f"ℹ️ No {model_type} predictions to save")
                
        except Exception as e:
            logger.error(f"❌ Failed to save {model_type} predictions: {e}")
            raise
            
    def run_complete_ml_pipeline(self, days_back: int = 30):
        """Run the complete ML pipeline"""
        logger.info("🚀 Starting complete ML pipeline...")
        
        try:
            # Initialize Spark
            self.initialize_spark()
            
            # Load training data
            df = self.load_training_data(days_back)
            
            if df.count() == 0:
                logger.warning("⚠️ No training data available")
                return
                
            # Feature engineering
            df_features = self.feature_engineering(df)
            df_features.cache()  # Cache as we'll use it multiple times
            
            # Train all models
            logger.info("🤖 Training ML models...")
            
            # 1. Anomaly Detection
            anomaly_model = self.train_anomaly_detection_model(df_features)
            
            # 2. Predictive Maintenance
            maintenance_model = self.train_predictive_maintenance_model(df_features)
            
            # 3. Device Clustering
            clustering_model = self.train_device_clustering_model(df_features)
            
            # 4. Failure Prediction
            failure_model = self.train_failure_prediction_model(df_features)
            
            logger.info("✅ ML pipeline completed successfully")
            logger.info(f"📊 Trained {len(self.models)} models:")
            for model_name in self.models.keys():
                logger.info(f"   • {model_name}")
                
        except Exception as e:
            logger.error(f"❌ ML pipeline failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                
    def run_inference_pipeline(self):
        """Run inference on recent data using trained models"""
        logger.info("🔮 Running inference pipeline...")
        
        try:
            self.initialize_spark()
            
            # Load recent data (last 24 hours)
            df = self.load_training_data(days_back=1)
            
            if df.count() == 0:
                logger.warning("⚠️ No recent data for inference")
                return
                
            # Apply feature engineering
            df_features = self.feature_engineering(df)
            
            # Run inference with each model (if they exist)
            # Note: In production, you would load saved models from disk/MLflow
            logger.info("🔮 Inference would be performed with pre-trained models")
            logger.info(f"📊 Processing {df_features.count()} records for inference")
            
        except Exception as e:
            logger.error(f"❌ Inference pipeline failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Main function"""
    import sys
    
    ml_pipeline = IoTMLPipeline()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "train":
            days_back = int(sys.argv[2]) if len(sys.argv) > 2 else 30
            ml_pipeline.run_complete_ml_pipeline(days_back)
            
        elif command == "inference":
            ml_pipeline.run_inference_pipeline()
            
        else:
            print("Usage: python iot_ml_pipeline.py [train|inference] [days_back]")
    else:
        # Default: run training
        ml_pipeline.run_complete_ml_pipeline()

if __name__ == "__main__":
    main()
