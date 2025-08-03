#!/usr/bin/env python3
"""
IoT Batch Analytics Pipeline
Processes historical IoT sensor data to generate daily aggregates and insights
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IoTBatchProcessor:
    def __init__(self):
        self.spark = None
        self.postgres_url = None
        self.postgres_properties = None
        
    def initialize_spark(self):
        """Initialize Spark session with required configurations"""
        try:
            self.spark = SparkSession.builder \
                .appName("IoTBatchAnalytics") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
                .getOrCreate()
                
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark session initialized successfully")
            
            # Setup PostgreSQL connection
            self.postgres_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:" \
                              f"{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'iot_analytics')}"
            
            self.postgres_properties = {
                "user": os.getenv('POSTGRES_USER', 'postgres'),
                "password": os.getenv('POSTGRES_PASSWORD', 'postgres'),
                "driver": "org.postgresql.Driver"
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark: {e}")
            raise
            
    def read_sensor_data(self, start_date: str = None, end_date: str = None):
        """Read sensor data from PostgreSQL with optional date filtering"""
        try:
            query = "(SELECT * FROM sensor_readings"
            
            if start_date and end_date:
                query += f" WHERE timestamp >= '{start_date}' AND timestamp < '{end_date}'"
            elif start_date:
                query += f" WHERE timestamp >= '{start_date}'"
                
            query += " ORDER BY timestamp) AS sensor_data"
            
            df = self.spark.read.jdbc(
                url=self.postgres_url,
                table=query,
                properties=self.postgres_properties
            )
            
            logger.info(f"📊 Read {df.count()} sensor records")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to read sensor data: {e}")
            raise
            
    def calculate_daily_aggregates(self, sensor_df):
        """Calculate daily aggregates for each device"""
        try:
            # Add date column and calculate aggregates
            daily_agg = sensor_df \
                .withColumn("date", to_date(col("timestamp"))) \
                .groupBy("device_id", "date") \
                .agg(
                    avg("temperature").alias("avg_temperature"),
                    max("temperature").alias("max_temperature"),
                    min("temperature").alias("min_temperature"),
                    avg("humidity").alias("avg_humidity"),
                    avg("pressure").alias("avg_pressure"),
                    count("*").alias("total_readings"),
                    avg("battery_level").alias("avg_battery_level"),
                    avg("signal_strength").alias("avg_signal_strength"),
                    current_timestamp().alias("created_at")
                ) \
                .orderBy("device_id", "date")
                
            logger.info(f"📈 Calculated daily aggregates for {daily_agg.count()} device-days")
            return daily_agg
            
        except Exception as e:
            logger.error(f"❌ Failed to calculate daily aggregates: {e}")
            raise
            
    def detect_anomalies(self, sensor_df):
        """Detect anomalies in sensor readings using statistical methods"""
        try:
            # Calculate statistics for anomaly detection
            stats_df = sensor_df.groupBy("device_id") \
                .agg(
                    avg("temperature").alias("avg_temp"),
                    stddev("temperature").alias("std_temp"),
                    avg("humidity").alias("avg_humidity"),
                    stddev("humidity").alias("std_humidity"),
                    avg("pressure").alias("avg_pressure"),
                    stddev("pressure").alias("std_pressure")
                )
            
            # Join back with original data to calculate anomaly scores
            anomaly_df = sensor_df.alias("s") \
                .join(stats_df.alias("st"), col("s.device_id") == col("st.device_id")) \
                .select(
                    col("s.device_id"),
                    col("s.timestamp"),
                    col("s.temperature"),
                    col("s.humidity"),
                    col("s.pressure"),
                    # Calculate Z-scores for anomaly detection
                    abs((col("s.temperature") - col("st.avg_temp")) / col("st.std_temp")).alias("temp_zscore"),
                    abs((col("s.humidity") - col("st.avg_humidity")) / col("st.std_humidity")).alias("humidity_zscore"),
                    abs((col("s.pressure") - col("st.avg_pressure")) / col("st.std_pressure")).alias("pressure_zscore")
                ) \
                .withColumn("anomaly_score", 
                           greatest(col("temp_zscore"), col("humidity_zscore"), col("pressure_zscore"))) \
                .filter(col("anomaly_score") > 3.0) \
                .withColumn("anomaly_type",
                           when(col("temp_zscore") > 3.0, "temperature")
                           .when(col("humidity_zscore") > 3.0, "humidity")
                           .when(col("pressure_zscore") > 3.0, "pressure")
                           .otherwise("multiple")) \
                .withColumn("detected_at", col("timestamp")) \
                .withColumn("features", 
                           to_json(struct(
                               col("temperature"),
                               col("humidity"), 
                               col("pressure"),
                               col("temp_zscore"),
                               col("humidity_zscore"),
                               col("pressure_zscore")
                           ))) \
                .withColumn("created_at", current_timestamp()) \
                .select("device_id", "anomaly_score", "anomaly_type", "detected_at", "features", "created_at")
                
            logger.info(f"🚨 Detected {anomaly_df.count()} anomalies")
            return anomaly_df
            
        except Exception as e:
            logger.error(f"❌ Failed to detect anomalies: {e}")
            raise
            
    def generate_device_health_report(self, sensor_df):
        """Generate device health and performance report"""
        try:
            # Calculate device health metrics
            health_df = sensor_df \
                .groupBy("device_id") \
                .agg(
                    count("*").alias("total_readings"),
                    max("timestamp").alias("last_reading"),
                    avg("battery_level").alias("avg_battery"),
                    min("battery_level").alias("min_battery"),
                    avg("signal_strength").alias("avg_signal"),
                    min("signal_strength").alias("min_signal"),
                    # Data quality metrics
                    sum(when(col("temperature").isNull(), 1).otherwise(0)).alias("temp_null_count"),
                    sum(when(col("humidity").isNull(), 1).otherwise(0)).alias("humidity_null_count"),
                    sum(when(col("pressure").isNull(), 1).otherwise(0)).alias("pressure_null_count")
                ) \
                .withColumn("data_quality_score",
                           1.0 - (col("temp_null_count") + col("humidity_null_count") + col("pressure_null_count")) / 
                           (col("total_readings") * 3)) \
                .withColumn("battery_health", 
                           when(col("avg_battery") > 80, "excellent")
                           .when(col("avg_battery") > 60, "good") 
                           .when(col("avg_battery") > 40, "warning")
                           .otherwise("critical")) \
                .withColumn("connectivity_health",
                           when(col("avg_signal") > 80, "excellent")
                           .when(col("avg_signal") > 60, "good")
                           .when(col("avg_signal") > 40, "warning") 
                           .otherwise("poor"))
                           
            logger.info(f"🏥 Generated health report for {health_df.count()} devices")
            return health_df
            
        except Exception as e:
            logger.error(f"❌ Failed to generate device health report: {e}")
            raise
            
    def save_to_postgres(self, df, table_name, mode="append"):
        """Save DataFrame to PostgreSQL"""
        try:
            df.write \
                .mode(mode) \
                .jdbc(
                    url=self.postgres_url,
                    table=table_name,
                    properties=self.postgres_properties
                )
            logger.info(f"💾 Saved {df.count()} records to {table_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save to {table_name}: {e}")
            raise
            
    def run_daily_batch(self, target_date: str = None):
        """Run the complete daily batch processing pipeline"""
        if not target_date:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
        logger.info(f"🚀 Starting daily batch processing for {target_date}")
        
        try:
            # Initialize Spark
            self.initialize_spark()
            
            # Set date range for processing
            start_date = target_date
            end_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Read sensor data for the target date
            sensor_df = self.read_sensor_data(start_date, end_date)
            
            if sensor_df.count() == 0:
                logger.warning(f"⚠️ No data found for {target_date}")
                return
                
            # Cache the DataFrame as we'll use it multiple times
            sensor_df.cache()
            
            # Calculate daily aggregates
            daily_agg_df = self.calculate_daily_aggregates(sensor_df)
            
            # Save daily aggregates (upsert mode)
            self.save_to_postgres(daily_agg_df, "daily_aggregates", mode="overwrite")
            
            # Detect anomalies
            anomaly_df = self.detect_anomalies(sensor_df)
            
            if anomaly_df.count() > 0:
                self.save_to_postgres(anomaly_df, "anomaly_detections")
                
            # Generate device health report
            health_df = self.generate_device_health_report(sensor_df)
            
            # Show summary statistics
            logger.info("📊 Batch Processing Summary:")
            logger.info(f"   • Processed records: {sensor_df.count()}")
            logger.info(f"   • Daily aggregates: {daily_agg_df.count()}")
            logger.info(f"   • Anomalies detected: {anomaly_df.count()}")
            logger.info(f"   • Devices analyzed: {health_df.count()}")
            
            # Show sample results
            logger.info("\n📈 Sample Daily Aggregates:")
            daily_agg_df.show(10, truncate=False)
            
            if anomaly_df.count() > 0:
                logger.info("\n🚨 Sample Anomalies:")
                anomaly_df.show(5, truncate=False)
                
            logger.info("\n🏥 Device Health Summary:")
            health_df.select("device_id", "battery_health", "connectivity_health", 
                           "data_quality_score", "total_readings").show(10)
            
            logger.info(f"✅ Daily batch processing completed successfully for {target_date}")
            
        except Exception as e:
            logger.error(f"❌ Daily batch processing failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                
    def run_historical_analysis(self, days_back: int = 7):
        """Run analysis on historical data"""
        logger.info(f"📊 Starting historical analysis for last {days_back} days")
        
        try:
            self.initialize_spark()
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            sensor_df = self.read_sensor_data(
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            
            if sensor_df.count() == 0:
                logger.warning("⚠️ No historical data found")
                return
                
            sensor_df.cache()
            
            # Generate comprehensive analytics
            logger.info("\n📈 Historical Trends Analysis:")
            
            # Daily trends
            daily_trends = sensor_df \
                .withColumn("date", to_date(col("timestamp"))) \
                .groupBy("date") \
                .agg(
                    avg("temperature").alias("avg_temp"),
                    avg("humidity").alias("avg_humidity"),
                    avg("pressure").alias("avg_pressure"),
                    count("*").alias("reading_count")
                ) \
                .orderBy("date")
                
            daily_trends.show()
            
            # Device performance analysis
            logger.info("\n📱 Device Performance Analysis:")
            device_performance = sensor_df \
                .groupBy("device_id") \
                .agg(
                    count("*").alias("total_readings"),
                    avg("temperature").alias("avg_temp"),
                    stddev("temperature").alias("temp_variability"),
                    avg("battery_level").alias("avg_battery"),
                    avg("signal_strength").alias("avg_signal")
                ) \
                .orderBy(desc("total_readings"))
                
            device_performance.show(20)
            
            # Correlation analysis
            logger.info("\n🔗 Sensor Correlation Analysis:")
            correlation_matrix = sensor_df.select("temperature", "humidity", "pressure").toPandas().corr()
            print(correlation_matrix)
            
            logger.info("✅ Historical analysis completed")
            
        except Exception as e:
            logger.error(f"❌ Historical analysis failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Main function"""
    import sys
    
    processor = IoTBatchProcessor()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "daily":
            target_date = sys.argv[2] if len(sys.argv) > 2 else None
            processor.run_daily_batch(target_date)
            
        elif command == "historical":
            days_back = int(sys.argv[2]) if len(sys.argv) > 2 else 7
            processor.run_historical_analysis(days_back)
            
        else:
            print("Usage: python iot_batch_processor.py [daily|historical] [date|days]")
    else:
        # Default: run daily batch for yesterday
        processor.run_daily_batch()

if __name__ == "__main__":
    main()
