"""
IoT Sensor Data Streaming Pipeline
Processes real-time IoT sensor data from multiple domains: smart city, industrial, environmental
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IoTStreamProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.postgres_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'iot_warehouse')}"
        self.postgres_properties = {
            "user": os.getenv('POSTGRES_USER', 'postgres'),
            "password": os.getenv('POSTGRES_PASSWORD', 'postgres'),
            "driver": "org.postgresql.Driver"
        }
        self.checkpoint_location = "/opt/data/checkpoints"
        
    def _create_spark_session(self):
        """Create Spark session with IoT-optimized configurations"""
        return SparkSession.builder \
            .appName("IoTStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "2000") \
            .config("spark.sql.streaming.metricsEnabled", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0,io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    def define_iot_schemas(self):
        """Define schemas for different IoT sensor types"""
        
        # Traffic sensor schema
        traffic_schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("location", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("street_name", StringType(), True),
                StructField("intersection", StringType(), True)
            ]), True),
            StructField("vehicle_count", IntegerType(), True),
            StructField("avg_speed", DoubleType(), True),
            StructField("congestion_level", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("lane_occupancy", DoubleType(), True),
            StructField("heavy_vehicle_count", IntegerType(), True)
        ])
        
        # Air quality sensor schema
        air_quality_schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("location", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("zone_type", StringType(), True)
            ]), True),
            StructField("pm2_5", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("no2", DoubleType(), True),
            StructField("o3", DoubleType(), True),
            StructField("co", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("air_quality_index", IntegerType(), True)
        ])
        
        # Industrial equipment schema
        industrial_schema = StructType([
            StructField("equipment_id", StringType(), True),
            StructField("facility_id", StringType(), True),
            StructField("equipment_type", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("vibration", DoubleType(), True),
            StructField("power_consumption", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("efficiency_rating", DoubleType(), True),
            StructField("maintenance_due", BooleanType(), True)
        ])
        
        # Weather station schema
        weather_schema = StructType([
            StructField("station_id", StringType(), True),
            StructField("coordinates", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("elevation", DoubleType(), True)
            ]), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_direction", DoubleType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("atmospheric_pressure", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("weather_condition", StringType(), True)
        ])
        
        # Energy meter schema
        energy_schema = StructType([
            StructField("meter_id", StringType(), True),
            StructField("building_id", StringType(), True),
            StructField("building_type", StringType(), True),
            StructField("consumption_kwh", DoubleType(), True),
            StructField("voltage", DoubleType(), True),
            StructField("current", DoubleType(), True),
            StructField("power_factor", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("peak_demand", DoubleType(), True),
            StructField("cost_per_kwh", DoubleType(), True)
        ])
        
        return {
            "traffic": traffic_schema,
            "air_quality": air_quality_schema,
            "industrial": industrial_schema,
            "weather": weather_schema,
            "energy": energy_schema
        }
    
    def read_kafka_stream(self, topic, schema):
        """Read IoT data from Kafka topic with schema"""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.consumer.session.timeout.ms", "30000") \
            .load()
        
        # Parse JSON data with error handling
        parsed_df = df.select(
            col("timestamp").alias("kafka_timestamp"),
            col("offset"),
            col("partition"),
            from_json(col("value").cast("string"), schema).alias("data"),
            col("key").cast("string").alias("sensor_key")
        ).select(
            col("kafka_timestamp"),
            col("offset"),
            col("partition"),
            col("sensor_key"),
            col("data.*")
        ).filter(col("sensor_id").isNotNull())  # Filter out malformed records
        
        return parsed_df
    
    def enrich_iot_data(self, df, sensor_type):
        """Enrich IoT data with additional computed features"""
        enriched_df = df.withColumn("sensor_type", lit(sensor_type)) \
                       .withColumn("processing_timestamp", current_timestamp()) \
                       .withColumn("hour_of_day", hour(col("timestamp"))) \
                       .withColumn("day_of_week", dayofweek(col("timestamp"))) \
                       .withColumn("is_weekend", dayofweek(col("timestamp")).isin([1, 7]))
        
        # Sensor-specific enrichments
        if sensor_type == "traffic":
            enriched_df = enriched_df.withColumn(
                "traffic_flow_rate", 
                col("vehicle_count") / 60.0  # vehicles per minute
            ).withColumn(
                "congestion_score",
                when(col("congestion_level") == "heavy", 3)
                .when(col("congestion_level") == "moderate", 2)
                .when(col("congestion_level") == "light", 1)
                .otherwise(0)
            )
            
        elif sensor_type == "air_quality":
            # Calculate Air Quality Index
            enriched_df = enriched_df.withColumn(
                "calculated_aqi",
                greatest(
                    col("pm2_5") * 2,  # Simplified AQI calculation
                    col("pm10") * 1.5,
                    col("no2") * 0.5,
                    col("o3") * 0.8,
                    col("co") * 0.1
                )
            ).withColumn(
                "air_quality_category",
                when(col("calculated_aqi") <= 50, "Good")
                .when(col("calculated_aqi") <= 100, "Moderate")
                .when(col("calculated_aqi") <= 150, "Unhealthy for Sensitive")
                .when(col("calculated_aqi") <= 200, "Unhealthy")
                .otherwise("Very Unhealthy")
            )
            
        elif sensor_type == "industrial":
            # Calculate equipment efficiency and health score
            enriched_df = enriched_df.withColumn(
                "temperature_anomaly",
                when(col("temperature") > 80, True).otherwise(False)
            ).withColumn(
                "pressure_anomaly", 
                when(col("pressure") > 150, True).otherwise(False)
            ).withColumn(
                "vibration_anomaly",
                when(col("vibration") > 5.0, True).otherwise(False)
            ).withColumn(
                "health_score",
                (100 - 
                 when(col("temperature_anomaly"), 20).otherwise(0) -
                 when(col("pressure_anomaly"), 25).otherwise(0) -
                 when(col("vibration_anomaly"), 15).otherwise(0) -
                 when(col("maintenance_due"), 10).otherwise(0))
            )
            
        elif sensor_type == "weather":
            # Calculate weather indices
            enriched_df = enriched_df.withColumn(
                "heat_index",
                col("temperature") + (col("humidity") / 100.0) * 5  # Simplified heat index
            ).withColumn(
                "wind_chill",
                when(col("temperature") < 10,
                     col("temperature") - (col("wind_speed") * 0.5))
                .otherwise(col("temperature"))
            ).withColumn(
                "weather_severity",
                when((col("wind_speed") > 50) | (col("precipitation") > 50), "severe")
                .when((col("wind_speed") > 25) | (col("precipitation") > 20), "moderate")
                .otherwise("normal")
            )
            
        elif sensor_type == "energy":
            # Calculate energy efficiency metrics
            enriched_df = enriched_df.withColumn(
                "power_kw", col("voltage") * col("current") / 1000.0
            ).withColumn(
                "daily_cost_estimate", col("consumption_kwh") * col("cost_per_kwh") * 24
            ).withColumn(
                "efficiency_rating",
                when(col("power_factor") > 0.95, "excellent")
                .when(col("power_factor") > 0.85, "good")
                .when(col("power_factor") > 0.75, "fair")
                .otherwise("poor")
            )
        
        return enriched_df
    
    def detect_anomalies(self, df, sensor_type):
        """Detect anomalies in IoT sensor data using statistical methods"""
        
        # Define window for statistical calculations
        window_spec = Window.partitionBy("sensor_id").orderBy("timestamp").rowsBetween(-20, -1)
        
        if sensor_type == "traffic":
            df_with_stats = df.withColumn(
                "avg_vehicle_count", avg("vehicle_count").over(window_spec)
            ).withColumn(
                "stddev_vehicle_count", stddev("vehicle_count").over(window_spec)
            )
            
            anomaly_df = df_with_stats.withColumn(
                "vehicle_count_anomaly",
                abs(col("vehicle_count") - col("avg_vehicle_count")) > 
                (col("stddev_vehicle_count") * 2)
            )
            
        elif sensor_type == "air_quality":
            df_with_stats = df.withColumn(
                "avg_pm2_5", avg("pm2_5").over(window_spec)
            ).withColumn(
                "stddev_pm2_5", stddev("pm2_5").over(window_spec)
            )
            
            anomaly_df = df_with_stats.withColumn(
                "air_quality_anomaly",
                (abs(col("pm2_5") - col("avg_pm2_5")) > (col("stddev_pm2_5") * 2)) |
                (col("calculated_aqi") > 200)
            )
            
        elif sensor_type == "industrial":
            anomaly_df = df.withColumn(
                "equipment_anomaly",
                col("temperature_anomaly") | col("pressure_anomaly") | col("vibration_anomaly")
            )
            
        elif sensor_type == "weather":
            anomaly_df = df.withColumn(
                "weather_anomaly",
                (col("weather_severity") == "severe") |
                (col("temperature") < -30) | (col("temperature") > 50) |
                (col("wind_speed") > 100)
            )
            
        elif sensor_type == "energy":
            df_with_stats = df.withColumn(
                "avg_consumption", avg("consumption_kwh").over(window_spec)
            ).withColumn(
                "stddev_consumption", stddev("consumption_kwh").over(window_spec)
            )
            
            anomaly_df = df_with_stats.withColumn(
                "energy_anomaly",
                (abs(col("consumption_kwh") - col("avg_consumption")) > 
                 (col("stddev_consumption") * 3)) |
                (col("power_factor") < 0.7)
            )
        
        else:
            anomaly_df = df.withColumn("anomaly_detected", lit(False))
            
        return anomaly_df
    
    def create_real_time_aggregations(self, df, sensor_type):
        """Create real-time aggregations for dashboards"""
        aggregations = {}
        
        if sensor_type == "traffic":
            # Traffic flow by location
            traffic_agg = df.groupBy(
                window(col("timestamp"), "5 minutes"),
                col("location.street_name").alias("street")
            ).agg(
                avg("vehicle_count").alias("avg_vehicles"),
                avg("avg_speed").alias("avg_speed"),
                count("*").alias("reading_count")
            )
            aggregations["traffic_flow"] = traffic_agg
            
        elif sensor_type == "air_quality":
            # Air quality by zone
            air_agg = df.groupBy(
                window(col("timestamp"), "10 minutes"),
                col("location.zone_type").alias("zone")
            ).agg(
                avg("calculated_aqi").alias("avg_aqi"),
                avg("pm2_5").alias("avg_pm2_5"),
                avg("temperature").alias("avg_temp"),
                count("*").alias("reading_count")
            )
            aggregations["air_quality"] = air_agg
            
        elif sensor_type == "industrial":
            # Equipment health by facility
            industrial_agg = df.groupBy(
                window(col("timestamp"), "15 minutes"),
                col("facility_id"),
                col("equipment_type")
            ).agg(
                avg("health_score").alias("avg_health_score"),
                avg("efficiency_rating").alias("avg_efficiency"),
                count("*").alias("equipment_count"),
                sum(when(col("equipment_anomaly"), 1).otherwise(0)).alias("anomaly_count")
            )
            aggregations["equipment_health"] = industrial_agg
            
        elif sensor_type == "weather":
            # Weather conditions by region
            weather_agg = df.groupBy(
                window(col("timestamp"), "30 minutes")
            ).agg(
                avg("temperature").alias("avg_temperature"),
                avg("humidity").alias("avg_humidity"),
                avg("wind_speed").alias("avg_wind_speed"),
                sum("precipitation").alias("total_precipitation"),
                count("*").alias("station_count")
            )
            aggregations["weather_summary"] = weather_agg
            
        elif sensor_type == "energy":
            # Energy consumption by building type
            energy_agg = df.groupBy(
                window(col("timestamp"), "1 hour"),
                col("building_type")
            ).agg(
                sum("consumption_kwh").alias("total_consumption"),
                avg("power_factor").alias("avg_power_factor"),
                sum("daily_cost_estimate").alias("estimated_cost"),
                count("*").alias("meter_count")
            )
            aggregations["energy_consumption"] = energy_agg
        
        return aggregations
    
    def write_to_delta_lake(self, df, table_name, layer="bronze"):
        """Write data to Delta Lake with different layers"""
        delta_path = f"/opt/data/{layer}/{table_name}"
        
        query = df.writeStream \
                 .format("delta") \
                 .outputMode("append") \
                 .option("checkpointLocation", f"{self.checkpoint_location}/{layer}_{table_name}") \
                 .option("path", delta_path) \
                 .trigger(processingTime="30 seconds") \
                 .start()
        
        return query
    
    def write_to_postgres(self, df, table_name):
        """Write aggregated data to PostgreSQL for dashboards"""
        def write_batch(batch_df, batch_id):
            try:
                batch_df.write \
                       .mode("append") \
                       .jdbc(self.postgres_url, table_name, properties=self.postgres_properties)
                logger.info(f"Batch {batch_id} written to {table_name}: {batch_df.count()} records")
            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to {table_name}: {str(e)}")
        
        query = df.writeStream \
                 .foreachBatch(write_batch) \
                 .outputMode("update") \
                 .trigger(processingTime="1 minute") \
                 .start()
        
        return query
    
    def start_iot_streaming_pipeline(self):
        """Start the complete IoT streaming pipeline"""
        schemas = self.define_iot_schemas()
        queries = []
        
        # Process each IoT sensor type
        for sensor_type in ["traffic", "air_quality", "industrial", "weather", "energy"]:
            try:
                logger.info(f"Starting {sensor_type} sensor stream processing...")
                
                # Read from Kafka
                raw_stream = self.read_kafka_stream(f"iot_{sensor_type}", schemas[sensor_type])
                
                # Write raw data to bronze layer
                bronze_query = self.write_to_delta_lake(raw_stream, f"{sensor_type}_raw", "bronze")
                queries.append(bronze_query)
                
                # Enrich data
                enriched_stream = self.enrich_iot_data(raw_stream, sensor_type)
                
                # Detect anomalies
                anomaly_stream = self.detect_anomalies(enriched_stream, sensor_type)
                
                # Write enriched data to silver layer
                silver_query = self.write_to_delta_lake(anomaly_stream, f"{sensor_type}_enriched", "silver")
                queries.append(silver_query)
                
                # Create real-time aggregations
                aggregations = self.create_real_time_aggregations(anomaly_stream, sensor_type)
                
                # Write aggregations to PostgreSQL and gold layer
                for agg_name, agg_df in aggregations.items():
                    postgres_query = self.write_to_postgres(agg_df, f"{sensor_type}_{agg_name}")
                    gold_query = self.write_to_delta_lake(agg_df, f"{sensor_type}_{agg_name}", "gold")
                    queries.extend([postgres_query, gold_query])
                
                logger.info(f"{sensor_type} sensor stream processing started successfully")
                
            except Exception as e:
                logger.error(f"Error starting {sensor_type} sensor stream: {str(e)}")
        
        return queries
    
    def monitor_streaming_queries(self, queries):
        """Monitor streaming queries and handle failures"""
        try:
            logger.info(f"Monitoring {len(queries)} streaming queries...")
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping all IoT streaming queries...")
            for query in queries:
                query.stop()
        except Exception as e:
            logger.error(f"IoT streaming error: {str(e)}")
            for query in queries:
                query.stop()
            raise

def main():
    """Main execution function"""
    processor = IoTStreamProcessor()
    
    try:
        # Start IoT streaming pipeline
        queries = processor.start_iot_streaming_pipeline()
        
        # Monitor queries
        processor.monitor_streaming_queries(queries)
        
    except Exception as e:
        logger.error(f"IoT pipeline failed: {str(e)}")
        raise
    finally:
        processor.spark.stop()

if __name__ == "__main__":
    main()
