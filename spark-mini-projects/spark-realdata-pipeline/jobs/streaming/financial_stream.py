"""
Financial Data Streaming Pipeline
Processes real-time stock prices, crypto data, and market news
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import logging
from datetime import datetime
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinancialStreamProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.postgres_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'realdata_warehouse')}"
        self.postgres_properties = {
            "user": os.getenv('POSTGRES_USER', 'postgres'),
            "password": os.getenv('POSTGRES_PASSWORD', 'postgres'),
            "driver": "org.postgresql.Driver"
        }
        
    def _create_spark_session(self):
        """Create Spark session with required configurations"""
        return SparkSession.builder \
            .appName("FinancialStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/financial") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "500") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0") \
            .getOrCreate()
    
    def define_schemas(self):
        """Define schemas for financial data sources"""
        stock_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_percent", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("open", DoubleType(), True),
            StructField("previous_close", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("market_cap", LongType(), True),
            StructField("pe_ratio", DoubleType(), True),
            StructField("sector", StringType(), True),
            StructField("exchange", StringType(), True)
        ])
        
        crypto_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price_usd", DoubleType(), True),
            StructField("price_btc", DoubleType(), True),
            StructField("volume_24h", DoubleType(), True),
            StructField("market_cap", LongType(), True),
            StructField("change_1h", DoubleType(), True),
            StructField("change_24h", DoubleType(), True),
            StructField("change_7d", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("rank", IntegerType(), True),
            StructField("circulating_supply", DoubleType(), True),
            StructField("total_supply", DoubleType(), True)
        ])
        
        market_news_schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("url", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("source", StringType(), True),
            StructField("symbols", ArrayType(StringType()), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("sentiment_label", StringType(), True),
            StructField("categories", ArrayType(StringType()), True)
        ])
        
        return {
            "stocks": stock_schema,
            "crypto": crypto_schema,
            "market_news": market_news_schema
        }
    
    def read_kafka_stream(self, topic, schema):
        """Read financial data from Kafka topic"""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            col("timestamp").alias("kafka_timestamp"),
            col("offset"),
            col("partition"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select(
            col("kafka_timestamp"),
            col("offset"),
            col("partition"),
            col("data.*")
        )
        
        return parsed_df
    
    def calculate_technical_indicators(self, df, data_type):
        """Calculate technical indicators for financial data"""
        if data_type in ["stocks", "crypto"]:
            # Simple moving averages (using window functions)
            window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-9, 0)
            
            df_with_indicators = df.withColumn(
                "sma_10", avg("price" if data_type == "stocks" else "price_usd").over(window_spec)
            ).withColumn(
                "volatility", stddev("price" if data_type == "stocks" else "price_usd").over(window_spec)
            )
            
            # Price momentum
            lag_window = Window.partitionBy("symbol").orderBy("timestamp")
            df_with_indicators = df_with_indicators.withColumn(
                "price_momentum", 
                col("price" if data_type == "stocks" else "price_usd") - 
                lag("price" if data_type == "stocks" else "price_usd", 1).over(lag_window)
            )
            
            # Relative Strength Index (RSI) approximation
            df_with_indicators = df_with_indicators.withColumn(
                "rsi_signal",
                when(col("change_percent" if data_type == "stocks" else "change_24h") > 5, "overbought")
                .when(col("change_percent" if data_type == "stocks" else "change_24h") < -5, "oversold")
                .otherwise("neutral")
            )
            
            return df_with_indicators
        
        return df
    
    def detect_market_anomalies(self, df, data_type):
        """Detect unusual market movements and anomalies"""
        if data_type in ["stocks", "crypto"]:
            # Detect unusual volume
            df_with_anomalies = df.withColumn(
                "volume_anomaly",
                when(col("volume" if data_type == "stocks" else "volume_24h") > 
                     avg("volume" if data_type == "stocks" else "volume_24h").over(
                         Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-20, -1)
                     ) * 2, True).otherwise(False)
            )
            
            # Detect price spikes
            df_with_anomalies = df_with_anomalies.withColumn(
                "price_spike",
                when(abs(col("change_percent" if data_type == "stocks" else "change_24h")) > 10, True).otherwise(False)
            )
            
            # Market trend signal
            df_with_anomalies = df_with_anomalies.withColumn(
                "trend_signal",
                when(col("price_momentum") > 0, "bullish")
                .when(col("price_momentum") < 0, "bearish")
                .otherwise("sideways")
            )
            
            return df_with_anomalies
        
        return df
    
    def enrich_financial_data(self, df, data_type):
        """Enrich financial data with market context"""
        enriched_df = df.withColumn("data_type", lit(data_type)) \
                       .withColumn("processing_timestamp", current_timestamp()) \
                       .withColumn("market_session", 
                                 when(hour(col("timestamp")).between(9, 16), "market_hours")
                                 .otherwise("after_hours"))
        
        # Add market cap categories for stocks
        if data_type == "stocks":
            enriched_df = enriched_df.withColumn(
                "market_cap_category",
                when(col("market_cap") > 10000000000, "large_cap")
                .when(col("market_cap") > 2000000000, "mid_cap")
                .otherwise("small_cap")
            )
        
        # Add crypto ranking categories
        elif data_type == "crypto":
            enriched_df = enriched_df.withColumn(
                "crypto_tier",
                when(col("rank") <= 10, "top_10")
                .when(col("rank") <= 50, "top_50")
                .when(col("rank") <= 100, "top_100")
                .otherwise("altcoin")
            )
        
        return enriched_df
    
    def create_market_alerts(self, df, data_type):
        """Generate trading alerts based on market conditions"""
        alerts_df = df.filter(
            (col("volume_anomaly") == True) |
            (col("price_spike") == True) |
            (abs(col("change_percent" if data_type == "stocks" else "change_24h")) > 15)
        ).select(
            col("symbol"),
            col("timestamp"),
            col("price" if data_type == "stocks" else "price_usd").alias("current_price"),
            col("change_percent" if data_type == "stocks" else "change_24h").alias("change_pct"),
            col("volume" if data_type == "stocks" else "volume_24h").alias("current_volume"),
            col("trend_signal"),
            lit(data_type).alias("asset_type"),
            current_timestamp().alias("alert_timestamp"),
            when(col("volume_anomaly"), "High Volume")
            .when(col("price_spike"), "Price Spike")
            .otherwise("Significant Movement").alias("alert_type")
        )
        
        return alerts_df
    
    def calculate_market_indices(self, stocks_df):
        """Calculate custom market indices and sector performance"""
        # Sector performance
        sector_performance = stocks_df.groupBy("sector", window(col("timestamp"), "5 minutes")) \
            .agg(
                avg("change_percent").alias("avg_sector_change"),
                count("*").alias("stock_count"),
                sum("market_cap").alias("total_market_cap")
            )
        
        # Market breadth indicators
        market_breadth = stocks_df.groupBy(window(col("timestamp"), "5 minutes")) \
            .agg(
                sum(when(col("change_percent") > 0, 1).otherwise(0)).alias("advancing_stocks"),
                sum(when(col("change_percent") < 0, 1).otherwise(0)).alias("declining_stocks"),
                count("*").alias("total_stocks"),
                avg("change_percent").alias("market_avg_change")
            )
        
        return {
            "sector_performance": sector_performance,
            "market_breadth": market_breadth
        }
    
    def write_to_bronze_layer(self, df, data_type):
        """Write raw financial data to bronze layer"""
        bronze_path = f"{os.getenv('BRONZE_LAYER_PATH', '/tmp/bronze')}/financial_{data_type}"
        
        query = df.writeStream \
                 .format("delta") \
                 .outputMode("append") \
                 .option("checkpointLocation", f"/tmp/checkpoints/bronze_financial_{data_type}") \
                 .option("path", bronze_path) \
                 .trigger(processingTime="10 seconds") \
                 .start()
        
        return query
    
    def write_to_silver_layer(self, df, data_type):
        """Write processed financial data to silver layer"""
        silver_path = f"{os.getenv('SILVER_LAYER_PATH', '/tmp/silver')}/financial_{data_type}"
        
        query = df.writeStream \
                 .format("delta") \
                 .outputMode("append") \
                 .option("checkpointLocation", f"/tmp/checkpoints/silver_financial_{data_type}") \
                 .option("path", silver_path) \
                 .trigger(processingTime="30 seconds") \
                 .start()
        
        return query
    
    def write_alerts_to_postgres(self, alerts_df):
        """Write trading alerts to PostgreSQL"""
        def write_alerts_batch(batch_df, batch_id):
            try:
                batch_df.write \
                       .mode("append") \
                       .jdbc(self.postgres_url, "trading_alerts", properties=self.postgres_properties)
                logger.info(f"Alert batch {batch_id} written: {batch_df.count()} alerts")
            except Exception as e:
                logger.error(f"Error writing alert batch {batch_id}: {str(e)}")
        
        query = alerts_df.writeStream \
                        .foreachBatch(write_alerts_batch) \
                        .outputMode("append") \
                        .trigger(processingTime="30 seconds") \
                        .start()
        
        return query
    
    def start_financial_streaming_pipeline(self):
        """Start the complete financial streaming pipeline"""
        schemas = self.define_schemas()
        queries = []
        
        for data_type in ["stocks", "crypto", "market_news"]:
            try:
                logger.info(f"Starting {data_type} financial stream processing...")
                
                # Read from Kafka
                raw_stream = self.read_kafka_stream(f"financial_{data_type}", schemas[data_type])
                
                # Write to bronze layer
                bronze_query = self.write_to_bronze_layer(raw_stream, data_type)
                queries.append(bronze_query)
                
                # Process and enrich data
                if data_type in ["stocks", "crypto"]:
                    # Calculate technical indicators
                    enriched_stream = self.calculate_technical_indicators(raw_stream, data_type)
                    
                    # Detect anomalies
                    anomaly_stream = self.detect_market_anomalies(enriched_stream, data_type)
                    
                    # Final enrichment
                    processed_stream = self.enrich_financial_data(anomaly_stream, data_type)
                    
                    # Generate alerts
                    alerts_stream = self.create_market_alerts(processed_stream, data_type)
                    alert_query = self.write_alerts_to_postgres(alerts_stream)
                    queries.append(alert_query)
                    
                    # Market indices for stocks
                    if data_type == "stocks":
                        indices = self.calculate_market_indices(processed_stream)
                        for index_name, index_df in indices.items():
                            index_query = self.write_to_postgres(index_df, f"market_{index_name}")
                            queries.append(index_query)
                
                else:  # market_news
                    processed_stream = self.enrich_financial_data(raw_stream, data_type)
                
                # Write to silver layer
                silver_query = self.write_to_silver_layer(processed_stream, data_type)
                queries.append(silver_query)
                
                logger.info(f"{data_type} financial stream processing started successfully")
                
            except Exception as e:
                logger.error(f"Error starting {data_type} financial stream: {str(e)}")
        
        return queries
    
    def write_to_postgres(self, df, table_name):
        """Write data to PostgreSQL"""
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
    
    def monitor_streaming_queries(self, queries):
        """Monitor streaming queries"""
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping all financial streaming queries...")
            for query in queries:
                query.stop()
        except Exception as e:
            logger.error(f"Financial streaming error: {str(e)}")
            for query in queries:
                query.stop()
            raise

def main():
    """Main execution function"""
    processor = FinancialStreamProcessor()
    
    try:
        # Start financial streaming pipeline
        queries = processor.start_financial_streaming_pipeline()
        
        # Monitor queries
        processor.monitor_streaming_queries(queries)
        
    except Exception as e:
        logger.error(f"Financial pipeline failed: {str(e)}")
        raise
    finally:
        processor.spark.stop()

if __name__ == "__main__":
    main()
