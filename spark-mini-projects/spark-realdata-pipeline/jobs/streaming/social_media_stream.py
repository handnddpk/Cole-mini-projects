"""
Real-time Social Media Streaming Pipeline
Processes Twitter, Reddit, and news data streams in real-time
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

class SocialMediaStreamProcessor:
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
            .appName("SocialMediaStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/social_media") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0") \
            .getOrCreate()
    
    def define_schemas(self):
        """Define schemas for different data sources"""
        twitter_schema = StructType([
            StructField("id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("username", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("retweet_count", IntegerType(), True),
            StructField("like_count", IntegerType(), True),
            StructField("reply_count", IntegerType(), True),
            StructField("hashtags", ArrayType(StringType()), True),
            StructField("mentions", ArrayType(StringType()), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("location", StringType(), True),
            StructField("language", StringType(), True)
        ])
        
        reddit_schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("selftext", StringType(), True),
            StructField("author", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("score", IntegerType(), True),
            StructField("upvote_ratio", DoubleType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("url", StringType(), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("is_self", BooleanType(), True)
        ])
        
        news_schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("content", StringType(), True),
            StructField("author", StringType(), True),
            StructField("source", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("url", StringType(), True),
            StructField("category", StringType(), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("language", StringType(), True)
        ])
        
        return {
            "twitter": twitter_schema,
            "reddit": reddit_schema,
            "news": news_schema
        }
    
    def read_kafka_stream(self, topic, schema):
        """Read data from Kafka topic"""
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
    
    def enrich_social_data(self, df, source_type):
        """Enrich social media data with additional features"""
        enriched_df = df.withColumn("source_type", lit(source_type)) \
                       .withColumn("processing_timestamp", current_timestamp()) \
                       .withColumn("hour_of_day", hour(col("created_at") if source_type != "reddit" else col("created_utc"))) \
                       .withColumn("day_of_week", dayofweek(col("created_at") if source_type != "reddit" else col("created_utc")))
        
        if source_type == "twitter":
            enriched_df = enriched_df.withColumn("engagement_rate", 
                                               (col("retweet_count") + col("like_count") + col("reply_count")) / 
                                               when(col("retweet_count") + col("like_count") + col("reply_count") > 0, 
                                                   col("retweet_count") + col("like_count") + col("reply_count")).otherwise(1))
        elif source_type == "reddit":
            enriched_df = enriched_df.withColumn("engagement_rate", 
                                               col("score") * col("upvote_ratio") / 
                                               when(col("num_comments") > 0, col("num_comments")).otherwise(1))
        
        return enriched_df
    
    def apply_data_quality_rules(self, df, source_type):
        """Apply data quality checks and cleaning"""
        # Remove duplicates
        df_clean = df.dropDuplicates(["id"])
        
        # Filter out null/empty essential fields
        if source_type == "twitter":
            df_clean = df_clean.filter(col("text").isNotNull() & (col("text") != ""))
        elif source_type == "reddit":
            df_clean = df_clean.filter(col("title").isNotNull() & (col("title") != ""))
        elif source_type == "news":
            df_clean = df_clean.filter(col("title").isNotNull() & (col("title") != ""))
        
        # Add data quality flags
        df_clean = df_clean.withColumn("data_quality_score", 
                                     when(col("sentiment_score").isNotNull(), 1.0).otherwise(0.8))
        
        return df_clean
    
    def write_to_bronze_layer(self, df, source_type):
        """Write raw data to bronze layer (Delta Lake)"""
        bronze_path = f"{os.getenv('BRONZE_LAYER_PATH', '/tmp/bronze')}/{source_type}"
        
        query = df.writeStream \
                 .format("delta") \
                 .outputMode("append") \
                 .option("checkpointLocation", f"/tmp/checkpoints/bronze_{source_type}") \
                 .option("path", bronze_path) \
                 .trigger(processingTime="30 seconds") \
                 .start()
        
        return query
    
    def write_to_silver_layer(self, df, source_type):
        """Write cleaned and enriched data to silver layer"""
        silver_path = f"{os.getenv('SILVER_LAYER_PATH', '/tmp/silver')}/{source_type}"
        
        query = df.writeStream \
                 .format("delta") \
                 .outputMode("append") \
                 .option("checkpointLocation", f"/tmp/checkpoints/silver_{source_type}") \
                 .option("path", silver_path) \
                 .trigger(processingTime="1 minute") \
                 .start()
        
        return query
    
    def write_to_postgres(self, df, table_name):
        """Write data to PostgreSQL for real-time analytics"""
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
                 .trigger(processingTime="2 minutes") \
                 .start()
        
        return query
    
    def create_real_time_aggregations(self, df, source_type):
        """Create real-time aggregations for dashboards"""
        if source_type == "twitter":
            # Twitter trending hashtags
            hashtag_trends = df.select(explode(col("hashtags")).alias("hashtag")) \
                              .groupBy("hashtag", window(col("processing_timestamp"), "5 minutes")) \
                              .agg(count("*").alias("mention_count")) \
                              .orderBy(desc("mention_count"))
            
            # Sentiment by hour
            sentiment_hourly = df.groupBy(window(col("processing_timestamp"), "1 hour")) \
                                .agg(avg("sentiment_score").alias("avg_sentiment"),
                                    count("*").alias("tweet_count"))
            
            return {
                "hashtag_trends": hashtag_trends,
                "sentiment_hourly": sentiment_hourly
            }
        
        elif source_type == "reddit":
            # Subreddit activity
            subreddit_activity = df.groupBy("subreddit", window(col("processing_timestamp"), "10 minutes")) \
                                  .agg(count("*").alias("post_count"),
                                      avg("score").alias("avg_score"),
                                      avg("sentiment_score").alias("avg_sentiment"))
            
            return {"subreddit_activity": subreddit_activity}
        
        elif source_type == "news":
            # News by category and sentiment
            news_trends = df.groupBy("category", window(col("processing_timestamp"), "15 minutes")) \
                           .agg(count("*").alias("article_count"),
                               avg("sentiment_score").alias("avg_sentiment"))
            
            return {"news_trends": news_trends}
    
    def start_streaming_pipeline(self):
        """Start the complete streaming pipeline"""
        schemas = self.define_schemas()
        queries = []
        
        # Process each data source
        for source_type in ["twitter", "reddit", "news"]:
            try:
                logger.info(f"Starting {source_type} stream processing...")
                
                # Read from Kafka
                raw_stream = self.read_kafka_stream(f"{source_type}_stream", schemas[source_type])
                
                # Write to bronze layer (raw data)
                bronze_query = self.write_to_bronze_layer(raw_stream, source_type)
                queries.append(bronze_query)
                
                # Clean and enrich data
                enriched_stream = self.enrich_social_data(raw_stream, source_type)
                cleaned_stream = self.apply_data_quality_rules(enriched_stream, source_type)
                
                # Write to silver layer
                silver_query = self.write_to_silver_layer(cleaned_stream, source_type)
                queries.append(silver_query)
                
                # Create real-time aggregations
                aggregations = self.create_real_time_aggregations(cleaned_stream, source_type)
                
                # Write aggregations to PostgreSQL for dashboards
                for agg_name, agg_df in aggregations.items():
                    agg_query = self.write_to_postgres(agg_df, f"{source_type}_{agg_name}")
                    queries.append(agg_query)
                
                logger.info(f"{source_type} stream processing started successfully")
                
            except Exception as e:
                logger.error(f"Error starting {source_type} stream: {str(e)}")
        
        return queries
    
    def monitor_streaming_queries(self, queries):
        """Monitor streaming queries and handle failures"""
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping all streaming queries...")
            for query in queries:
                query.stop()
        except Exception as e:
            logger.error(f"Streaming error: {str(e)}")
            for query in queries:
                query.stop()
            raise

def main():
    """Main execution function"""
    processor = SocialMediaStreamProcessor()
    
    try:
        # Start streaming pipeline
        queries = processor.start_streaming_pipeline()
        
        # Monitor queries
        processor.monitor_streaming_queries(queries)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        processor.spark.stop()

if __name__ == "__main__":
    main()
