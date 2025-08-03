"""
Daily Batch Processing Pipeline
Aggregates and processes daily data for historical analysis and reporting
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
import logging
from datetime import datetime, timedelta
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DailyBatchProcessor:
    def __init__(self, processing_date=None):
        self.spark = self._create_spark_session()
        self.processing_date = processing_date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.postgres_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'realdata_warehouse')}"
        self.postgres_properties = {
            "user": os.getenv('POSTGRES_USER', 'postgres'),
            "password": os.getenv('POSTGRES_PASSWORD', 'postgres'),
            "driver": "org.postgresql.Driver"
        }
        
    def _create_spark_session(self):
        """Create Spark session with optimized configurations for batch processing"""
        return SparkSession.builder \
            .appName("DailyBatchProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
            .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.7.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    def read_silver_layer_data(self, data_source, start_date=None, end_date=None):
        """Read data from silver layer for processing"""
        silver_path = f"{os.getenv('SILVER_LAYER_PATH', '/tmp/silver')}/{data_source}"
        
        if start_date and end_date:
            # Read with date filtering for efficiency
            df = self.spark.read.format("delta").load(silver_path) \
                     .filter(col("processing_timestamp").between(start_date, end_date))
        else:
            # Read all data (for full reprocessing)
            df = self.spark.read.format("delta").load(silver_path)
        
        return df
    
    def process_social_media_daily_aggregates(self):
        """Create daily aggregates for social media data"""
        logger.info("Processing social media daily aggregates...")
        
        results = {}
        
        for source in ["twitter", "reddit", "news"]:
            try:
                # Read silver layer data
                df = self.read_silver_layer_data(
                    source, 
                    f"{self.processing_date} 00:00:00",
                    f"{self.processing_date} 23:59:59"
                )
                
                if source == "twitter":
                    # Twitter daily aggregates
                    twitter_daily = df.groupBy(
                        to_date(col("created_at")).alias("date")
                    ).agg(
                        count("*").alias("total_tweets"),
                        countDistinct("user_id").alias("unique_users"),
                        avg("sentiment_score").alias("avg_sentiment"),
                        sum("retweet_count").alias("total_retweets"),
                        sum("like_count").alias("total_likes"),
                        sum("reply_count").alias("total_replies"),
                        collect_set("language").alias("languages_used"),
                        size(collect_set("language")).alias("language_diversity")
                    ).withColumn("source", lit("twitter"))
                    
                    # Top hashtags by day
                    hashtag_daily = df.select(
                        to_date(col("created_at")).alias("date"),
                        explode(col("hashtags")).alias("hashtag")
                    ).groupBy("date", "hashtag") \
                     .agg(count("*").alias("mention_count")) \
                     .filter(col("mention_count") >= 5)  # Filter noise
                    
                    # Top trending hashtags
                    top_hashtags = hashtag_daily.withColumn(
                        "rank", 
                        row_number().over(
                            Window.partitionBy("date").orderBy(desc("mention_count"))
                        )
                    ).filter(col("rank") <= 20)
                    
                    results[f"{source}_daily"] = twitter_daily
                    results[f"{source}_hashtags"] = top_hashtags
                
                elif source == "reddit":
                    # Reddit daily aggregates
                    reddit_daily = df.groupBy(
                        to_date(col("created_utc")).alias("date")
                    ).agg(
                        count("*").alias("total_posts"),
                        countDistinct("author").alias("unique_authors"),
                        countDistinct("subreddit").alias("active_subreddits"),
                        avg("sentiment_score").alias("avg_sentiment"),
                        avg("score").alias("avg_score"),
                        avg("upvote_ratio").alias("avg_upvote_ratio"),
                        sum("num_comments").alias("total_comments")
                    ).withColumn("source", lit("reddit"))
                    
                    # Subreddit activity
                    subreddit_daily = df.groupBy(
                        to_date(col("created_utc")).alias("date"),
                        col("subreddit")
                    ).agg(
                        count("*").alias("post_count"),
                        avg("score").alias("avg_score"),
                        avg("sentiment_score").alias("avg_sentiment"),
                        sum("num_comments").alias("total_comments")
                    ).filter(col("post_count") >= 3)  # Filter low-activity subreddits
                    
                    results[f"{source}_daily"] = reddit_daily
                    results[f"{source}_subreddits"] = subreddit_daily
                
                elif source == "news":
                    # News daily aggregates
                    news_daily = df.groupBy(
                        to_date(col("published_at")).alias("date")
                    ).agg(
                        count("*").alias("total_articles"),
                        countDistinct("source").alias("unique_sources"),
                        countDistinct("author").alias("unique_authors"),
                        avg("sentiment_score").alias("avg_sentiment"),
                        collect_set("category").alias("categories_covered")
                    ).withColumn("source", lit("news"))
                    
                    # News by category
                    category_daily = df.groupBy(
                        to_date(col("published_at")).alias("date"),
                        col("category")
                    ).agg(
                        count("*").alias("article_count"),
                        avg("sentiment_score").alias("avg_sentiment"),
                        countDistinct("source").alias("source_count")
                    )
                    
                    results[f"{source}_daily"] = news_daily
                    results[f"{source}_categories"] = category_daily
                
                logger.info(f"Processed {source} daily aggregates successfully")
                
            except Exception as e:
                logger.error(f"Error processing {source} daily aggregates: {str(e)}")
        
        return results
    
    def process_financial_daily_aggregates(self):
        """Create daily aggregates for financial data"""
        logger.info("Processing financial daily aggregates...")
        
        results = {}
        
        for data_type in ["stocks", "crypto"]:
            try:
                df = self.read_silver_layer_data(
                    f"financial_{data_type}",
                    f"{self.processing_date} 00:00:00", 
                    f"{self.processing_date} 23:59:59"
                )
                
                if data_type == "stocks":
                    # Stock daily OHLCV
                    stock_daily = df.groupBy(
                        to_date(col("timestamp")).alias("date"),
                        col("symbol"),
                        col("sector")
                    ).agg(
                        first("price").alias("open"),
                        max("price").alias("high"),
                        min("price").alias("low"),
                        last("price").alias("close"),
                        sum("volume").alias("volume"),
                        last("market_cap").alias("market_cap"),
                        avg("pe_ratio").alias("avg_pe_ratio")
                    ).withColumn(
                        "daily_return", 
                        (col("close") - col("open")) / col("open") * 100
                    )
                    
                    # Sector performance
                    sector_daily = df.groupBy(
                        to_date(col("timestamp")).alias("date"),
                        col("sector")
                    ).agg(
                        count("symbol").alias("stock_count"),
                        avg("change_percent").alias("avg_change"),
                        sum("volume").alias("total_volume"),
                        sum("market_cap").alias("total_market_cap"),
                        countDistinct(when(col("change_percent") > 0, col("symbol"))).alias("gainers"),
                        countDistinct(when(col("change_percent") < 0, col("symbol"))).alias("losers")
                    )
                    
                    # Market cap analysis
                    market_cap_daily = df.groupBy(
                        to_date(col("timestamp")).alias("date"),
                        col("market_cap_category")
                    ).agg(
                        count("*").alias("stock_count"),
                        avg("change_percent").alias("avg_performance"),
                        sum("market_cap").alias("total_market_cap")
                    )
                    
                    results[f"{data_type}_daily"] = stock_daily
                    results[f"{data_type}_sectors"] = sector_daily
                    results[f"{data_type}_market_cap"] = market_cap_daily
                
                elif data_type == "crypto":
                    # Crypto daily aggregates
                    crypto_daily = df.groupBy(
                        to_date(col("timestamp")).alias("date"),
                        col("symbol"),
                        col("crypto_tier")
                    ).agg(
                        first("price_usd").alias("open_price"),
                        max("price_usd").alias("high_price"),
                        min("price_usd").alias("low_price"),
                        last("price_usd").alias("close_price"),
                        avg("volume_24h").alias("avg_volume"),
                        last("market_cap").alias("market_cap"),
                        last("rank").alias("final_rank")
                    ).withColumn(
                        "daily_return",
                        (col("close_price") - col("open_price")) / col("open_price") * 100
                    )
                    
                    # Crypto tier performance
                    tier_daily = df.groupBy(
                        to_date(col("timestamp")).alias("date"),
                        col("crypto_tier")
                    ).agg(
                        count("*").alias("crypto_count"),
                        avg("change_24h").alias("avg_change"),
                        sum("market_cap").alias("total_market_cap"),
                        avg("volume_24h").alias("avg_volume")
                    )
                    
                    results[f"{data_type}_daily"] = crypto_daily
                    results[f"{data_type}_tiers"] = tier_daily
                
                logger.info(f"Processed {data_type} daily aggregates successfully")
                
            except Exception as e:
                logger.error(f"Error processing {data_type} daily aggregates: {str(e)}")
        
        return results
    
    def calculate_cross_domain_correlations(self):
        """Calculate correlations between different data domains"""
        logger.info("Calculating cross-domain correlations...")
        
        try:
            # Get daily aggregates for correlation analysis
            social_sentiment = self.spark.read \
                .jdbc(self.postgres_url, "twitter_daily", properties=self.postgres_properties) \
                .select("date", "avg_sentiment") \
                .withColumnRenamed("avg_sentiment", "social_sentiment")
            
            market_performance = self.spark.read \
                .jdbc(self.postgres_url, "stocks_sectors", properties=self.postgres_properties) \
                .groupBy("date") \
                .agg(avg("avg_change").alias("market_performance"))
            
            # Join for correlation
            correlation_data = social_sentiment.join(market_performance, "date", "inner")
            
            # Calculate correlation coefficient
            correlation_result = correlation_data.select(
                corr("social_sentiment", "market_performance").alias("sentiment_market_correlation")
            ).collect()[0]["sentiment_market_correlation"]
            
            # Create correlation summary
            correlation_summary = self.spark.createDataFrame([
                (self.processing_date, "social_sentiment_vs_market", correlation_result, datetime.now())
            ], ["date", "correlation_type", "correlation_value", "calculated_at"])
            
            return correlation_summary
            
        except Exception as e:
            logger.error(f"Error calculating correlations: {str(e)}")
            return None
    
    def detect_daily_anomalies(self):
        """Detect anomalies in daily data patterns"""
        logger.info("Detecting daily anomalies...")
        
        anomalies = []
        
        try:
            # Check for unusual social media activity
            twitter_data = self.spark.read \
                .jdbc(self.postgres_url, "twitter_daily", properties=self.postgres_properties) \
                .orderBy("date")
            
            # Calculate rolling averages for anomaly detection
            window_spec = Window.orderBy("date").rowsBetween(-6, -1)  # Previous 7 days
            
            twitter_with_baseline = twitter_data.withColumn(
                "avg_tweets_baseline", avg("total_tweets").over(window_spec)
            ).withColumn(
                "avg_sentiment_baseline", avg("avg_sentiment").over(window_spec)
            )
            
            # Detect anomalies
            twitter_anomalies = twitter_with_baseline.filter(
                (col("total_tweets") > col("avg_tweets_baseline") * 2) |
                (abs(col("avg_sentiment") - col("avg_sentiment_baseline")) > 0.3)
            ).select(
                col("date"),
                lit("twitter_activity").alias("anomaly_type"),
                col("total_tweets").alias("observed_value"),
                col("avg_tweets_baseline").alias("expected_value"),
                lit("High activity or sentiment deviation").alias("description")
            )
            
            anomalies.append(twitter_anomalies)
            
            # Check for unusual market movements
            stocks_data = self.spark.read \
                .jdbc(self.postgres_url, "stocks_sectors", properties=self.postgres_properties)
            
            market_anomalies = stocks_data.filter(
                abs(col("avg_change")) > 5.0  # More than 5% average change
            ).select(
                col("date"),
                lit("market_volatility").alias("anomaly_type"),
                col("avg_change").alias("observed_value"),
                lit(0.0).alias("expected_value"),
                concat(lit("High volatility in "), col("sector")).alias("description")
            )
            
            anomalies.append(market_anomalies)
            
            # Combine all anomalies
            if anomalies:
                all_anomalies = anomalies[0]
                for anomaly_df in anomalies[1:]:
                    all_anomalies = all_anomalies.union(anomaly_df)
                
                return all_anomalies.withColumn("detected_at", current_timestamp())
            
        except Exception as e:
            logger.error(f"Error detecting anomalies: {str(e)}")
        
        return None
    
    def write_to_gold_layer(self, df, table_name):
        """Write processed data to gold layer (Delta Lake)"""
        gold_path = f"{os.getenv('GOLD_LAYER_PATH', '/tmp/gold')}/{table_name}"
        
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .save(gold_path)
        
        logger.info(f"Written {df.count()} records to gold layer: {table_name}")
    
    def write_to_postgres(self, df, table_name, mode="overwrite"):
        """Write results to PostgreSQL"""
        df.write \
          .mode(mode) \
          .jdbc(self.postgres_url, table_name, properties=self.postgres_properties)
        
        logger.info(f"Written {df.count()} records to PostgreSQL: {table_name}")
    
    def run_daily_batch_processing(self):
        """Run the complete daily batch processing pipeline"""
        logger.info(f"Starting daily batch processing for {self.processing_date}")
        
        try:
            # Process social media aggregates
            social_results = self.process_social_media_daily_aggregates()
            
            # Process financial aggregates
            financial_results = self.process_financial_daily_aggregates()
            
            # Combine all results
            all_results = {**social_results, **financial_results}
            
            # Write results to gold layer and PostgreSQL
            for table_name, df in all_results.items():
                if df is not None and df.count() > 0:
                    # Write to gold layer
                    self.write_to_gold_layer(df, table_name)
                    
                    # Write to PostgreSQL for analytics
                    self.write_to_postgres(df, table_name)
            
            # Calculate cross-domain correlations
            correlations = self.calculate_cross_domain_correlations()
            if correlations is not None:
                self.write_to_postgres(correlations, "daily_correlations", mode="append")
            
            # Detect anomalies
            anomalies = self.detect_daily_anomalies()
            if anomalies is not None and anomalies.count() > 0:
                self.write_to_postgres(anomalies, "daily_anomalies", mode="append")
            
            logger.info(f"Daily batch processing completed successfully for {self.processing_date}")
            
        except Exception as e:
            logger.error(f"Daily batch processing failed: {str(e)}")
            raise

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Daily Batch Processing Pipeline")
    parser.add_argument("--date", help="Processing date (YYYY-MM-DD)", default=None)
    parser.add_argument("--reprocess", action="store_true", help="Reprocess historical data")
    
    args = parser.parse_args()
    
    if args.reprocess:
        # Process last 30 days
        end_date = datetime.now()
        for i in range(30):
            processing_date = (end_date - timedelta(days=i)).strftime("%Y-%m-%d")
            processor = DailyBatchProcessor(processing_date)
            try:
                processor.run_daily_batch_processing()
            except Exception as e:
                logger.error(f"Failed to process {processing_date}: {str(e)}")
            finally:
                processor.spark.stop()
    else:
        processor = DailyBatchProcessor(args.date)
        try:
            processor.run_daily_batch_processing()
        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}")
            raise
        finally:
            processor.spark.stop()

if __name__ == "__main__":
    main()
