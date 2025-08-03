"""
Sentiment Analysis and Trend Prediction ML Pipeline
Uses Spark MLlib for real-time sentiment analysis and trend prediction
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import *
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.clustering import KMeans
import os
import logging
from datetime import datetime, timedelta
import joblib
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MLPipelineProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.postgres_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'realdata_warehouse')}"
        self.postgres_properties = {
            "user": os.getenv('POSTGRES_USER', 'postgres'),
            "password": os.getenv('POSTGRES_PASSWORD', 'postgres'),
            "driver": "org.postgresql.Driver"
        }
        self.model_path = "/tmp/models"
        
    def _create_spark_session(self):
        """Create Spark session optimized for ML workloads"""
        return SparkSession.builder \
            .appName("MLPipelineProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.7.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    def prepare_sentiment_training_data(self):
        """Prepare training data for sentiment analysis model"""
        logger.info("Preparing sentiment training data...")
        
        # Read historical social media data with sentiment labels
        social_data = []
        
        # Twitter data
        try:
            twitter_df = self.spark.read.format("delta").load(
                f"{os.getenv('SILVER_LAYER_PATH', '/tmp/silver')}/twitter"
            ).select(
                col("text").alias("content"),
                col("sentiment_score"),
                lit("twitter").alias("source")
            ).filter(col("sentiment_score").isNotNull())
            social_data.append(twitter_df)
        except Exception as e:
            logger.warning(f"Could not load Twitter data: {str(e)}")
        
        # Reddit data
        try:
            reddit_df = self.spark.read.format("delta").load(
                f"{os.getenv('SILVER_LAYER_PATH', '/tmp/silver')}/reddit"
            ).select(
                concat(col("title"), lit(" "), col("selftext")).alias("content"),
                col("sentiment_score"),
                lit("reddit").alias("source")
            ).filter(col("sentiment_score").isNotNull())
            social_data.append(reddit_df)
        except Exception as e:
            logger.warning(f"Could not load Reddit data: {str(e)}")
        
        # News data
        try:
            news_df = self.spark.read.format("delta").load(
                f"{os.getenv('SILVER_LAYER_PATH', '/tmp/silver')}/news"
            ).select(
                concat(col("title"), lit(" "), col("description")).alias("content"),
                col("sentiment_score"),
                lit("news").alias("source")
            ).filter(col("sentiment_score").isNotNull())
            social_data.append(news_df)
        except Exception as e:
            logger.warning(f"Could not load News data: {str(e)}")
        
        if not social_data:
            raise ValueError("No training data available")
        
        # Combine all social data
        combined_df = social_data[0]
        for df in social_data[1:]:
            combined_df = combined_df.union(df)
        
        # Create sentiment labels (positive/negative/neutral)
        labeled_df = combined_df.withColumn(
            "sentiment_label",
            when(col("sentiment_score") > 0.1, 2.0)  # Positive
            .when(col("sentiment_score") < -0.1, 0.0)  # Negative
            .otherwise(1.0)  # Neutral
        ).filter(col("content").isNotNull() & (length(col("content")) > 10))
        
        return labeled_df
    
    def build_sentiment_analysis_model(self, training_data):
        """Build and train sentiment analysis model"""
        logger.info("Building sentiment analysis model...")
        
        # Text preprocessing pipeline
        tokenizer = Tokenizer(inputCol="content", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=10000)
        idf = IDF(inputCol="raw_features", outputCol="features")
        
        # Classifier
        classifier = RandomForestClassifier(
            labelCol="sentiment_label",
            featuresCol="features",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, classifier])
        
        # Split data
        train_data, test_data = training_data.randomSplit([0.8, 0.2], seed=42)
        
        # Train model
        model = pipeline.fit(train_data)
        
        # Evaluate model
        predictions = model.transform(test_data)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="sentiment_label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        logger.info(f"Sentiment model accuracy: {accuracy:.4f}")
        
        # Save model
        model.write().overwrite().save(f"{self.model_path}/sentiment_model")
        
        return model, accuracy
    
    def prepare_trend_prediction_data(self):
        """Prepare data for trend prediction models"""
        logger.info("Preparing trend prediction data...")
        
        # Load historical aggregated data
        try:
            # Social media trends
            social_trends = self.spark.read \
                .jdbc(self.postgres_url, "twitter_daily", properties=self.postgres_properties) \
                .select(
                    col("date"),
                    col("total_tweets").alias("social_volume"),
                    col("avg_sentiment").alias("social_sentiment")
                )
            
            # Market data
            market_trends = self.spark.read \
                .jdbc(self.postgres_url, "stocks_daily", properties=self.postgres_properties) \
                .groupBy("date") \
                .agg(
                    avg("daily_return").alias("market_return"),
                    sum("volume").alias("market_volume"),
                    count("*").alias("active_stocks")
                )
            
            # Join data
            trend_data = social_trends.join(market_trends, "date", "inner") \
                .orderBy("date")
            
            # Create features with lag variables
            window_spec = Window.orderBy("date")
            
            feature_data = trend_data
            for lag in [1, 2, 3, 7]:  # 1, 2, 3, and 7 days lag
                feature_data = feature_data.withColumn(
                    f"social_volume_lag_{lag}",
                    lag("social_volume", lag).over(window_spec)
                ).withColumn(
                    f"social_sentiment_lag_{lag}",
                    lag("social_sentiment", lag).over(window_spec)
                ).withColumn(
                    f"market_return_lag_{lag}",
                    lag("market_return", lag).over(window_spec)
                )
            
            # Create target variable (next day market return)
            feature_data = feature_data.withColumn(
                "target_market_return",
                lead("market_return", 1).over(window_spec)
            )
            
            # Remove rows with null values
            clean_data = feature_data.filter(
                col("target_market_return").isNotNull() &
                col("social_volume_lag_7").isNotNull()
            )
            
            return clean_data
            
        except Exception as e:
            logger.error(f"Error preparing trend prediction data: {str(e)}")
            return None
    
    def build_trend_prediction_model(self, training_data):
        """Build trend prediction model"""
        logger.info("Building trend prediction model...")
        
        # Feature columns
        feature_cols = [col for col in training_data.columns 
                       if col not in ['date', 'target_market_return']]
        
        # Feature vector assembler
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        # Regression model
        regressor = RandomForestRegressor(
            labelCol="target_market_return",
            featuresCol="features",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        # Pipeline
        pipeline = Pipeline(stages=[assembler, regressor])
        
        # Split data
        train_data, test_data = training_data.randomSplit([0.8, 0.2], seed=42)
        
        # Train model
        model = pipeline.fit(train_data)
        
        # Evaluate model
        predictions = model.transform(test_data)
        evaluator = RegressionEvaluator(
            labelCol="target_market_return",
            predictionCol="prediction",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        logger.info(f"Trend prediction model RMSE: {rmse:.4f}")
        
        # Save model
        model.write().overwrite().save(f"{self.model_path}/trend_prediction_model")
        
        return model, rmse
    
    def build_user_clustering_model(self):
        """Build user clustering model for social media users"""
        logger.info("Building user clustering model...")
        
        try:
            # Load user behavior data
            user_data = self.spark.read.format("delta").load(
                f"{os.getenv('SILVER_LAYER_PATH', '/tmp/silver')}/twitter"
            ).groupBy("user_id") \
             .agg(
                 count("*").alias("tweet_count"),
                 avg("sentiment_score").alias("avg_sentiment"),
                 sum("retweet_count").alias("total_retweets"),
                 sum("like_count").alias("total_likes"),
                 countDistinct("hashtags").alias("hashtag_diversity"),
                 max("engagement_rate").alias("max_engagement")
             ).filter(col("tweet_count") >= 5)  # Filter active users
            
            # Feature engineering
            feature_cols = ["tweet_count", "avg_sentiment", "total_retweets", 
                          "total_likes", "hashtag_diversity", "max_engagement"]
            
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            
            # Standardize features
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            
            # K-means clustering
            kmeans = KMeans(featuresCol="scaled_features", k=5, seed=42)
            
            # Pipeline
            pipeline = Pipeline(stages=[assembler, scaler, kmeans])
            
            # Train model
            model = pipeline.fit(user_data)
            
            # Get predictions
            clustered_users = model.transform(user_data)
            
            # Analyze clusters
            cluster_summary = clustered_users.groupBy("prediction") \
                .agg(
                    count("*").alias("user_count"),
                    avg("tweet_count").alias("avg_tweets"),
                    avg("avg_sentiment").alias("cluster_sentiment"),
                    avg("max_engagement").alias("avg_engagement")
                )
            
            cluster_summary.show()
            
            # Save model and results
            model.write().overwrite().save(f"{self.model_path}/user_clustering_model")
            
            # Save cluster results to PostgreSQL
            clustered_users.select("user_id", "prediction") \
                          .withColumnRenamed("prediction", "cluster_id") \
                          .write \
                          .mode("overwrite") \
                          .jdbc(self.postgres_url, "user_clusters", properties=self.postgres_properties)
            
            return model, cluster_summary
            
        except Exception as e:
            logger.error(f"Error building user clustering model: {str(e)}")
            return None, None
    
    def build_anomaly_detection_model(self):
        """Build anomaly detection model for unusual patterns"""
        logger.info("Building anomaly detection model...")
        
        try:
            # Load recent data for anomaly detection
            recent_data = self.spark.read \
                .jdbc(self.postgres_url, "twitter_daily", properties=self.postgres_properties) \
                .select(
                    col("date"),
                    col("total_tweets"),
                    col("avg_sentiment"),
                    col("unique_users"),
                    col("language_diversity")
                ).orderBy("date")
            
            # Calculate rolling statistics
            window_spec = Window.orderBy("date").rowsBetween(-6, 0)  # 7-day window
            
            stats_data = recent_data.withColumn(
                "tweets_rolling_mean", avg("total_tweets").over(window_spec)
            ).withColumn(
                "tweets_rolling_std", stddev("total_tweets").over(window_spec)
            ).withColumn(
                "sentiment_rolling_mean", avg("avg_sentiment").over(window_spec)
            ).withColumn(
                "sentiment_rolling_std", stddev("avg_sentiment").over(window_spec)
            )
            
            # Calculate z-scores
            anomaly_data = stats_data.withColumn(
                "tweets_zscore",
                (col("total_tweets") - col("tweets_rolling_mean")) / col("tweets_rolling_std")
            ).withColumn(
                "sentiment_zscore",
                (col("avg_sentiment") - col("sentiment_rolling_mean")) / col("sentiment_rolling_std")
            )
            
            # Flag anomalies (|z-score| > 2)
            anomalies = anomaly_data.withColumn(
                "is_anomaly",
                (abs(col("tweets_zscore")) > 2) | (abs(col("sentiment_zscore")) > 2)
            ).filter(col("is_anomaly") == True)
            
            # Save anomalies
            anomalies.select(
                col("date"),
                col("total_tweets"),
                col("avg_sentiment"),
                col("tweets_zscore"),
                col("sentiment_zscore"),
                lit("statistical_anomaly").alias("anomaly_type"),
                current_timestamp().alias("detected_at")
            ).write \
             .mode("append") \
             .jdbc(self.postgres_url, "ml_detected_anomalies", properties=self.postgres_properties)
            
            logger.info(f"Detected {anomalies.count()} anomalies")
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Error in anomaly detection: {str(e)}")
            return None
    
    def generate_ml_insights(self):
        """Generate insights using trained models"""
        logger.info("Generating ML insights...")
        
        insights = []
        
        try:
            # Load sentiment model and make predictions on recent data
            sentiment_model = Pipeline.load(f"{self.model_path}/sentiment_model")
            
            # Get recent unprocessed text data
            recent_text = self.spark.read.format("delta").load(
                f"{os.getenv('SILVER_LAYER_PATH', '/tmp/silver')}/twitter"
            ).filter(col("processing_timestamp") > (current_timestamp() - expr("INTERVAL 1 HOUR"))) \
             .select("text", "user_id", "created_at")
            
            if recent_text.count() > 0:
                # Predict sentiment
                sentiment_predictions = sentiment_model.transform(
                    recent_text.withColumnRenamed("text", "content")
                )
                
                # Aggregate predictions
                sentiment_insight = sentiment_predictions.groupBy() \
                    .agg(
                        count("*").alias("total_analyzed"),
                        avg("prediction").alias("avg_predicted_sentiment"),
                        countDistinct("user_id").alias("unique_users")
                    ).withColumn("insight_type", lit("real_time_sentiment")) \
                     .withColumn("generated_at", current_timestamp())
                
                insights.append(sentiment_insight)
            
            # Load trend prediction model
            try:
                trend_model = Pipeline.load(f"{self.model_path}/trend_prediction_model")
                
                # Get latest features for prediction
                latest_features = self.prepare_trend_prediction_data()
                if latest_features is not None and latest_features.count() > 0:
                    latest_row = latest_features.orderBy(desc("date")).limit(1)
                    
                    # Predict next day market trend
                    trend_prediction = trend_model.transform(latest_row)
                    
                    prediction_value = trend_prediction.select("prediction").collect()[0]["prediction"]
                    
                    trend_insight = self.spark.createDataFrame([
                        ("trend_prediction", prediction_value, datetime.now())
                    ], ["insight_type", "predicted_market_return", "generated_at"])
                    
                    insights.append(trend_insight)
                    
            except Exception as e:
                logger.warning(f"Could not generate trend predictions: {str(e)}")
            
            # Combine insights
            if insights:
                # Save insights to PostgreSQL
                for insight in insights:
                    insight.write \
                          .mode("append") \
                          .jdbc(self.postgres_url, "ml_insights", properties=self.postgres_properties)
                
                logger.info(f"Generated {len(insights)} ML insights")
            
        except Exception as e:
            logger.error(f"Error generating ML insights: {str(e)}")
    
    def run_ml_pipeline(self, retrain_models=False):
        """Run the complete ML pipeline"""
        logger.info("Starting ML pipeline...")
        
        try:
            if retrain_models:
                logger.info("Retraining models...")
                
                # Train sentiment analysis model
                sentiment_data = self.prepare_sentiment_training_data()
                if sentiment_data is not None and sentiment_data.count() > 100:
                    self.build_sentiment_analysis_model(sentiment_data)
                
                # Train trend prediction model
                trend_data = self.prepare_trend_prediction_data()
                if trend_data is not None and trend_data.count() > 50:
                    self.build_trend_prediction_model(trend_data)
                
                # Train user clustering model
                self.build_user_clustering_model()
            
            # Run anomaly detection
            self.build_anomaly_detection_model()
            
            # Generate insights using existing models
            self.generate_ml_insights()
            
            logger.info("ML pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"ML pipeline failed: {str(e)}")
            raise

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="ML Pipeline Processor")
    parser.add_argument("--retrain", action="store_true", help="Retrain all models")
    parser.add_argument("--model", choices=["sentiment", "trend", "clustering", "anomaly"], 
                       help="Train specific model only")
    
    args = parser.parse_args()
    
    processor = MLPipelineProcessor()
    
    try:
        if args.model:
            if args.model == "sentiment":
                data = processor.prepare_sentiment_training_data()
                processor.build_sentiment_analysis_model(data)
            elif args.model == "trend":
                data = processor.prepare_trend_prediction_data()
                processor.build_trend_prediction_model(data)
            elif args.model == "clustering":
                processor.build_user_clustering_model()
            elif args.model == "anomaly":
                processor.build_anomaly_detection_model()
        else:
            processor.run_ml_pipeline(retrain_models=args.retrain)
            
    except Exception as e:
        logger.error(f"ML pipeline execution failed: {str(e)}")
        raise
    finally:
        processor.spark.stop()

if __name__ == "__main__":
    main()
