"""
Real Data Ingestion Pipeline
Collects data from various APIs and publishes to Kafka topics
"""

import tweepy
import praw
import requests
import json
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
import os
from typing import Dict, List, Any
import schedule
import threading
from textblob import TextBlob
import yfinance as yf
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngestionPipeline:
    def __init__(self):
        self.kafka_producer = self._create_kafka_producer()
        self.twitter_api = self._setup_twitter_api()
        self.reddit_api = self._setup_reddit_api()
        self.last_twitter_id = None
        self.last_reddit_timestamp = None
        
    def _create_kafka_producer(self):
        """Create Kafka producer"""
        return KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            retries=3,
            acks='all'
        )
    
    def _setup_twitter_api(self):
        """Setup Twitter API client"""
        try:
            bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
            if not bearer_token:
                logger.warning("Twitter Bearer Token not found")
                return None
                
            return tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
        except Exception as e:
            logger.error(f"Error setting up Twitter API: {str(e)}")
            return None
    
    def _setup_reddit_api(self):
        """Setup Reddit API client"""
        try:
            client_id = os.getenv('REDDIT_CLIENT_ID')
            client_secret = os.getenv('REDDIT_CLIENT_SECRET')
            
            if not client_id or not client_secret:
                logger.warning("Reddit API credentials not found")
                return None
                
            return praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent="RealDataPipeline/1.0"
            )
        except Exception as e:
            logger.error(f"Error setting up Reddit API: {str(e)}")
            return None
    
    def calculate_sentiment(self, text: str) -> float:
        """Calculate sentiment score using TextBlob"""
        try:
            blob = TextBlob(text)
            return blob.sentiment.polarity
        except Exception as e:
            logger.warning(f"Error calculating sentiment: {str(e)}")
            return 0.0
    
    def ingest_twitter_data(self):
        """Ingest data from Twitter API"""
        if not self.twitter_api:
            logger.warning("Twitter API not available")
            return
            
        try:
            logger.info("Ingesting Twitter data...")
            
            # Search for tweets about trending topics
            search_terms = [
                "bitcoin OR cryptocurrency OR crypto",
                "stock market OR stocks OR trading",
                "AI OR artificial intelligence OR machine learning",
                "climate change OR global warming",
                "tech news OR technology"
            ]
            
            for term in search_terms:
                tweets = tweepy.Paginator(
                    self.twitter_api.search_recent_tweets,
                    query=f"{term} -is:retweet lang:en",
                    tweet_fields=['created_at', 'author_id', 'public_metrics', 'entities', 'lang', 'geo'],
                    max_results=100
                ).flatten(limit=500)
                
                for tweet in tweets:
                    try:
                        # Skip if we've already processed this tweet
                        if self.last_twitter_id and tweet.id <= self.last_twitter_id:
                            continue
                            
                        # Extract hashtags and mentions
                        hashtags = []
                        mentions = []
                        
                        if tweet.entities:
                            if 'hashtags' in tweet.entities:
                                hashtags = [tag['tag'] for tag in tweet.entities['hashtags']]
                            if 'mentions' in tweet.entities:
                                mentions = [mention['username'] for mention in tweet.entities['mentions']]
                        
                        # Calculate sentiment
                        sentiment_score = self.calculate_sentiment(tweet.text)
                        
                        # Prepare tweet data
                        tweet_data = {
                            "id": str(tweet.id),
                            "text": tweet.text,
                            "user_id": str(tweet.author_id),
                            "username": f"user_{tweet.author_id}",  # API v2 doesn't provide username directly
                            "created_at": tweet.created_at.isoformat(),
                            "retweet_count": tweet.public_metrics['retweet_count'],
                            "like_count": tweet.public_metrics['like_count'],
                            "reply_count": tweet.public_metrics['reply_count'],
                            "hashtags": hashtags,
                            "mentions": mentions,
                            "sentiment_score": sentiment_score,
                            "location": None,  # Geo data rarely available
                            "language": tweet.lang,
                            "search_term": term,
                            "ingested_at": datetime.now().isoformat()
                        }
                        
                        # Send to Kafka
                        self.kafka_producer.send(
                            'twitter_stream',
                            key=str(tweet.id),
                            value=tweet_data
                        )
                        
                        # Update last processed ID
                        if not self.last_twitter_id or tweet.id > self.last_twitter_id:
                            self.last_twitter_id = tweet.id
                            
                    except Exception as e:
                        logger.error(f"Error processing tweet {tweet.id}: {str(e)}")
                
                # Rate limiting
                time.sleep(2)
            
            logger.info("Twitter data ingestion completed")
            
        except Exception as e:
            logger.error(f"Error in Twitter data ingestion: {str(e)}")
    
    def ingest_reddit_data(self):
        """Ingest data from Reddit API"""
        if not self.reddit_api:
            logger.warning("Reddit API not available")
            return
            
        try:
            logger.info("Ingesting Reddit data...")
            
            # Subreddits to monitor
            subreddits = [
                'technology', 'investing', 'cryptocurrency', 'science',
                'worldnews', 'business', 'artificial', 'MachineLearning'
            ]
            
            for subreddit_name in subreddits:
                try:
                    subreddit = self.reddit_api.subreddit(subreddit_name)
                    
                    # Get new posts
                    for post in subreddit.new(limit=50):
                        try:
                            # Skip if we've already processed posts after this timestamp
                            post_timestamp = datetime.fromtimestamp(post.created_utc)
                            if (self.last_reddit_timestamp and 
                                post_timestamp <= self.last_reddit_timestamp):
                                continue
                            
                            # Calculate sentiment
                            content = f"{post.title} {post.selftext}"
                            sentiment_score = self.calculate_sentiment(content)
                            
                            # Prepare Reddit data
                            reddit_data = {
                                "id": post.id,
                                "title": post.title,
                                "selftext": post.selftext,
                                "author": str(post.author) if post.author else "[deleted]",
                                "subreddit": subreddit_name,
                                "created_utc": post_timestamp.isoformat(),
                                "score": post.score,
                                "upvote_ratio": post.upvote_ratio,
                                "num_comments": post.num_comments,
                                "url": post.url,
                                "sentiment_score": sentiment_score,
                                "is_self": post.is_self,
                                "ingested_at": datetime.now().isoformat()
                            }
                            
                            # Send to Kafka
                            self.kafka_producer.send(
                                'reddit_stream',
                                key=post.id,
                                value=reddit_data
                            )
                            
                            # Update last processed timestamp
                            if (not self.last_reddit_timestamp or 
                                post_timestamp > self.last_reddit_timestamp):
                                self.last_reddit_timestamp = post_timestamp
                                
                        except Exception as e:
                            logger.error(f"Error processing Reddit post {post.id}: {str(e)}")
                    
                    # Rate limiting
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error accessing subreddit {subreddit_name}: {str(e)}")
            
            logger.info("Reddit data ingestion completed")
            
        except Exception as e:
            logger.error(f"Error in Reddit data ingestion: {str(e)}")
    
    def ingest_news_data(self):
        """Ingest news data from News API"""
        try:
            logger.info("Ingesting news data...")
            
            api_key = os.getenv('NEWS_API_KEY')
            if not api_key:
                logger.warning("News API key not found")
                return
            
            base_url = "https://newsapi.org/v2/top-headlines"
            
            # Categories to fetch
            categories = ['business', 'technology', 'science', 'general']
            
            for category in categories:
                try:
                    params = {
                        'apiKey': api_key,
                        'category': category,
                        'language': 'en',
                        'pageSize': 100
                    }
                    
                    response = requests.get(base_url, params=params)
                    response.raise_for_status()
                    
                    news_data = response.json()
                    
                    for article in news_data.get('articles', []):
                        try:
                            # Skip articles without essential data
                            if not article.get('title') or not article.get('publishedAt'):
                                continue
                            
                            # Calculate sentiment
                            content = f"{article.get('title', '')} {article.get('description', '')}"
                            sentiment_score = self.calculate_sentiment(content)
                            
                            # Prepare news data
                            news_item = {
                                "id": f"news_{hash(article['url'])}",  # Generate ID from URL hash
                                "title": article['title'],
                                "description": article.get('description', ''),
                                "content": article.get('content', ''),
                                "author": article.get('author', 'Unknown'),
                                "source": article['source']['name'],
                                "published_at": article['publishedAt'],
                                "url": article['url'],
                                "category": category,
                                "sentiment_score": sentiment_score,
                                "language": 'en',
                                "ingested_at": datetime.now().isoformat()
                            }
                            
                            # Send to Kafka
                            self.kafka_producer.send(
                                'news_stream',
                                key=news_item['id'],
                                value=news_item
                            )
                            
                        except Exception as e:
                            logger.error(f"Error processing news article: {str(e)}")
                    
                    # Rate limiting
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error fetching news for category {category}: {str(e)}")
            
            logger.info("News data ingestion completed")
            
        except Exception as e:
            logger.error(f"Error in news data ingestion: {str(e)}")
    
    def ingest_financial_data(self):
        """Ingest financial data from various sources"""
        try:
            logger.info("Ingesting financial data...")
            
            # Stock symbols to track
            stock_symbols = [
                'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
                'SPY', 'QQQ', 'VTI', 'BTC-USD', 'ETH-USD'
            ]
            
            for symbol in stock_symbols:
                try:
                    # Get stock data
                    ticker = yf.Ticker(symbol)
                    
                    # Get current data
                    info = ticker.info
                    history = ticker.history(period="1d", interval="1m")
                    
                    if history.empty:
                        continue
                    
                    # Get latest price data
                    latest_data = history.iloc[-1]
                    
                    # Determine if it's crypto or stock
                    is_crypto = symbol.endswith('-USD')
                    
                    if is_crypto:
                        # Crypto data structure
                        crypto_data = {
                            "symbol": symbol,
                            "price_usd": float(latest_data['Close']),
                            "price_btc": float(latest_data['Close']) / 50000 if symbol != 'BTC-USD' else 1.0,  # Approximate
                            "volume_24h": float(latest_data['Volume']),
                            "market_cap": info.get('marketCap', 0),
                            "change_1h": 0.0,  # Not available in yfinance
                            "change_24h": float(((latest_data['Close'] - latest_data['Open']) / latest_data['Open']) * 100),
                            "change_7d": 0.0,  # Not available in yfinance
                            "timestamp": datetime.now().isoformat(),
                            "rank": 1 if symbol == 'BTC-USD' else 2 if symbol == 'ETH-USD' else 100,
                            "circulating_supply": info.get('circulatingSupply', 0),
                            "total_supply": info.get('totalSupply', 0),
                            "ingested_at": datetime.now().isoformat()
                        }
                        
                        self.kafka_producer.send(
                            'financial_crypto',
                            key=symbol,
                            value=crypto_data
                        )
                    else:
                        # Stock data structure
                        stock_data = {
                            "symbol": symbol,
                            "price": float(latest_data['Close']),
                            "volume": int(latest_data['Volume']),
                            "change": float(latest_data['Close'] - latest_data['Open']),
                            "change_percent": float(((latest_data['Close'] - latest_data['Open']) / latest_data['Open']) * 100),
                            "high": float(latest_data['High']),
                            "low": float(latest_data['Low']),
                            "open": float(latest_data['Open']),
                            "previous_close": float(history.iloc[-2]['Close']) if len(history) > 1 else float(latest_data['Open']),
                            "timestamp": datetime.now().isoformat(),
                            "market_cap": info.get('marketCap', 0),
                            "pe_ratio": info.get('trailingPE', 0),
                            "sector": info.get('sector', 'Unknown'),
                            "exchange": info.get('exchange', 'Unknown'),
                            "ingested_at": datetime.now().isoformat()
                        }
                        
                        self.kafka_producer.send(
                            'financial_stocks',
                            key=symbol,
                            value=stock_data
                        )
                
                except Exception as e:
                    logger.error(f"Error processing symbol {symbol}: {str(e)}")
                
                # Rate limiting
                time.sleep(0.5)
            
            logger.info("Financial data ingestion completed")
            
        except Exception as e:
            logger.error(f"Error in financial data ingestion: {str(e)}")
    
    def ingest_market_news(self):
        """Ingest financial news using Alpha Vantage API"""
        try:
            logger.info("Ingesting market news...")
            
            api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
            if not api_key:
                logger.warning("Alpha Vantage API key not found")
                return
            
            url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&apikey={api_key}&limit=200"
            
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            
            for article in data.get('feed', []):
                try:
                    # Extract ticker symbols mentioned
                    symbols = [ticker['ticker'] for ticker in article.get('ticker_sentiment', [])]
                    
                    # Calculate sentiment (use provided sentiment or calculate own)
                    sentiment_score = float(article.get('overall_sentiment_score', 0))
                    if sentiment_score == 0:
                        content = f"{article.get('title', '')} {article.get('summary', '')}"
                        sentiment_score = self.calculate_sentiment(content)
                    
                    # Prepare market news data
                    market_news = {
                        "id": f"market_news_{hash(article['url'])}",
                        "title": article['title'],
                        "summary": article['summary'],
                        "url": article['url'],
                        "published_at": article['time_published'],
                        "source": article['source'],
                        "symbols": symbols,
                        "sentiment_score": sentiment_score,
                        "sentiment_label": article.get('overall_sentiment_label', 'Neutral'),
                        "categories": [topic['topic'] for topic in article.get('topics', [])],
                        "ingested_at": datetime.now().isoformat()
                    }
                    
                    # Send to Kafka
                    self.kafka_producer.send(
                        'financial_market_news',
                        key=market_news['id'],
                        value=market_news
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing market news article: {str(e)}")
            
            logger.info("Market news ingestion completed")
            
        except Exception as e:
            logger.error(f"Error in market news ingestion: {str(e)}")
    
    def run_ingestion_cycle(self):
        """Run one complete ingestion cycle"""
        logger.info("Starting data ingestion cycle...")
        
        try:
            # Run all ingestion methods
            self.ingest_twitter_data()
            self.ingest_reddit_data()
            self.ingest_news_data()
            self.ingest_financial_data()
            self.ingest_market_news()
            
            # Flush Kafka producer
            self.kafka_producer.flush()
            
            logger.info("Data ingestion cycle completed")
            
        except Exception as e:
            logger.error(f"Error in ingestion cycle: {str(e)}")
    
    def schedule_ingestion(self):
        """Schedule regular data ingestion"""
        logger.info("Setting up ingestion schedule...")
        
        # Schedule different data sources at different intervals
        schedule.every(5).minutes.do(self.ingest_twitter_data)
        schedule.every(10).minutes.do(self.ingest_reddit_data)
        schedule.every(30).minutes.do(self.ingest_news_data)
        schedule.every(1).minutes.do(self.ingest_financial_data)  # More frequent for financial data
        schedule.every(15).minutes.do(self.ingest_market_news)
        
        # Run scheduled jobs
        while True:
            try:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds
            except KeyboardInterrupt:
                logger.info("Ingestion scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in scheduler: {str(e)}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.kafka_producer.close()
            logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

def main():
    """Main execution function"""
    ingestion_pipeline = DataIngestionPipeline()
    
    try:
        import sys
        if len(sys.argv) > 1 and sys.argv[1] == "--once":
            # Run once and exit
            ingestion_pipeline.run_ingestion_cycle()
        else:
            # Run continuously with scheduling
            ingestion_pipeline.schedule_ingestion()
    except KeyboardInterrupt:
        logger.info("Ingestion pipeline stopped by user")
    except Exception as e:
        logger.error(f"Ingestion pipeline failed: {str(e)}")
    finally:
        ingestion_pipeline.cleanup()

if __name__ == "__main__":
    main()
