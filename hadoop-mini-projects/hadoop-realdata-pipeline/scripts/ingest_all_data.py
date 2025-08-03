#!/usr/bin/env python3
"""
Real Data Ingestion Script
Fetches data from multiple external APIs and stores in HDFS
"""

import json
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
OUTPUT_DIR = "/data/raw"
HDFS_BASE_PATH = "/raw-data"

# API Endpoints and configurations
APIS = {
    "stocks": {
        "url": "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/{start_date}/{end_date}",
        "params": {"adjusted": "true", "sort": "asc"},
        "api_key_param": "apikey",
        "rate_limit": 5  # requests per minute
    },
    "weather": {
        "url": "https://api.openweathermap.org/data/2.5/weather",
        "params": {"q": "New York,US", "units": "metric"},
        "api_key_param": "appid",
        "rate_limit": 60
    },
    "crypto": {
        "url": "https://api.coindesk.com/v1/bpi/currentprice.json",
        "params": {},
        "api_key_param": None,
        "rate_limit": 10
    }
}

# Major US cities for weather data
CITIES = [
    "New York,US", "Los Angeles,US", "Chicago,US", "Houston,US", 
    "Phoenix,US", "Philadelphia,US", "San Antonio,US", "San Diego,US"
]

# Popular stocks to track
STOCKS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NFLX", "NVDA"]

class DataIngestionError(Exception):
    """Custom exception for data ingestion errors"""
    pass

class RateLimiter:
    """Simple rate limiter for API calls"""
    def __init__(self, max_calls_per_minute):
        self.max_calls = max_calls_per_minute
        self.calls = []
    
    def wait_if_needed(self):
        now = time.time()
        # Remove calls older than 1 minute
        self.calls = [call_time for call_time in self.calls if now - call_time < 60]
        
        if len(self.calls) >= self.max_calls:
            sleep_time = 60 - (now - self.calls[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached, waiting {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        self.calls.append(now)

def get_api_key(service):
    """Get API key from environment variables"""
    key_map = {
        "stocks": "POLYGON_API_KEY",
        "weather": "OPENWEATHER_API_KEY",
        "alpha_vantage": "ALPHA_VANTAGE_API_KEY"
    }
    
    api_key = os.getenv(key_map.get(service, f"{service.upper()}_API_KEY"))
    if not api_key and service != "crypto":
        logger.warning(f"No API key found for {service}, using demo data")
    return api_key

def fetch_stock_data():
    """Fetch stock market data"""
    logger.info("Fetching stock market data...")
    
    # Use demo data if no API key available
    api_key = get_api_key("stocks")
    if not api_key:
        return generate_demo_stock_data()
    
    rate_limiter = RateLimiter(5)  # 5 calls per minute for free tier
    stock_data = []
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    for symbol in STOCKS:
        try:
            rate_limiter.wait_if_needed()
            
            # Use Alpha Vantage free API as fallback
            url = f"https://www.alphavantage.co/query"
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "apikey": get_api_key("alpha_vantage") or "demo",
                "outputsize": "compact"
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if "Time Series (Daily)" in data:
                time_series = data["Time Series (Daily)"]
                for date, values in time_series.items():
                    stock_record = {
                        "symbol": symbol,
                        "date": date,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"]),
                        "timestamp": datetime.now().isoformat()
                    }
                    stock_data.append(stock_record)
            
            logger.info(f"Fetched data for {symbol}")
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            continue
    
    # Save to file
    if stock_data:
        filename = f"{OUTPUT_DIR}/stocks/stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(stock_data, f, indent=2)
        
        logger.info(f"Saved {len(stock_data)} stock records to {filename}")
    
    return stock_data

def fetch_weather_data():
    """Fetch weather data for major cities"""
    logger.info("Fetching weather data...")
    
    api_key = get_api_key("weather")
    if not api_key:
        return generate_demo_weather_data()
    
    rate_limiter = RateLimiter(60)  # 60 calls per minute for free tier
    weather_data = []
    
    for city in CITIES:
        try:
            rate_limiter.wait_if_needed()
            
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {
                "q": city,
                "appid": api_key,
                "units": "metric"
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            weather_record = {
                "city": city,
                "country": data.get("sys", {}).get("country"),
                "temperature": data.get("main", {}).get("temp"),
                "feels_like": data.get("main", {}).get("feels_like"),
                "humidity": data.get("main", {}).get("humidity"),
                "pressure": data.get("main", {}).get("pressure"),
                "weather_main": data.get("weather", [{}])[0].get("main"),
                "weather_description": data.get("weather", [{}])[0].get("description"),
                "wind_speed": data.get("wind", {}).get("speed"),
                "visibility": data.get("visibility"),
                "timestamp": datetime.now().isoformat()
            }
            weather_data.append(weather_record)
            
            logger.info(f"Fetched weather data for {city}")
            
        except Exception as e:
            logger.error(f"Error fetching weather data for {city}: {e}")
            continue
    
    # Save to file
    if weather_data:
        filename = f"{OUTPUT_DIR}/weather/weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(weather_data, f, indent=2)
        
        logger.info(f"Saved {len(weather_data)} weather records to {filename}")
    
    return weather_data

def fetch_crypto_data():
    """Fetch cryptocurrency data (no API key required)"""
    logger.info("Fetching cryptocurrency data...")
    
    crypto_data = []
    
    try:
        # Bitcoin price from CoinDesk API (free)
        url = "https://api.coindesk.com/v1/bpi/currentprice.json"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        crypto_record = {
            "cryptocurrency": "Bitcoin",
            "symbol": "BTC",
            "price_usd": float(data["bpi"]["USD"]["rate"].replace(",", "")),
            "price_eur": float(data["bpi"]["EUR"]["rate"].replace(",", "")),
            "price_gbp": float(data["bpi"]["GBP"]["rate"].replace(",", "")),
            "last_updated": data["time"]["updated"],
            "timestamp": datetime.now().isoformat()
        }
        crypto_data.append(crypto_record)
        
        logger.info("Fetched Bitcoin price data")
        
    except Exception as e:
        logger.error(f"Error fetching crypto data: {e}")
    
    # Save to file
    if crypto_data:
        filename = f"{OUTPUT_DIR}/crypto/crypto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(crypto_data, f, indent=2)
        
        logger.info(f"Saved {len(crypto_data)} crypto records to {filename}")
    
    return crypto_data

def generate_demo_stock_data():
    """Generate demo stock data when API is not available"""
    logger.info("Generating demo stock data...")
    
    import random
    demo_data = []
    
    for symbol in STOCKS[:3]:  # Limit to 3 stocks for demo
        base_price = random.uniform(100, 300)
        for i in range(30):  # 30 days of data
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            price_change = random.uniform(-0.05, 0.05)  # ±5% daily change
            
            open_price = base_price * (1 + price_change)
            high_price = open_price * (1 + random.uniform(0, 0.03))
            low_price = open_price * (1 - random.uniform(0, 0.03))
            close_price = open_price * (1 + random.uniform(-0.02, 0.02))
            
            demo_data.append({
                "symbol": symbol,
                "date": date,
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": random.randint(1000000, 10000000),
                "timestamp": datetime.now().isoformat()
            })
    
    return demo_data

def generate_demo_weather_data():
    """Generate demo weather data when API is not available"""
    logger.info("Generating demo weather data...")
    
    import random
    demo_data = []
    
    for city in CITIES[:4]:  # Limit to 4 cities for demo
        demo_data.append({
            "city": city,
            "country": "US",
            "temperature": random.uniform(-10, 35),
            "feels_like": random.uniform(-15, 40),
            "humidity": random.randint(20, 90),
            "pressure": random.randint(980, 1030),
            "weather_main": random.choice(["Clear", "Clouds", "Rain", "Snow"]),
            "weather_description": random.choice(["clear sky", "few clouds", "light rain", "heavy snow"]),
            "wind_speed": random.uniform(0, 20),
            "visibility": random.randint(5000, 10000),
            "timestamp": datetime.now().isoformat()
        })
    
    return demo_data

def upload_to_hdfs():
    """Upload collected data to HDFS"""
    logger.info("Uploading data to HDFS...")
    
    try:
        # Create HDFS directories
        os.system("hdfs dfs -mkdir -p /raw-data/stocks")
        os.system("hdfs dfs -mkdir -p /raw-data/weather") 
        os.system("hdfs dfs -mkdir -p /raw-data/crypto")
        
        # Upload files to HDFS
        for data_type in ["stocks", "weather", "crypto"]:
            local_path = f"{OUTPUT_DIR}/{data_type}/"
            hdfs_path = f"/raw-data/{data_type}/"
            
            if os.path.exists(local_path):
                os.system(f"hdfs dfs -put {local_path}*.json {hdfs_path}")
                logger.info(f"Uploaded {data_type} data to HDFS")
        
        # Verify upload
        os.system("hdfs dfs -ls -R /raw-data/")
        
    except Exception as e:
        logger.error(f"Error uploading to HDFS: {e}")
        raise DataIngestionError(f"HDFS upload failed: {e}")

def main():
    """Main ingestion function"""
    logger.info("Starting real data ingestion pipeline...")
    
    # Create output directories
    os.makedirs(f"{OUTPUT_DIR}/stocks", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/weather", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/crypto", exist_ok=True)
    
    # Collect data from all sources
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(fetch_stock_data): "stocks",
            executor.submit(fetch_weather_data): "weather", 
            executor.submit(fetch_crypto_data): "crypto"
        }
        
        results = {}
        for future in as_completed(futures):
            source = futures[future]
            try:
                data = future.result()
                results[source] = data
                logger.info(f"Successfully collected {source} data: {len(data)} records")
            except Exception as e:
                logger.error(f"Failed to collect {source} data: {e}")
                results[source] = []
    
    # Upload to HDFS if data was collected
    total_records = sum(len(data) for data in results.values())
    if total_records > 0:
        upload_to_hdfs()
        logger.info(f"Data ingestion complete! Total records: {total_records}")
    else:
        logger.warning("No data was collected from any source")
    
    # Generate summary report
    summary = {
        "ingestion_time": datetime.now().isoformat(),
        "sources": results,
        "total_records": total_records,
        "status": "success" if total_records > 0 else "partial_failure"
    }
    
    with open(f"{OUTPUT_DIR}/ingestion_summary.json", 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info("Ingestion summary saved")

if __name__ == "__main__":
    main()
