"""
Comprehensive Real Data Pipeline DAG
Orchestrates the complete data pipeline from ingestion to analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
import requests
import json
import pandas as pd
import os
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'hadoop-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Create DAG
dag = DAG(
    'comprehensive_real_data_pipeline',
    default_args=default_args,
    description='Complete real data pipeline with Hadoop ecosystem',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['hadoop', 'hive', 'pig', 'sqoop', 'spark', 'real-data'],
)

# Python functions for data ingestion
def ingest_financial_data(**context):
    """Ingest financial data from multiple APIs"""
    import yfinance as yf
    import requests
    from datetime import datetime, timedelta
    
    logging.info("Starting financial data ingestion...")
    
    # S&P 500 symbols (top 50 for demo)
    symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
        'BRKB', 'JPM', 'JNJ', 'V', 'PG', 'UNH', 'MA', 'HD', 'DIS', 'PYPL',
        'BAC', 'ADBE', 'CRM', 'NFLX', 'INTC', 'KO', 'PEP', 'T', 'VZ', 'MRK',
        'WMT', 'ABBV', 'PFE', 'CSCO', 'NKE', 'TMO', 'ABT', 'CVX', 'LLY',
        'AVGO', 'ACN', 'MDT', 'TXN', 'HON', 'QCOM', 'COST', 'NEE', 'BMY',
        'UPS', 'LOW', 'IBM', 'LMT'
    ]
    
    financial_data = []
    
    for symbol in symbols:
        try:
            # Fetch data from yfinance
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")
            
            for date, row in hist.iterrows():
                record = {
                    'symbol': symbol,
                    'date': date.strftime('%Y-%m-%d'),
                    'open': float(row['Open']),
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume']),
                    'timestamp': datetime.now().isoformat()
                }
                financial_data.append(record)
            
            logging.info(f"Fetched data for {symbol}")
            
        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {e}")
            continue
    
    # Save to JSON file
    output_file = f"/data/financial/stocks_{context['ds']}.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(financial_data, f, indent=2)
    
    logging.info(f"Saved {len(financial_data)} financial records to {output_file}")
    return len(financial_data)

def ingest_weather_data(**context):
    """Ingest weather data from OpenWeatherMap API"""
    import requests
    import os
    from datetime import datetime
    
    api_key = os.getenv('OPENWEATHER_API_KEY', 'demo_key')
    
    # Major cities worldwide
    cities = [
        'New York,US', 'Los Angeles,US', 'Chicago,US', 'Houston,US', 'Phoenix,US',
        'London,GB', 'Paris,FR', 'Tokyo,JP', 'Sydney,AU', 'Toronto,CA',
        'Mumbai,IN', 'Beijing,CN', 'Berlin,DE', 'Moscow,RU', 'Cairo,EG'
    ]
    
    weather_data = []
    
    for city in cities:
        try:
            if api_key == 'demo_key':
                # Generate mock data if no API key
                weather_record = {
                    'city': city,
                    'country': city.split(',')[1] if ',' in city else 'Unknown',
                    'temperature': round(random.uniform(-10, 35), 2),
                    'feels_like': round(random.uniform(-15, 40), 2),
                    'humidity': random.randint(20, 90),
                    'pressure': random.randint(980, 1030),
                    'weather_main': random.choice(['Clear', 'Clouds', 'Rain', 'Snow']),
                    'weather_description': random.choice(['clear sky', 'few clouds', 'light rain']),
                    'wind_speed': round(random.uniform(0, 20), 2),
                    'visibility': random.randint(5000, 10000),
                    'timestamp': datetime.now().isoformat()
                }
            else:
                # Real API call
                url = "https://api.openweathermap.org/data/2.5/weather"
                params = {
                    'q': city,
                    'appid': api_key,
                    'units': 'metric'
                }
                
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                weather_record = {
                    'city': city,
                    'country': data.get('sys', {}).get('country'),
                    'temperature': data.get('main', {}).get('temp'),
                    'feels_like': data.get('main', {}).get('feels_like'),
                    'humidity': data.get('main', {}).get('humidity'),
                    'pressure': data.get('main', {}).get('pressure'),
                    'weather_main': data.get('weather', [{}])[0].get('main'),
                    'weather_description': data.get('weather', [{}])[0].get('description'),
                    'wind_speed': data.get('wind', {}).get('speed'),
                    'visibility': data.get('visibility'),
                    'timestamp': datetime.now().isoformat()
                }
            
            weather_data.append(weather_record)
            logging.info(f"Fetched weather data for {city}")
            
        except Exception as e:
            logging.error(f"Error fetching weather data for {city}: {e}")
            continue
    
    # Save to JSON file
    output_file = f"/data/weather/weather_{context['ds']}.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(weather_data, f, indent=2)
    
    logging.info(f"Saved {len(weather_data)} weather records to {output_file}")
    return len(weather_data)

def ingest_news_data(**context):
    """Ingest news data from NewsAPI"""
    import requests
    import os
    from datetime import datetime
    
    api_key = os.getenv('NEWS_API_KEY', 'demo_key')
    
    categories = ['business', 'technology', 'science', 'health']
    news_data = []
    
    for category in categories:
        try:
            if api_key == 'demo_key':
                # Generate mock news data
                for i in range(5):
                    news_record = {
                        'category': category,
                        'title': f"Mock {category} news article {i+1}",
                        'description': f"This is a mock {category} news description",
                        'url': f"https://example.com/news/{category}/{i+1}",
                        'published_at': datetime.now().isoformat(),
                        'source': 'Mock News',
                        'sentiment_score': round(random.uniform(-1, 1), 2),
                        'timestamp': datetime.now().isoformat()
                    }
                    news_data.append(news_record)
            else:
                # Real API call
                url = "https://newsapi.org/v2/top-headlines"
                params = {
                    'apiKey': api_key,
                    'category': category,
                    'language': 'en',
                    'pageSize': 20
                }
                
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                for article in data.get('articles', []):
                    news_record = {
                        'category': category,
                        'title': article.get('title'),
                        'description': article.get('description'),
                        'url': article.get('url'),
                        'published_at': article.get('publishedAt'),
                        'source': article.get('source', {}).get('name'),
                        'sentiment_score': 0.0,  # Would need NLP processing
                        'timestamp': datetime.now().isoformat()
                    }
                    news_data.append(news_record)
            
            logging.info(f"Fetched news data for {category}")
            
        except Exception as e:
            logging.error(f"Error fetching news data for {category}: {e}")
            continue
    
    # Save to JSON file
    output_file = f"/data/news/news_{context['ds']}.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(news_data, f, indent=2)
    
    logging.info(f"Saved {len(news_data)} news records to {output_file}")
    return len(news_data)

def validate_data_quality(**context):
    """Validate data quality and completeness"""
    import json
    import os
    from datetime import datetime
    
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'validation_date': context['ds'],
        'results': {}
    }
    
    # Check financial data
    financial_file = f"/data/financial/stocks_{context['ds']}.json"
    if os.path.exists(financial_file):
        with open(financial_file, 'r') as f:
            financial_data = json.load(f)
        
        validation_results['results']['financial'] = {
            'file_exists': True,
            'record_count': len(financial_data),
            'has_required_fields': all('symbol' in record and 'close' in record for record in financial_data[:5]),
            'status': 'PASS' if len(financial_data) > 0 else 'FAIL'
        }
    else:
        validation_results['results']['financial'] = {
            'file_exists': False,
            'status': 'FAIL'
        }
    
    # Check weather data
    weather_file = f"/data/weather/weather_{context['ds']}.json"
    if os.path.exists(weather_file):
        with open(weather_file, 'r') as f:
            weather_data = json.load(f)
        
        validation_results['results']['weather'] = {
            'file_exists': True,
            'record_count': len(weather_data),
            'has_required_fields': all('city' in record and 'temperature' in record for record in weather_data[:5]),
            'status': 'PASS' if len(weather_data) > 0 else 'FAIL'
        }
    else:
        validation_results['results']['weather'] = {
            'file_exists': False,
            'status': 'FAIL'
        }
    
    # Save validation results
    validation_file = f"/data/validation/validation_{context['ds']}.json"
    os.makedirs(os.path.dirname(validation_file), exist_ok=True)
    
    with open(validation_file, 'w') as f:
        json.dump(validation_results, f, indent=2)
    
    logging.info(f"Data validation completed: {validation_results}")
    
    # Raise exception if critical data is missing
    failed_validations = [k for k, v in validation_results['results'].items() if v['status'] == 'FAIL']
    if failed_validations:
        raise ValueError(f"Data validation failed for: {failed_validations}")
    
    return validation_results

# Task definitions
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Data ingestion tasks
ingest_financial_task = PythonOperator(
    task_id='ingest_financial_data',
    python_callable=ingest_financial_data,
    dag=dag,
)

ingest_weather_task = PythonOperator(
    task_id='ingest_weather_data',
    python_callable=ingest_weather_data,
    dag=dag,
)

ingest_news_task = PythonOperator(
    task_id='ingest_news_data',
    python_callable=ingest_news_data,
    dag=dag,
)

# Data validation task
validate_data_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# HDFS upload task
upload_to_hdfs_task = BashOperator(
    task_id='upload_to_hdfs',
    bash_command="""
    # Create HDFS directories
    hdfs dfs -mkdir -p /raw-data/financial/{{ ds }}
    hdfs dfs -mkdir -p /raw-data/weather/{{ ds }}
    hdfs dfs -mkdir -p /raw-data/news/{{ ds }}
    
    # Upload data files
    hdfs dfs -put /data/financial/stocks_{{ ds }}.json /raw-data/financial/{{ ds }}/
    hdfs dfs -put /data/weather/weather_{{ ds }}.json /raw-data/weather/{{ ds }}/
    hdfs dfs -put /data/news/news_{{ ds }}.json /raw-data/news/{{ ds }}/
    
    echo "Data uploaded to HDFS successfully"
    """,
    dag=dag,
)

# Hive table creation and data loading
create_hive_tables_task = HiveOperator(
    task_id='create_hive_tables',
    hql="""
    CREATE DATABASE IF NOT EXISTS real_data_warehouse
    COMMENT 'Real Data Warehouse'
    LOCATION '/warehouse/real_data_warehouse';
    
    USE real_data_warehouse;
    
    CREATE EXTERNAL TABLE IF NOT EXISTS financial_data (
        symbol STRING,
        date STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        timestamp STRING
    )
    PARTITIONED BY (ingestion_date STRING)
    STORED AS TEXTFILE
    LOCATION '/warehouse/financial_data/';
    
    CREATE EXTERNAL TABLE IF NOT EXISTS weather_data (
        city STRING,
        country STRING,
        temperature DOUBLE,
        feels_like DOUBLE,
        humidity INT,
        pressure INT,
        weather_main STRING,
        weather_description STRING,
        wind_speed DOUBLE,
        visibility INT,
        timestamp STRING
    )
    PARTITIONED BY (ingestion_date STRING)
    STORED AS TEXTFILE
    LOCATION '/warehouse/weather_data/';
    
    CREATE EXTERNAL TABLE IF NOT EXISTS news_data (
        category STRING,
        title STRING,
        description STRING,
        url STRING,
        published_at STRING,
        source STRING,
        sentiment_score DOUBLE,
        timestamp STRING
    )
    PARTITIONED BY (ingestion_date STRING)
    STORED AS TEXTFILE
    LOCATION '/warehouse/news_data/';
    """,
    hive_cli_conn_id='hive_default',
    dag=dag,
)

# Pig ETL processing
pig_etl_task = BashOperator(
    task_id='pig_etl_processing',
    bash_command="""
    pig -x mapreduce -f /scripts/comprehensive_etl.pig -param input_date={{ ds }}
    """,
    dag=dag,
)

# Spark analytics job
spark_analytics_task = SparkSubmitOperator(
    task_id='spark_analytics',
    application='/scripts/comprehensive_analytics.py',
    application_args=['{{ ds }}'],
    conf={
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    },
    dag=dag,
)

# Data quality monitoring
data_quality_monitoring_task = BashOperator(
    task_id='data_quality_monitoring',
    bash_command="""
    python /scripts/data_quality_monitor.py --date {{ ds }}
    """,
    dag=dag,
)

# Generate reports
generate_reports_task = BashOperator(
    task_id='generate_reports',
    bash_command="""
    python /scripts/generate_daily_reports.py --date {{ ds }}
    """,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Define task dependencies
start_task >> [ingest_financial_task, ingest_weather_task, ingest_news_task]

[ingest_financial_task, ingest_weather_task, ingest_news_task] >> validate_data_task

validate_data_task >> upload_to_hdfs_task

upload_to_hdfs_task >> create_hive_tables_task

create_hive_tables_task >> pig_etl_task

pig_etl_task >> spark_analytics_task

spark_analytics_task >> data_quality_monitoring_task

data_quality_monitoring_task >> generate_reports_task

generate_reports_task >> end_task
