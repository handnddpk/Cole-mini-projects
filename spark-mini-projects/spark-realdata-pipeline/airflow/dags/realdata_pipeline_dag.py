"""
Real Data Pipeline DAG
Orchestrates the complete data pipeline from ingestion to ML insights
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
import os
import logging

# Default arguments
default_args = {
    'owner': 'realdata-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
dag = DAG(
    'realdata_pipeline_dag',
    default_args=default_args,
    description='Complete real data processing pipeline',
    schedule_interval=timedelta(hours=1),  # Run hourly
    max_active_runs=1,
    tags=['realdata', 'spark', 'streaming', 'ml']
)

def check_data_availability():
    """Check if required data sources are available"""
    logging.info("Checking data availability...")
    
    # Check if Kafka topics have recent data
    # This is a simplified check - in production, you'd use Kafka client
    return True

def trigger_data_ingestion():
    """Trigger data ingestion from external APIs"""
    logging.info("Triggering data ingestion...")
    
    # Execute data collector
    import subprocess
    result = subprocess.run([
        'python', '/opt/airflow/jobs/ingestion/data_collector.py', '--once'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Data ingestion failed: {result.stderr}")
    
    logging.info("Data ingestion completed successfully")
    return True

def validate_streaming_jobs():
    """Validate that streaming jobs are running properly"""
    logging.info("Validating streaming jobs...")
    
    # Check if streaming jobs are healthy
    # This could involve checking Spark UI, logs, or database for recent data
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check for recent data in bronze layer tables
    recent_data_query = """
    SELECT COUNT(*) as count
    FROM (
        SELECT 1 FROM twitter_daily WHERE created_at > NOW() - INTERVAL '2 hours'
        UNION ALL
        SELECT 1 FROM stocks_daily WHERE created_at > NOW() - INTERVAL '2 hours'
    ) recent;
    """
    
    result = postgres_hook.get_first(recent_data_query)
    if result[0] == 0:
        logging.warning("No recent data found in streaming tables")
    else:
        logging.info(f"Found {result[0]} recent records")
    
    return True

def run_data_quality_checks():
    """Run data quality checks on ingested data"""
    logging.info("Running data quality checks...")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check for null values in critical fields
    quality_checks = [
        ("Twitter data completeness", 
         "SELECT COUNT(*) FROM twitter_daily WHERE avg_sentiment IS NULL AND created_at > NOW() - INTERVAL '1 day'"),
        ("Stock data completeness",
         "SELECT COUNT(*) FROM stocks_daily WHERE close_price IS NULL AND created_at > NOW() - INTERVAL '1 day'"),
        ("Duplicate records check",
         "SELECT COUNT(*) - COUNT(DISTINCT date, symbol) FROM stocks_daily WHERE created_at > NOW() - INTERVAL '1 day'")
    ]
    
    for check_name, query in quality_checks:
        result = postgres_hook.get_first(query)
        if result[0] > 0:
            logging.warning(f"{check_name}: Found {result[0]} issues")
        else:
            logging.info(f"{check_name}: PASSED")
    
    return True

def generate_daily_report():
    """Generate daily data processing report"""
    logging.info("Generating daily report...")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Generate summary statistics
    report_queries = {
        'twitter_activity': """
            SELECT 
                DATE(created_at) as date,
                SUM(total_tweets) as total_tweets,
                AVG(avg_sentiment) as avg_sentiment,
                SUM(unique_users) as unique_users
            FROM twitter_daily 
            WHERE created_at > NOW() - INTERVAL '1 day'
            GROUP BY DATE(created_at)
            ORDER BY date DESC;
        """,
        'market_performance': """
            SELECT 
                DATE(created_at) as date,
                COUNT(DISTINCT symbol) as symbols_tracked,
                AVG(daily_return) as avg_return,
                COUNT(*) as total_records
            FROM stocks_daily 
            WHERE created_at > NOW() - INTERVAL '1 day'
            GROUP BY DATE(created_at)
            ORDER BY date DESC;
        """,
        'anomalies_detected': """
            SELECT 
                DATE(detected_at) as date,
                COUNT(*) as anomaly_count,
                STRING_AGG(DISTINCT anomaly_type, ', ') as anomaly_types
            FROM daily_anomalies 
            WHERE detected_at > NOW() - INTERVAL '1 day'
            GROUP BY DATE(detected_at)
            ORDER BY date DESC;
        """
    }
    
    report_data = {}
    for report_name, query in report_queries.items():
        result = postgres_hook.get_records(query)
        report_data[report_name] = result
        logging.info(f"{report_name}: {len(result)} records")
    
    # In production, you might send this report via email or save to a dashboard
    logging.info("Daily report generated successfully")
    return report_data

# Task definitions

# 1. Data availability check
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag
)

# 2. Trigger data ingestion
ingestion_task = PythonOperator(
    task_id='trigger_data_ingestion',
    python_callable=trigger_data_ingestion,
    dag=dag
)

# 3. Wait for data to be processed (small delay)
wait_for_processing = BashOperator(
    task_id='wait_for_processing',
    bash_command='sleep 120',  # Wait 2 minutes
    dag=dag
)

# 4. Run batch processing
batch_processing_task = BashOperator(
    task_id='run_batch_processing',
    bash_command='cd /opt/airflow/jobs && python batch/daily_aggregator.py --date {{ ds }}',
    dag=dag
)

# 5. Validate streaming jobs
validate_streaming_task = PythonOperator(
    task_id='validate_streaming_jobs',
    python_callable=validate_streaming_jobs,
    dag=dag
)

# 6. Run ML pipeline
ml_pipeline_task = BashOperator(
    task_id='run_ml_pipeline',
    bash_command='cd /opt/airflow/jobs && python ml/ml_pipeline.py',
    dag=dag
)

# 7. Data quality checks
quality_check_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

# 8. Generate daily report
report_task = PythonOperator(
    task_id='generate_daily_report',
    python_callable=generate_daily_report,
    dag=dag
)

# 9. Cleanup old data (monthly)
cleanup_task = PostgresOperator(
    task_id='cleanup_old_data',
    postgres_conn_id='postgres_default',
    sql="""
    -- Delete data older than 90 days
    DELETE FROM twitter_daily WHERE created_at < NOW() - INTERVAL '90 days';
    DELETE FROM reddit_daily WHERE created_at < NOW() - INTERVAL '90 days';
    DELETE FROM news_daily WHERE created_at < NOW() - INTERVAL '90 days';
    DELETE FROM stocks_daily WHERE created_at < NOW() - INTERVAL '90 days';
    DELETE FROM crypto_daily WHERE created_at < NOW() - INTERVAL '90 days';
    DELETE FROM trading_alerts WHERE created_at < NOW() - INTERVAL '30 days';
    DELETE FROM daily_anomalies WHERE created_at < NOW() - INTERVAL '60 days';
    """,
    dag=dag
)

# Task dependencies
check_data_task >> ingestion_task >> wait_for_processing
wait_for_processing >> [batch_processing_task, validate_streaming_task]
[batch_processing_task, validate_streaming_task] >> ml_pipeline_task
ml_pipeline_task >> quality_check_task >> report_task
report_task >> cleanup_task
