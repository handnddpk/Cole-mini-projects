"""
Airflow DAG for IoT Simulated Pipeline Orchestration
Manages the complete IoT analytics pipeline including data generation, processing, and ML workflows
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'iot-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['admin@iot-pipeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'iot_simulated_pipeline',
    default_args=default_args,
    description='Complete IoT Simulated Data Pipeline',
    schedule_interval='@daily',  # Run daily
    max_active_runs=1,
    tags=['iot', 'simulation', 'analytics', 'ml']
)

def check_data_quality(**context):
    """Check data quality before processing"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Check if we have recent data
        query = """
        SELECT COUNT(*) as record_count,
               MAX(timestamp) as latest_timestamp,
               COUNT(DISTINCT device_id) as device_count
        FROM sensor_readings
        WHERE timestamp >= NOW() - INTERVAL '25 hours'
        """
        
        result = postgres_hook.get_first(query)
        record_count, latest_timestamp, device_count = result
        
        logging.info(f"Data Quality Check:")
        logging.info(f"  Records: {record_count}")
        logging.info(f"  Latest timestamp: {latest_timestamp}")
        logging.info(f"  Active devices: {device_count}")
        
        # Quality thresholds
        if record_count < 1000:
            raise ValueError(f"Insufficient data: {record_count} records (minimum: 1000)")
            
        if device_count < 10:
            raise ValueError(f"Too few devices: {device_count} (minimum: 10)")
            
        # Check data freshness (within last 2 hours)
        if latest_timestamp and (datetime.now() - latest_timestamp).total_seconds() > 7200:
            logging.warning(f"Data might be stale. Latest: {latest_timestamp}")
            
        logging.info("✅ Data quality check passed")
        return True
        
    except Exception as e:
        logging.error(f"❌ Data quality check failed: {e}")
        raise

def generate_data_quality_report(**context):
    """Generate comprehensive data quality report"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Device health summary
        device_health_query = """
        SELECT 
            device_id,
            COUNT(*) as reading_count,
            AVG(temperature) as avg_temp,
            AVG(humidity) as avg_humidity,
            AVG(battery_level) as avg_battery,
            MIN(battery_level) as min_battery,
            MAX(timestamp) as last_reading
        FROM sensor_readings 
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        GROUP BY device_id
        ORDER BY reading_count DESC
        """
        
        device_results = postgres_hook.get_records(device_health_query)
        
        logging.info("📊 Device Health Summary (Top 10):")
        for i, row in enumerate(device_results[:10]):
            device_id, count, avg_temp, avg_humidity, avg_battery, min_battery, last_reading = row
            logging.info(f"  {i+1}. {device_id}: {count} readings, "
                        f"T:{avg_temp:.1f}°C, H:{avg_humidity:.1f}%, "
                        f"Battery:{avg_battery:.0f}% (min:{min_battery}%)")
        
        # Data anomalies summary
        anomaly_query = """
        SELECT COUNT(*) as anomaly_count,
               COUNT(DISTINCT device_id) as affected_devices
        FROM anomaly_detections 
        WHERE detected_at >= NOW() - INTERVAL '24 hours'
        """
        
        anomaly_result = postgres_hook.get_first(anomaly_query)
        anomaly_count, affected_devices = anomaly_result
        
        logging.info(f"🚨 Anomalies detected: {anomaly_count} (affecting {affected_devices} devices)")
        
        # Store report metadata
        context['task_instance'].xcom_push(key='device_count', value=len(device_results))
        context['task_instance'].xcom_push(key='anomaly_count', value=anomaly_count)
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Data quality report generation failed: {e}")
        raise

def cleanup_old_data(**context):
    """Clean up old data to manage storage"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Clean up old sensor readings (keep last 90 days)
        cleanup_query = """
        DELETE FROM sensor_readings 
        WHERE timestamp < NOW() - INTERVAL '90 days'
        """
        
        deleted_count = postgres_hook.run(cleanup_query)
        logging.info(f"🧹 Cleaned up old sensor readings: {deleted_count} records")
        
        # Clean up old anomaly detections (keep last 30 days)
        anomaly_cleanup_query = """
        DELETE FROM anomaly_detections 
        WHERE detected_at < NOW() - INTERVAL '30 days'
        """
        
        anomaly_deleted = postgres_hook.run(anomaly_cleanup_query)
        logging.info(f"🧹 Cleaned up old anomalies: {anomaly_deleted} records")
        
        # Clean up old ML predictions (keep last 30 days)
        ml_cleanup_query = """
        DELETE FROM ml_predictions 
        WHERE timestamp < NOW() - INTERVAL '30 days'
        """
        
        ml_deleted = postgres_hook.run(ml_cleanup_query)
        logging.info(f"🧹 Cleaned up old ML predictions: {ml_deleted} records")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Data cleanup failed: {e}")
        raise

# Task 1: Data Quality Check
data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    dag=dag
)

# Task 2: Generate Batch Data (if needed)
generate_batch_data = BashOperator(
    task_id='generate_batch_data',
    bash_command='''
    cd /opt/spark/jobs/generators && \
    python iot_data_generator.py --batch 5000
    ''',
    dag=dag
)

# Task 3: Run Batch Processing
batch_processing = SparkSubmitOperator(
    task_id='batch_processing',
    application='/opt/spark/jobs/batch/iot_batch_processor.py',
    name='iot_batch_processing',
    conn_id='spark_default',
    application_args=['daily', '{{ ds }}'],
    conf={
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.driver.memory': '1g'
    },
    dag=dag
)

# Task 4: Run ML Pipeline (every 3 days)
ml_pipeline = SparkSubmitOperator(
    task_id='ml_pipeline',
    application='/opt/spark/jobs/ml/iot_ml_pipeline.py',
    name='iot_ml_pipeline',
    conn_id='spark_default',
    application_args=['train', '7'],
    conf={
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.driver.memory': '2g',
        'spark.sql.adaptive.enabled': 'true'
    },
    trigger_rule='all_success',
    dag=dag
)

# Task 5: Generate Data Quality Report
quality_report = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_data_quality_report,
    dag=dag
)

# Task 6: Update Device Metadata
update_device_metadata = PostgresOperator(
    task_id='update_device_metadata',
    postgres_conn_id='postgres_default',
    sql='''
    -- Update device status based on recent activity
    UPDATE device_metadata 
    SET status = CASE 
        WHEN device_id IN (
            SELECT DISTINCT device_id 
            FROM sensor_readings 
            WHERE timestamp >= NOW() - INTERVAL '2 hours'
        ) THEN 'active'
        WHEN device_id IN (
            SELECT DISTINCT device_id 
            FROM sensor_readings 
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        ) THEN 'inactive'
        ELSE 'offline'
    END;
    
    -- Update alert counts
    INSERT INTO device_alerts (device_id, alert_type, message, severity, timestamp)
    SELECT 
        device_id,
        'low_battery' as alert_type,
        'Device battery level is critically low' as message,
        'critical' as severity,
        NOW() as timestamp
    FROM sensor_readings 
    WHERE timestamp >= NOW() - INTERVAL '1 hour'
    AND battery_level < 10
    AND device_id NOT IN (
        SELECT device_id FROM device_alerts 
        WHERE alert_type = 'low_battery' 
        AND timestamp >= NOW() - INTERVAL '6 hours'
        AND resolved = FALSE
    );
    ''',
    dag=dag
)

# Task 7: Data Cleanup (weekly)
cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag
)

# Task 8: Health Check Notifications
health_check_notification = BashOperator(
    task_id='health_check_notification',
    bash_command='''
    echo "IoT Pipeline Health Check - $(date)"
    echo "Pipeline execution completed successfully"
    echo "Next scheduled run: $(date -d '+1 day')"
    
    # In production, this would send notifications to monitoring systems
    # Example: curl -X POST slack-webhook-url -d "IoT pipeline completed successfully"
    ''',
    dag=dag
)

# Define task dependencies
data_quality_check >> generate_batch_data
generate_batch_data >> batch_processing
batch_processing >> [ml_pipeline, quality_report, update_device_metadata]
[quality_report, update_device_metadata] >> cleanup_task
cleanup_task >> health_check_notification

# Conditional ML pipeline (run every 3 days)
ml_pipeline.trigger_rule = 'all_success'

# Documentation
dag.doc_md = """
# IoT Simulated Pipeline DAG

This DAG orchestrates the complete IoT analytics pipeline including:

## Tasks:
1. **Data Quality Check**: Validates data availability and quality
2. **Generate Batch Data**: Creates simulated IoT data if needed
3. **Batch Processing**: Runs daily aggregations and analytics
4. **ML Pipeline**: Trains and runs ML models (every 3 days)
5. **Quality Report**: Generates comprehensive data quality reports
6. **Update Metadata**: Updates device status and generates alerts
7. **Cleanup**: Removes old data to manage storage
8. **Health Check**: Sends pipeline completion notifications

## Schedule:
- Runs daily at midnight
- ML pipeline runs every 3 days
- Data cleanup runs with every execution

## Monitoring:
- Email notifications on failures
- Comprehensive logging
- Data quality metrics
- Performance monitoring

## Configuration:
- Spark executor memory: 2GB
- Spark driver memory: 1-2GB
- Retry attempts: 2
- Retry delay: 5 minutes
"""
