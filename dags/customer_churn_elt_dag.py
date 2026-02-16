"""
Customer Churn ELT Pipeline DAG

Architecture Decision: Separation of Concerns
- Airflow: Orchestration, scheduling, monitoring, retry logic
- Processing Script: Business logic, data transformations
- DuckDB: Data storage and SQL-based transformations

Why this separation?
1. Testability: Processing logic can be tested independently
2. Maintainability: Business logic changes don't require DAG changes
3. Reusability: Processing script can be used outside Airflow
4. Performance: Airflow focuses on orchestration, not data processing
5. Code Quality: Clear boundaries between orchestration and processing

ELT vs ETL:
- ELT: Extract -> Load (raw) -> Transform (in database)
- ETL: Extract -> Transform (in memory) -> Load
- We use ELT because DuckDB's columnar engine is faster than Python for transformations
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import sys
import logging
import os

# Configure logging
logger = logging.getLogger(__name__)

# Import the processing module
sys.path.insert(0, os.getenv('SCRIPTS_PATH', '/opt/airflow/scripts'))
from customer_churn_elt import CustomerChurnELT

# Configuration from environment variables or Airflow Variables
STAGING_DB_PATH = os.getenv('STAGING_DB_PATH', '/opt/airflow/databases/staging.duckdb')
PRODUCTION_DB_PATH = os.getenv('PRODUCTION_DB_PATH', '/opt/airflow/databases/production.duckdb')
CSV_PATH = os.getenv('CSV_PATH', '/opt/airflow/data/customer_churn_data.csv')


def get_elt_processor():
    """Factory function to create CustomerChurnELT instance with configured paths."""
    return CustomerChurnELT(
        staging_db_path=STAGING_DB_PATH,
        production_db_path=PRODUCTION_DB_PATH,
        csv_path=CSV_PATH
    )

# DAG default arguments
default_args = {
    'owner': os.getenv('DAG_OWNER', 'data-team'),
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': int(os.getenv('DAG_RETRIES', '2')),
    'retry_delay': timedelta(minutes=int(os.getenv('DAG_RETRY_DELAY_MINUTES', '5'))),
}

# DAG definition
dag = DAG(
    'customer_churn_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline for customer churn analytics',
    schedule_interval=timedelta(hours=int(os.getenv('DAG_SCHEDULE_HOURS', '1'))),
    catchup=False,
    max_active_runs=1,
    tags=['elt', 'customer_churn', 'analytics'],
)


def extract_and_load_task(**context):
    """
    Airflow task wrapper for Extract and Load phase.
    
    Why wrapper functions?
    - Airflow requires callable functions for PythonOperator
    - Allows passing Airflow context (execution_date, task_instance, etc.)
    - Enables XCom for inter-task communication
    """
    logger.info("Starting Extract and Load task")
    
    elt = get_elt_processor()
    row_count = elt.extract_and_load()
    
    context['task_instance'].xcom_push(key='staging_row_count', value=row_count)
    logger.info(f"Extract and Load completed: {row_count} rows")
    return row_count


def transform_task(**context):
    """
    Airflow task wrapper for Transform phase.
    
    Pulls staging row count from upstream task for validation.
    """
    logger.info("Starting Transform task")
    
    staging_rows = context['task_instance'].xcom_pull(
        task_ids='extract_and_load_to_staging',
        key='staging_row_count'
    )
    logger.info(f"Processing {staging_rows} rows from staging")
    
    elt = get_elt_processor()
    row_count = elt.transform()
    
    if row_count != staging_rows:
        logger.warning(f"Row count mismatch: staging={staging_rows}, production={row_count}")
    
    context['task_instance'].xcom_push(key='production_row_count', value=row_count)
    logger.info(f"Transform completed: {row_count} rows")
    return row_count


def generate_metrics_task(**context):
    """
    Airflow task wrapper for metrics generation.
    
    Generates data quality and business metrics for monitoring.
    """
    logger.info("Starting Metrics Generation task")
    
    elt = get_elt_processor()
    metrics = elt.generate_data_quality_metrics()
    
    logger.info(f"Metrics: {metrics}")
    context['task_instance'].xcom_push(key='metrics', value=metrics)
    
    return metrics


# Define Airflow tasks
# Why PythonOperator? Executes Python functions with full Airflow context

extract_load_task = PythonOperator(
    task_id='extract_and_load_to_staging',
    python_callable=extract_and_load_task,
    dag=dag,
    # Provide context to access execution_date, task_instance, etc.
    provide_context=True,
)

transform_load_task = PythonOperator(
    task_id='transform_and_load_to_production',
    python_callable=transform_task,
    dag=dag,
    provide_context=True,
)

data_quality_task = PythonOperator(
    task_id='generate_data_quality_report',
    python_callable=generate_metrics_task,
    dag=dag,
    provide_context=True,
)

# Define task dependencies (DAG structure)
# Why this order?
# 1. Extract and Load: Get raw data into staging
# 2. Transform: Process and load to production
# 3. Metrics: Generate quality metrics and KPIs
# This ensures data lineage and proper error handling
extract_load_task >> transform_load_task >> data_quality_task