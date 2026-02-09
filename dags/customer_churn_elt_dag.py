from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import duckdb
import pandas as pd
import os
from faker import Faker

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'customer_churn_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline for customer churn data',
    schedule_interval=timedelta(hours=1),  # Configurable hourly schedule
    catchup=False,
    tags=['elt', 'customer_churn', 'duckdb'],
)

def extract_and_load_to_staging(**context):
    """Extract CSV data and load to staging table in DuckDB"""
    
    # Initialize DuckDB connection
    db_path = '/opt/airflow/databases/staging.duckdb'
    conn = duckdb.connect(db_path)
    
    # Read CSV file
    csv_path = '/opt/airflow/data/customer_churn_data.csv'
    df = pd.read_csv(csv_path)
    
    # Create staging table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging_customer_churn (
            CustomerID INTEGER,
            Age INTEGER,
            Gender VARCHAR,
            Tenure INTEGER,
            MonthlyCharges DECIMAL(10,2),
            ContractType VARCHAR,
            InternetService VARCHAR,
            TotalCharges DECIMAL(10,2),
            TechSupport VARCHAR,
            Churn VARCHAR,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Clear existing data and insert new data
    conn.execute("DELETE FROM staging_customer_churn")
    conn.execute("INSERT INTO staging_customer_churn SELECT *, CURRENT_TIMESTAMP FROM df")
    
    row_count = conn.execute("SELECT COUNT(*) FROM staging_customer_churn").fetchone()[0]
    print(f"Loaded {row_count} rows to staging table")
    
    conn.close()

def transform_and_load_to_production(**context):
    """Transform data: handle missing values and anonymize PII"""
    
    fake = Faker()
    
    # Connect to staging database
    staging_db = '/opt/airflow/databases/staging.duckdb'
    staging_conn = duckdb.connect(staging_db)
    
    # Connect to production database
    prod_db = '/opt/airflow/databases/production.duckdb'
    prod_conn = duckdb.connect(prod_db)
    
    # Read from staging
    df = staging_conn.execute("SELECT * FROM staging_customer_churn").df()
    
    # Data transformations
    # 1. Handle missing values with defaults
    df['Age'] = df['Age'].fillna(df['Age'].median())
    df['Gender'] = df['Gender'].fillna('Unknown')
    df['Tenure'] = df['Tenure'].fillna(0)
    df['MonthlyCharges'] = df['MonthlyCharges'].fillna(df['MonthlyCharges'].mean())
    df['TotalCharges'] = df['TotalCharges'].fillna(0.0)
    df['ContractType'] = df['ContractType'].fillna('Month-to-Month')
    df['InternetService'] = df['InternetService'].fillna('None')
    df['TechSupport'] = df['TechSupport'].fillna('No')
    df['Churn'] = df['Churn'].fillna('No')
    
    # 2. Anonymize PII (CustomerID)
    df['AnonymizedCustomerID'] = [fake.uuid4() for _ in range(len(df))]
    df = df.drop('CustomerID', axis=1)
    
    # 3. Add derived fields for analytics
    df['ChurnFlag'] = df['Churn'].apply(lambda x: 1 if x == 'Yes' else 0)
    df['MonthlyChargesCategory'] = pd.cut(df['MonthlyCharges'], 
                                        bins=[0, 50, 100, float('inf')], 
                                        labels=['Low', 'Medium', 'High'])
    df['TenureCategory'] = pd.cut(df['Tenure'], 
                                bins=[0, 12, 36, float('inf')], 
                                labels=['New', 'Medium', 'Long'])
    
    # Create production table
    prod_conn.execute("""
        CREATE TABLE IF NOT EXISTS customer_churn_analytics (
            AnonymizedCustomerID VARCHAR,
            Age INTEGER,
            Gender VARCHAR,
            Tenure INTEGER,
            MonthlyCharges DECIMAL(10,2),
            ContractType VARCHAR,
            InternetService VARCHAR,
            TotalCharges DECIMAL(10,2),
            TechSupport VARCHAR,
            Churn VARCHAR,
            ChurnFlag INTEGER,
            MonthlyChargesCategory VARCHAR,
            TenureCategory VARCHAR,
            load_timestamp TIMESTAMP,
            processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Clear and insert transformed data
    prod_conn.execute("DELETE FROM customer_churn_analytics")
    prod_conn.execute("INSERT INTO customer_churn_analytics SELECT * FROM df")
    
    row_count = prod_conn.execute("SELECT COUNT(*) FROM customer_churn_analytics").fetchone()[0]
    print(f"Loaded {row_count} rows to production table")
    
    # Close connections
    staging_conn.close()
    prod_conn.close()

def generate_data_quality_report(**context):
    """Generate data quality metrics"""
    
    prod_db = '/opt/airflow/databases/production.duckdb'
    conn = duckdb.connect(prod_db)
    
    # Create metrics table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_metrics (
            metric_name VARCHAR,
            metric_value DECIMAL(10,2),
            run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Calculate metrics
    total_records = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics").fetchone()[0]
    churn_rate = conn.execute("SELECT AVG(ChurnFlag) * 100 FROM customer_churn_analytics").fetchone()[0]
    avg_monthly_charges = conn.execute("SELECT AVG(MonthlyCharges) FROM customer_churn_analytics").fetchone()[0]
    avg_tenure = conn.execute("SELECT AVG(Tenure) FROM customer_churn_analytics").fetchone()[0]
    
    # Insert metrics
    metrics = [
        ('total_records', total_records),
        ('churn_rate_percent', churn_rate),
        ('avg_monthly_charges', avg_monthly_charges),
        ('avg_tenure_months', avg_tenure)
    ]
    
    for metric_name, metric_value in metrics:
        conn.execute("""
            INSERT INTO data_quality_metrics (metric_name, metric_value) 
            VALUES (?, ?)
        """, [metric_name, metric_value])
    
    print(f"Generated metrics: Total Records: {total_records}, Churn Rate: {churn_rate:.2f}%")
    
    conn.close()

# Define tasks
extract_load_task = PythonOperator(
    task_id='extract_and_load_to_staging',
    python_callable=extract_and_load_to_staging,
    dag=dag,
)

transform_load_task = PythonOperator(
    task_id='transform_and_load_to_production',
    python_callable=transform_and_load_to_production,
    dag=dag,
)

data_quality_task = PythonOperator(
    task_id='generate_data_quality_report',
    python_callable=generate_data_quality_report,
    dag=dag,
)

# Set task dependencies
extract_load_task >> transform_load_task >> data_quality_task