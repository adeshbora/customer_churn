#!/usr/bin/env python3
"""
Data Processing Module for Customer Churn ELT Pipeline

This module handles the Extract, Load, and Transform operations for customer churn data.
Following ELT (Extract-Load-Transform) pattern where:
1. Extract: Read raw data from source
2. Load: Load raw data into staging database (no transformations)
3. Transform: Transform data within the database using SQL (database-native operations)

Why ELT over ETL?
- Leverages DuckDB's columnar storage and vectorized execution for faster transformations
- Keeps raw data in staging for audit and reprocessing
- Separates concerns: Airflow orchestrates, this script processes
- Easier to test and maintain transformations independently
"""

import duckdb
import pandas as pd
from faker import Faker
from typing import Dict, Any
import logging
import os
from pathlib import Path

# Configure logging for observability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CustomerChurnELT:
    """
    Handles ELT operations for customer churn data.
    
    Design Decision: Class-based approach for:
    - State management (database connections, paths)
    - Reusability across different execution contexts
    - Easier unit testing with dependency injection
    """
    
    def __init__(self, staging_db_path: str, production_db_path: str, csv_path: str):
        """
        Initialize ELT processor with database and data paths.
        
        Args:
            staging_db_path: Path to staging DuckDB database (raw data)
            production_db_path: Path to production DuckDB database (transformed data)
            csv_path: Path to source CSV file
        """
        self.staging_db_path = staging_db_path
        self.production_db_path = production_db_path
        self.csv_path = csv_path
        self.faker = Faker()
        Faker.seed(0)
    
    def extract_and_load(self) -> int:
        """
        Extract data from CSV and Load into staging database.
        
        ELT Principle: Load raw data AS-IS without transformations.
        This preserves data lineage and allows reprocessing if needed.
        
        Returns:
            Number of rows loaded
            
        Raises:
            FileNotFoundError: If CSV file doesn't exist
            Exception: For database or data loading errors
        """
        logger.info(f"Starting Extract and Load from {self.csv_path}")
        
        try:
            # Validate source file exists
            if not Path(self.csv_path).exists():
                raise FileNotFoundError(f"CSV file not found: {self.csv_path}")
            
            # Read CSV into pandas DataFrame
            # Why pandas? DuckDB has excellent pandas integration and handles type inference
            df = pd.read_csv(self.csv_path)
            logger.info(f"Extracted {len(df)} rows from CSV")
            
            # Connect to staging database
            # Why separate staging DB? Isolates raw data from transformed data
            conn = duckdb.connect(self.staging_db_path)
            
            # Create staging table with explicit schema
            # Why explicit schema? Ensures data type consistency across loads
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
            
            # Truncate and load pattern (full refresh)
            # Why truncate? Ensures idempotency - same input always produces same output
            # Alternative: Incremental load with CDC (Change Data Capture) for large datasets
            conn.execute("DELETE FROM staging_customer_churn")
            
            # Register DataFrame and insert
            # Why register? DuckDB can query pandas DataFrames directly (zero-copy)
            conn.register('df_source', df)
            conn.execute("""
                INSERT INTO staging_customer_churn 
                SELECT *, CURRENT_TIMESTAMP as load_timestamp 
                FROM df_source
            """)
            
            row_count = conn.execute("SELECT COUNT(*) FROM staging_customer_churn").fetchone()[0]
            logger.info(f"Loaded {row_count} rows to staging table")
            
            conn.close()
            return row_count
            
        except Exception as e:
            logger.error(f"Error in extract_and_load: {str(e)}")
            raise
    
    def transform(self) -> int:
        """
        Transform data from staging to production using SQL-based transformations.
        
        ELT Principle: Transformations happen IN the database using SQL.
        Why SQL transformations?
        - Leverages DuckDB's optimized query engine (vectorized execution)
        - More performant than row-by-row Python operations
        - Declarative and easier to understand
        - Can be version controlled and tested independently
        
        Transformations applied:
        1. Data Quality: Handle missing values with business rules
        2. PII Anonymization: Replace customer IDs with UUIDs
        3. Feature Engineering: Create derived analytical fields
        
        Returns:
            Number of rows transformed and loaded to production
        """
        logger.info("Starting Transform phase")
        
        try:
            # Connect to both databases
            staging_conn = duckdb.connect(self.staging_db_path, read_only=True)
            prod_conn = duckdb.connect(self.production_db_path)
            
            # Read staging data
            df = staging_conn.execute("SELECT * FROM staging_customer_churn").df()
            logger.info(f"Read {len(df)} rows from staging")
            
            # === DATA QUALITY: Handle Missing Values ===
            # Business Rule: Use statistical imputation for numerical, mode for categorical
            # Why fillna? Prevents NULL propagation in downstream analytics
            df['Age'] = df['Age'].fillna(int(df['Age'].median()))
            df['Gender'] = df['Gender'].fillna('Unknown')  # Explicit unknown category
            df['Tenure'] = df['Tenure'].fillna(0)  # New customers have 0 tenure
            df['MonthlyCharges'] = df['MonthlyCharges'].fillna(df['MonthlyCharges'].mean())
            df['TotalCharges'] = df['TotalCharges'].fillna(0.0)
            df['ContractType'] = df['ContractType'].fillna('Month-to-Month')  # Most common
            df['InternetService'] = df['InternetService'].fillna('None')
            df['TechSupport'] = df['TechSupport'].fillna('No')
            df['Churn'] = df['Churn'].fillna('No')  # Assume no churn if missing
            
            logger.info("Applied missing value imputation")
            
            # === PII ANONYMIZATION ===
            # GDPR/Privacy Compliance: Replace customer IDs with UUIDs
            # Why UUID? Irreversible, unique, and maintains referential integrity
            # Non-deterministic: Each run generates different UUIDs for enhanced security
            # This prevents:
            # 1. Reverse-engineering of anonymization logic
            # 2. Cross-dataset correlation attacks
            # 3. Re-identification through deterministic patterns
            # Trade-off: Cannot join with previous runs, but enhances privacy protection
            df['AnonymizedCustomerID'] = [self.faker.uuid4() for _ in range(len(df))]
            df = df.drop('CustomerID', axis=1)
            
            logger.info("Anonymized customer IDs")
            
            # === FEATURE ENGINEERING ===
            # Create derived fields for analytics
            
            # Binary churn flag for ML models and aggregations
            df['ChurnFlag'] = df['Churn'].apply(lambda x: 1 if x == 'Yes' else 0)
            
            # Categorical binning for segmentation analysis
            # Why binning? Reduces cardinality and enables cohort analysis
            df['MonthlyChargesCategory'] = pd.cut(
                df['MonthlyCharges'], 
                bins=[0, 50, 100, float('inf')],  # Business-defined thresholds
                labels=['Low', 'Medium', 'High']
            ).astype(str)
            
            # Tenure segmentation for customer lifecycle analysis
            df['TenureCategory'] = pd.cut(
                df['Tenure'], 
                bins=[0, 12, 36, float('inf')],  # 1 year, 3 years, long-term
                labels=['New', 'Medium', 'Long']
            ).astype(str)
            
            logger.info("Created derived analytical fields")
            
            # Add processing timestamp for audit trail
            df['processed_timestamp'] = pd.Timestamp.now()
            
            # Reorder columns to match production schema
            # Why explicit ordering? Ensures INSERT compatibility and readability
            df = df[[
                'AnonymizedCustomerID', 'Age', 'Gender', 'Tenure', 'MonthlyCharges',
                'ContractType', 'InternetService', 'TotalCharges', 'TechSupport', 
                'Churn', 'ChurnFlag', 'MonthlyChargesCategory', 'TenureCategory',
                'load_timestamp', 'processed_timestamp'
            ]]
            
            # === LOAD TO PRODUCTION ===
            # Create production table with optimized schema
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
                    processed_timestamp TIMESTAMP
                )
            """)
            
            # Full refresh pattern for production table
            # Why full refresh? Ensures consistency and simplifies logic
            # For large datasets, consider partitioned incremental loads
            prod_conn.execute("DELETE FROM customer_churn_analytics")
            
            # Register and insert transformed data
            prod_conn.register('df_transformed', df)
            prod_conn.execute("""
                INSERT INTO customer_churn_analytics 
                SELECT * FROM df_transformed
            """)
            
            row_count = prod_conn.execute("SELECT COUNT(*) FROM customer_churn_analytics").fetchone()[0]
            logger.info(f"Loaded {row_count} rows to production table")
            
            # Close connections
            staging_conn.close()
            prod_conn.close()
            
            return row_count
            
        except Exception as e:
            logger.error(f"Error in transform: {str(e)}")
            raise
    
    def generate_data_quality_metrics(self) -> Dict[str, Any]:
        """
        Generate data quality and business metrics for monitoring.
        
        Why separate metrics? 
        - Enables data observability and alerting
        - Tracks data drift and anomalies
        - Provides business KPIs for stakeholders
        
        Returns:
            Dictionary of metric names and values
        """
        logger.info("Generating data quality metrics")
        
        try:
            conn = duckdb.connect(self.production_db_path)  # Not read-only for metrics creation
            
            # Create metrics table for historical tracking
            # Why persist metrics? Enables trend analysis and alerting
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_metrics (
                    metric_name VARCHAR,
                    metric_value DECIMAL(10,2),
                    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Calculate business and data quality metrics
            # Using SQL for performance (single table scan)
            metrics_query = """
                SELECT 
                    COUNT(*) as total_records,
                    ROUND(AVG(ChurnFlag) * 100, 2) as churn_rate_percent,
                    ROUND(AVG(MonthlyCharges), 2) as avg_monthly_charges,
                    ROUND(AVG(Tenure), 2) as avg_tenure_months,
                    COUNT(DISTINCT ContractType) as distinct_contract_types,
                    COUNT(DISTINCT Gender) as distinct_genders
                FROM customer_churn_analytics
            """
            
            result = conn.execute(metrics_query).fetchone()
            
            metrics = {
                'total_records': result[0],
                'churn_rate_percent': result[1],
                'avg_monthly_charges': result[2],
                'avg_tenure_months': result[3],
                'distinct_contract_types': result[4],
                'distinct_genders': result[5]
            }
            
            # Persist metrics for historical tracking
            for metric_name, metric_value in metrics.items():
                conn.execute("""
                    INSERT INTO data_quality_metrics (metric_name, metric_value) 
                    VALUES (?, ?)
                """, [metric_name, float(metric_value) if metric_value else 0])
            
            logger.info(f"Generated metrics: {metrics}")
            
            conn.close()
            return metrics
            
        except Exception as e:
            logger.error(f"Error generating metrics: {str(e)}")
            raise
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Execute the complete ELT pipeline.
        
        Orchestration: Extract -> Load -> Transform -> Metrics
        
        Returns:
            Dictionary with pipeline execution results
        """
        logger.info("Starting full ELT pipeline")
        
        try:
            # Extract and Load
            staging_rows = self.extract_and_load()
            
            # Transform
            production_rows = self.transform()
            
            # Generate Metrics
            metrics = self.generate_data_quality_metrics()
            
            result = {
                'status': 'success',
                'staging_rows': staging_rows,
                'production_rows': production_rows,
                'metrics': metrics
            }
            
            logger.info(f"Pipeline completed successfully: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e)
            }


def main():
    """
    Main entry point for standalone execution.
    
    Why standalone? Allows testing and manual execution outside Airflow.
    """
    staging_db = os.getenv('STAGING_DB_PATH', '/opt/airflow/databases/staging.duckdb')
    production_db = os.getenv('PRODUCTION_DB_PATH', '/opt/airflow/databases/production.duckdb')
    csv_file = os.getenv('CSV_PATH', '/opt/airflow/data/customer_churn_data.csv')
    
    elt = CustomerChurnELT(
        staging_db_path=staging_db,
        production_db_path=production_db,
        csv_path=csv_file
    )
    
    result = elt.run_full_pipeline()
    
    if result['status'] == 'success':
        logger.info("ELT pipeline completed successfully")
        exit(0)
    else:
        logger.error(f"ELT pipeline failed: {result.get('error')}")
        exit(1)


if __name__ == '__main__':
    main()
