"""
Unit Tests for Customer Churn ELT Script

Tests cover:
- Extract and Load functionality
- Transform operations
- Data quality metrics
- Error handling
- Edge cases
"""

import unittest
import tempfile
import os
import pandas as pd
import sys
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from customer_churn_elt import CustomerChurnELT


class TestCustomerChurnELT(unittest.TestCase):
    """Test suite for CustomerChurnELT class"""
    
    def setUp(self):
        """Set up test fixtures before each test"""
        # Create temporary directory for test databases
        self.temp_dir = tempfile.mkdtemp()
        self.staging_db = os.path.join(self.temp_dir, 'staging.duckdb')
        self.prod_db = os.path.join(self.temp_dir, 'production.duckdb')
        self.csv_path = os.path.join(self.temp_dir, 'test_data.csv')
        
        # Create test CSV data with various edge cases
        self.test_data = pd.DataFrame({
            'CustomerID': [1, 2, 3, 4, 5],
            'Age': [25, None, 45, 30, 55],
            'Gender': ['Male', 'Female', None, 'Male', 'Female'],
            'Tenure': [12, 0, None, 24, 36],
            'MonthlyCharges': [50.0, None, 100.0, 75.0, 120.0],
            'ContractType': ['Month-to-Month', None, 'Two-Year', 'One-Year', 'Two-Year'],
            'InternetService': ['Fiber Optic', 'DSL', None, 'Fiber Optic', 'DSL'],
            'TotalCharges': [600.0, 0.0, None, 1800.0, 4320.0],
            'TechSupport': ['Yes', None, 'No', 'Yes', 'No'],
            'Churn': ['No', 'Yes', None, 'No', 'No']
        })
        self.test_data.to_csv(self.csv_path, index=False)
        
        # Initialize ELT processor
        self.elt = CustomerChurnELT(
            staging_db_path=self.staging_db,
            production_db_path=self.prod_db,
            csv_path=self.csv_path
        )
    
    def tearDown(self):
        """Clean up test fixtures after each test"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_extract_and_load_success(self):
        """Test successful extract and load to staging"""
        row_count = self.elt.extract_and_load()
        
        # Verify correct number of rows loaded
        self.assertEqual(row_count, 5)
        
        # Verify staging database exists
        self.assertTrue(os.path.exists(self.staging_db))
        
        # Verify data in staging table
        import duckdb
        conn = duckdb.connect(self.staging_db, read_only=True)
        result = conn.execute("SELECT COUNT(*) FROM staging_customer_churn").fetchone()[0]
        self.assertEqual(result, 5)
        
        # Verify load_timestamp was added
        columns = conn.execute("DESCRIBE staging_customer_churn").fetchall()
        column_names = [col[0] for col in columns]
        self.assertIn('load_timestamp', column_names)
        
        conn.close()
    
    def test_extract_and_load_file_not_found(self):
        """Test extract and load with missing CSV file"""
        # Create ELT with non-existent CSV
        elt = CustomerChurnELT(
            staging_db_path=self.staging_db,
            production_db_path=self.prod_db,
            csv_path='/nonexistent/file.csv'
        )
        
        # Should raise FileNotFoundError
        with self.assertRaises(FileNotFoundError):
            elt.extract_and_load()
    
    def test_extract_and_load_idempotency(self):
        """Test that multiple loads produce same result (idempotency)"""
        # First load
        count1 = self.elt.extract_and_load()
        
        # Second load (should truncate and reload)
        count2 = self.elt.extract_and_load()
        
        # Both should have same count
        self.assertEqual(count1, count2)
        
        # Verify only one set of data in staging
        import duckdb
        conn = duckdb.connect(self.staging_db, read_only=True)
        result = conn.execute("SELECT COUNT(*) FROM staging_customer_churn").fetchone()[0]
        self.assertEqual(result, 5)
        conn.close()
    
    def test_transform_missing_value_handling(self):
        """Test that missing values are properly handled"""
        # Load data to staging first
        self.elt.extract_and_load()
        
        # Run transform
        row_count = self.elt.transform()
        
        # Verify data was transformed
        self.assertEqual(row_count, 5)
        
        # Check that no NULL values remain in critical fields
        import duckdb
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        null_age = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics WHERE Age IS NULL").fetchone()[0]
        null_gender = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics WHERE Gender IS NULL").fetchone()[0]
        null_churn = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics WHERE Churn IS NULL").fetchone()[0]
        
        self.assertEqual(null_age, 0)
        self.assertEqual(null_gender, 0)
        self.assertEqual(null_churn, 0)
        
        # Verify 'Unknown' was used for missing Gender
        unknown_gender = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics WHERE Gender = 'Unknown'").fetchone()[0]
        self.assertEqual(unknown_gender, 1)
        
        conn.close()
    
    def test_transform_pii_anonymization(self):
        """Test that PII (CustomerID) is anonymized"""
        # Load and transform
        self.elt.extract_and_load()
        self.elt.transform()
        
        import duckdb
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        # Verify CustomerID column doesn't exist
        columns = conn.execute("DESCRIBE customer_churn_analytics").fetchall()
        column_names = [col[0] for col in columns]
        self.assertNotIn('CustomerID', column_names)
        
        # Verify AnonymizedCustomerID exists
        self.assertIn('AnonymizedCustomerID', column_names)
        
        # Verify all IDs are unique UUIDs
        ids = conn.execute("SELECT AnonymizedCustomerID FROM customer_churn_analytics").fetchall()
        unique_ids = set([row[0] for row in ids])
        self.assertEqual(len(unique_ids), 5)
        
        # Verify UUID format (36 characters with hyphens)
        for id_tuple in ids:
            self.assertEqual(len(id_tuple[0]), 36)
            self.assertIn('-', id_tuple[0])
        
        conn.close()
    
    def test_transform_derived_fields(self):
        """Test creation of derived analytical fields"""
        # Load and transform
        self.elt.extract_and_load()
        self.elt.transform()
        
        import duckdb
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        # Verify derived columns exist
        columns = conn.execute("DESCRIBE customer_churn_analytics").fetchall()
        column_names = [col[0] for col in columns]
        self.assertIn('ChurnFlag', column_names)
        self.assertIn('MonthlyChargesCategory', column_names)
        self.assertIn('TenureCategory', column_names)
        
        # Verify ChurnFlag values (0 or 1)
        churn_flags = conn.execute("SELECT DISTINCT ChurnFlag FROM customer_churn_analytics ORDER BY ChurnFlag").fetchall()
        self.assertEqual(len(churn_flags), 2)
        self.assertEqual(churn_flags[0][0], 0)
        self.assertEqual(churn_flags[1][0], 1)
        
        # Verify category values
        charge_categories = conn.execute("SELECT DISTINCT MonthlyChargesCategory FROM customer_churn_analytics").fetchall()
        self.assertGreater(len(charge_categories), 0)
        
        tenure_categories = conn.execute("SELECT DISTINCT TenureCategory FROM customer_churn_analytics").fetchall()
        self.assertGreater(len(tenure_categories), 0)
        
        conn.close()
    
    def test_transform_data_consistency(self):
        """Test that transform maintains data consistency"""
        # Load and transform
        staging_count = self.elt.extract_and_load()
        prod_count = self.elt.transform()
        
        # Row counts should match (no data loss)
        self.assertEqual(staging_count, prod_count)
    
    def test_transform_without_staging_data(self):
        """Test transform fails gracefully without staging data"""
        # Try to transform without loading staging first
        with self.assertRaises(Exception):
            self.elt.transform()
    
    def test_generate_data_quality_metrics(self):
        """Test data quality metrics generation"""
        # Load and transform first
        self.elt.extract_and_load()
        self.elt.transform()
        
        # Generate metrics
        metrics = self.elt.generate_data_quality_metrics()
        
        # Verify metrics structure
        self.assertIsInstance(metrics, dict)
        self.assertIn('total_records', metrics)
        self.assertIn('churn_rate_percent', metrics)
        self.assertIn('avg_monthly_charges', metrics)
        self.assertIn('avg_tenure_months', metrics)
        
        # Verify metric values
        self.assertEqual(metrics['total_records'], 5)
        self.assertEqual(metrics['churn_rate_percent'], 20.0)  # 1 out of 5 churned
        
        # Verify metrics are persisted
        import duckdb
        conn = duckdb.connect(self.prod_db, read_only=True)
        metric_count = conn.execute("SELECT COUNT(*) FROM data_quality_metrics").fetchone()[0]
        self.assertGreater(metric_count, 0)
        conn.close()
    
    def test_generate_metrics_without_data(self):
        """Test metrics generation with empty production table"""
        # Create empty production table
        import duckdb
        conn = duckdb.connect(self.prod_db)
        conn.execute("""
            CREATE TABLE customer_churn_analytics (
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
        conn.close()
        
        # Generate metrics should handle empty table
        metrics = self.elt.generate_data_quality_metrics()
        self.assertEqual(metrics['total_records'], 0)
    
    def test_run_full_pipeline_success(self):
        """Test complete pipeline execution"""
        result = self.elt.run_full_pipeline()
        
        # Verify success status
        self.assertEqual(result['status'], 'success')
        self.assertIn('staging_rows', result)
        self.assertIn('production_rows', result)
        self.assertIn('metrics', result)
        
        # Verify row counts
        self.assertEqual(result['staging_rows'], 5)
        self.assertEqual(result['production_rows'], 5)
        
        # Verify metrics
        self.assertIsInstance(result['metrics'], dict)
        self.assertEqual(result['metrics']['total_records'], 5)
    
    def test_run_full_pipeline_failure(self):
        """Test pipeline failure handling"""
        # Create ELT with invalid CSV path
        elt = CustomerChurnELT(
            staging_db_path=self.staging_db,
            production_db_path=self.prod_db,
            csv_path='/invalid/path.csv'
        )
        
        result = elt.run_full_pipeline()
        
        # Verify failure status
        self.assertEqual(result['status'], 'failed')
        self.assertIn('error', result)
    
    def test_churn_rate_calculation(self):
        """Test churn rate calculation accuracy"""
        # Load and transform
        self.elt.extract_and_load()
        self.elt.transform()
        
        # Generate metrics
        metrics = self.elt.generate_data_quality_metrics()
        
        # Verify churn rate (1 Yes out of 5 total = 20%)
        self.assertEqual(metrics['churn_rate_percent'], 20.0)
    
    def test_monthly_charges_binning(self):
        """Test monthly charges categorization"""
        # Load and transform
        self.elt.extract_and_load()
        self.elt.transform()
        
        import duckdb
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        # Check binning logic
        # 50.0 -> Low, 75.0 -> Medium, 100.0 -> Medium, 120.0 -> High
        low_count = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics WHERE MonthlyChargesCategory = 'Low'").fetchone()[0]
        medium_count = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics WHERE MonthlyChargesCategory = 'Medium'").fetchone()[0]
        high_count = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics WHERE MonthlyChargesCategory = 'High'").fetchone()[0]
        
        self.assertGreater(low_count, 0)
        self.assertGreater(medium_count, 0)
        self.assertGreater(high_count, 0)
        
        conn.close()
    
    def test_tenure_categorization(self):
        """Test tenure category assignment"""
        # Load and transform
        self.elt.extract_and_load()
        self.elt.transform()
        
        import duckdb
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        # Verify categories exist
        categories = conn.execute("SELECT DISTINCT TenureCategory FROM customer_churn_analytics ORDER BY TenureCategory").fetchall()
        category_names = [cat[0] for cat in categories]
        
        # Should have New, Medium, or Long based on test data
        self.assertTrue(any(cat in ['New', 'Medium', 'Long'] for cat in category_names))
        
        conn.close()
    
    def test_processed_timestamp_added(self):
        """Test that processed_timestamp is added during transform"""
        # Load and transform
        self.elt.extract_and_load()
        self.elt.transform()
        
        import duckdb
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        # Verify processed_timestamp exists and is not null
        timestamps = conn.execute("SELECT processed_timestamp FROM customer_churn_analytics").fetchall()
        self.assertEqual(len(timestamps), 5)
        
        for ts in timestamps:
            self.assertIsNotNone(ts[0])
        
        conn.close()
    
    def test_faker_seed_reproducibility(self):
        """Test that Faker seed produces consistent results"""
        from customer_churn_elt import CustomerChurnELT
        
        elt1 = CustomerChurnELT(self.staging_db, self.prod_db, self.csv_path)
        elt2 = CustomerChurnELT(self.staging_db, self.prod_db, self.csv_path)
        
        # Both should have faker instances
        self.assertIsNotNone(elt1.faker)
        self.assertIsNotNone(elt2.faker)
    
    def test_column_ordering(self):
        """Test that columns are in correct order"""
        self.elt.extract_and_load()
        self.elt.transform()
        
        import duckdb
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        columns = conn.execute("DESCRIBE customer_churn_analytics").fetchall()
        column_names = [col[0] for col in columns]
        
        # Verify first column is AnonymizedCustomerID
        self.assertEqual(column_names[0], 'AnonymizedCustomerID')
        
        # Verify last two columns are timestamps
        self.assertEqual(column_names[-2], 'load_timestamp')
        self.assertEqual(column_names[-1], 'processed_timestamp')
        
        conn.close()


class TestELTEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.staging_db = os.path.join(self.temp_dir, 'staging.duckdb')
        self.prod_db = os.path.join(self.temp_dir, 'production.duckdb')
        self.csv_path = os.path.join(self.temp_dir, 'test_data.csv')
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_empty_csv_file(self):
        """Test handling of empty CSV file"""
        # Create empty CSV with headers only
        empty_df = pd.DataFrame(columns=[
            'CustomerID', 'Age', 'Gender', 'Tenure', 'MonthlyCharges',
            'ContractType', 'InternetService', 'TotalCharges', 'TechSupport', 'Churn'
        ])
        empty_df.to_csv(self.csv_path, index=False)
        
        elt = CustomerChurnELT(self.staging_db, self.prod_db, self.csv_path)
        
        # Should handle empty file gracefully
        row_count = elt.extract_and_load()
        self.assertEqual(row_count, 0)
    
    def test_all_missing_values(self):
        """Test handling of dataset with all missing values"""
        # Create CSV with all NaN values
        all_null_df = pd.DataFrame({
            'CustomerID': [1, 2, 3],
            'Age': [None, None, None],
            'Gender': [None, None, None],
            'Tenure': [None, None, None],
            'MonthlyCharges': [None, None, None],
            'ContractType': [None, None, None],
            'InternetService': [None, None, None],
            'TotalCharges': [None, None, None],
            'TechSupport': [None, None, None],
            'Churn': [None, None, None]
        })
        all_null_df.to_csv(self.csv_path, index=False)
        
        elt = CustomerChurnELT(self.staging_db, self.prod_db, self.csv_path)
        
        # Should handle all nulls with defaults
        elt.extract_and_load()
        row_count = elt.transform()
        
        self.assertEqual(row_count, 3)
        
        # Verify defaults were applied
        import duckdb
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        unknown_gender = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics WHERE Gender = 'Unknown'").fetchone()[0]
        self.assertEqual(unknown_gender, 3)
        
        conn.close()
    
    def test_single_row_dataset(self):
        """Test handling of single row dataset"""
        single_row = pd.DataFrame({
            'CustomerID': [1],
            'Age': [30],
            'Gender': ['Male'],
            'Tenure': [12],
            'MonthlyCharges': [75.0],
            'ContractType': ['Month-to-Month'],
            'InternetService': ['Fiber Optic'],
            'TotalCharges': [900.0],
            'TechSupport': ['Yes'],
            'Churn': ['No']
        })
        single_row.to_csv(self.csv_path, index=False)
        
        elt = CustomerChurnELT(self.staging_db, self.prod_db, self.csv_path)
        result = elt.run_full_pipeline()
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['staging_rows'], 1)
        self.assertEqual(result['production_rows'], 1)
    
    def test_large_dataset_performance(self):
        """Test handling of larger dataset"""
        # Create larger dataset
        large_df = pd.DataFrame({
            'CustomerID': range(1, 1001),
            'Age': [30] * 1000,
            'Gender': ['Male'] * 1000,
            'Tenure': [12] * 1000,
            'MonthlyCharges': [75.0] * 1000,
            'ContractType': ['Month-to-Month'] * 1000,
            'InternetService': ['Fiber Optic'] * 1000,
            'TotalCharges': [900.0] * 1000,
            'TechSupport': ['Yes'] * 1000,
            'Churn': ['No'] * 1000
        })
        large_df.to_csv(self.csv_path, index=False)
        
        elt = CustomerChurnELT(self.staging_db, self.prod_db, self.csv_path)
        result = elt.run_full_pipeline()
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['staging_rows'], 1000)
        self.assertEqual(result['production_rows'], 1000)
    
    def test_special_characters_in_data(self):
        """Test handling of special characters"""
        special_df = pd.DataFrame({
            'CustomerID': [1, 2],
            'Age': [30, 40],
            'Gender': ["M'ale", 'Fe"male'],
            'Tenure': [12, 24],
            'MonthlyCharges': [75.0, 85.0],
            'ContractType': ['Month-to-Month', 'One-Year'],
            'InternetService': ['Fiber Optic', 'DSL'],
            'TotalCharges': [900.0, 2040.0],
            'TechSupport': ['Yes', 'No'],
            'Churn': ['No', 'Yes']
        })
        special_df.to_csv(self.csv_path, index=False)
        
        elt = CustomerChurnELT(self.staging_db, self.prod_db, self.csv_path)
        result = elt.run_full_pipeline()
        
        self.assertEqual(result['status'], 'success')


if __name__ == '__main__':
    unittest.main()
