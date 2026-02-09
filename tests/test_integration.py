import unittest
import tempfile
import os
import pandas as pd
import duckdb
from datetime import datetime

class TestPipelineIntegration(unittest.TestCase):
    
    def setUp(self):
        """Set up integration test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.staging_db = os.path.join(self.temp_dir, 'staging.duckdb')
        self.prod_db = os.path.join(self.temp_dir, 'production.duckdb')
        
        # Create test CSV data
        self.test_csv = os.path.join(self.temp_dir, 'test_data.csv')
        test_data = pd.DataFrame({
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
        test_data.to_csv(self.test_csv, index=False)
    
    def tearDown(self):
        """Clean up test environment"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_end_to_end_pipeline(self):
        """Test complete pipeline flow"""
        # Stage 1: Extract and Load to Staging
        staging_conn = duckdb.connect(self.staging_db)
        df = pd.read_csv(self.test_csv)
        
        staging_conn.execute("""
            CREATE TABLE staging_customer_churn (
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
        
        staging_conn.execute("INSERT INTO staging_customer_churn SELECT *, CURRENT_TIMESTAMP FROM df")
        staging_count = staging_conn.execute("SELECT COUNT(*) FROM staging_customer_churn").fetchone()[0]
        self.assertEqual(staging_count, 5)
        
        # Stage 2: Transform and Load to Production
        df = staging_conn.execute("SELECT * FROM staging_customer_churn").df()
        
        # Apply transformations
        df['Age'] = df['Age'].fillna(df['Age'].median())
        df['Gender'] = df['Gender'].fillna('Unknown')
        df['Tenure'] = df['Tenure'].fillna(0)
        df['MonthlyCharges'] = df['MonthlyCharges'].fillna(df['MonthlyCharges'].mean())
        df['TotalCharges'] = df['TotalCharges'].fillna(0.0)
        df['ContractType'] = df['ContractType'].fillna('Month-to-Month')
        df['InternetService'] = df['InternetService'].fillna('None')
        df['TechSupport'] = df['TechSupport'].fillna('No')
        df['Churn'] = df['Churn'].fillna('No')
        
        # Anonymize PII
        from faker import Faker
        fake = Faker()
        df['AnonymizedCustomerID'] = [fake.uuid4() for _ in range(len(df))]
        df = df.drop('CustomerID', axis=1)
        
        # Create derived fields
        df['ChurnFlag'] = df['Churn'].apply(lambda x: 1 if x == 'Yes' else 0)
        df['MonthlyChargesCategory'] = pd.cut(df['MonthlyCharges'], 
                                            bins=[0, 50, 100, float('inf')], 
                                            labels=['Low', 'Medium', 'High'])
        df['TenureCategory'] = pd.cut(df['Tenure'], 
                                    bins=[0, 12, 36, float('inf')], 
                                    labels=['New', 'Medium', 'Long'])
        
        # Load to production
        prod_conn = duckdb.connect(self.prod_db)
        prod_conn.execute("""
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
                processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        prod_conn.execute("INSERT INTO customer_churn_analytics SELECT * FROM df")
        prod_count = prod_conn.execute("SELECT COUNT(*) FROM customer_churn_analytics").fetchone()[0]
        self.assertEqual(prod_count, 5)
        
        # Stage 3: Verify Data Quality
        churn_rate = prod_conn.execute("SELECT AVG(ChurnFlag) * 100 FROM customer_churn_analytics").fetchone()[0]
        self.assertEqual(churn_rate, 20.0)  # 1 out of 5 churned
        
        # Verify no missing values in critical fields
        null_count = prod_conn.execute("SELECT COUNT(*) FROM customer_churn_analytics WHERE Age IS NULL OR Gender IS NULL").fetchone()[0]
        self.assertEqual(null_count, 0)
        
        # Verify anonymization
        customer_ids = prod_conn.execute("SELECT AnonymizedCustomerID FROM customer_churn_analytics").fetchall()
        self.assertEqual(len(set([row[0] for row in customer_ids])), 5)  # All unique
        
        staging_conn.close()
        prod_conn.close()

if __name__ == '__main__':
    unittest.main()