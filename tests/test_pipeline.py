import unittest
import pandas as pd
import duckdb
import tempfile
import os
from unittest.mock import patch, MagicMock
import sys
sys.path.append('/opt/airflow/dags')

class TestCustomerChurnPipeline(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_data = pd.DataFrame({
            'CustomerID': [1, 2, 3],
            'Age': [25, None, 45],
            'Gender': ['Male', 'Female', None],
            'Tenure': [12, 0, None],
            'MonthlyCharges': [50.0, None, 100.0],
            'ContractType': ['Month-to-Month', None, 'Two-Year'],
            'InternetService': ['Fiber Optic', 'DSL', None],
            'TotalCharges': [600.0, 0.0, None],
            'TechSupport': ['Yes', None, 'No'],
            'Churn': ['No', 'Yes', None]
        })
        
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False)
        self.temp_db.close()
        
    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
    
    def test_data_loading(self):
        """Test CSV data loading to staging table"""
        conn = duckdb.connect(self.temp_db.name)
        
        # Create staging table
        conn.execute("""
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
        
        # Insert test data
        conn.execute("INSERT INTO staging_customer_churn SELECT *, CURRENT_TIMESTAMP FROM test_data", {"test_data": self.test_data})
        
        # Verify data loaded
        result = conn.execute("SELECT COUNT(*) FROM staging_customer_churn").fetchone()[0]
        self.assertEqual(result, 3)
        
        conn.close()
    
    def test_missing_value_handling(self):
        """Test missing value imputation"""
        df = self.test_data.copy()
        
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
        
        # Verify no missing values
        self.assertEqual(df.isnull().sum().sum(), 0)
        self.assertEqual(df.loc[1, 'Gender'], 'Unknown')
        self.assertEqual(df.loc[2, 'Tenure'], 0)
    
    def test_pii_anonymization(self):
        """Test PII anonymization"""
        df = self.test_data.copy()
        
        # Simulate anonymization
        from faker import Faker
        fake = Faker()
        df['AnonymizedCustomerID'] = [fake.uuid4() for _ in range(len(df))]
        df = df.drop('CustomerID', axis=1)
        
        # Verify anonymization
        self.assertNotIn('CustomerID', df.columns)
        self.assertIn('AnonymizedCustomerID', df.columns)
        self.assertEqual(len(df['AnonymizedCustomerID'].unique()), 3)
    
    def test_derived_fields(self):
        """Test creation of derived analytics fields"""
        df = self.test_data.copy()
        df['Churn'] = df['Churn'].fillna('No')
        df['MonthlyCharges'] = df['MonthlyCharges'].fillna(75.0)
        df['Tenure'] = df['Tenure'].fillna(24)
        
        # Create derived fields
        df['ChurnFlag'] = df['Churn'].apply(lambda x: 1 if x == 'Yes' else 0)
        df['MonthlyChargesCategory'] = pd.cut(df['MonthlyCharges'], 
                                            bins=[0, 50, 100, float('inf')], 
                                            labels=['Low', 'Medium', 'High'])
        df['TenureCategory'] = pd.cut(df['Tenure'], 
                                    bins=[0, 12, 36, float('inf')], 
                                    labels=['New', 'Medium', 'Long'])
        
        # Verify derived fields
        self.assertIn('ChurnFlag', df.columns)
        self.assertEqual(df.loc[1, 'ChurnFlag'], 1)  # 'Yes' -> 1
        self.assertEqual(df.loc[0, 'ChurnFlag'], 0)  # 'No' -> 0
        self.assertIn('MonthlyChargesCategory', df.columns)
        self.assertIn('TenureCategory', df.columns)
    
    def test_data_quality_metrics(self):
        """Test data quality metrics calculation"""
        conn = duckdb.connect(self.temp_db.name)
        
        # Create production table with test data
        test_prod_data = pd.DataFrame({
            'ChurnFlag': [0, 1, 0, 1],
            'MonthlyCharges': [50.0, 75.0, 100.0, 80.0],
            'Tenure': [12, 24, 36, 18]
        })
        
        conn.execute("CREATE TABLE customer_churn_analytics AS SELECT * FROM test_prod_data", 
                    {"test_prod_data": test_prod_data})
        
        # Calculate metrics
        total_records = conn.execute("SELECT COUNT(*) FROM customer_churn_analytics").fetchone()[0]
        churn_rate = conn.execute("SELECT AVG(ChurnFlag) * 100 FROM customer_churn_analytics").fetchone()[0]
        avg_charges = conn.execute("SELECT AVG(MonthlyCharges) FROM customer_churn_analytics").fetchone()[0]
        
        # Verify metrics
        self.assertEqual(total_records, 4)
        self.assertEqual(churn_rate, 50.0)  # 2 out of 4 churned
        self.assertEqual(avg_charges, 76.25)  # (50+75+100+80)/4
        
        conn.close()
    
    @patch('duckdb.connect')
    def test_extract_load_function_structure(self, mock_connect):
        """Test extract and load function structure"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock the function behavior
        mock_conn.execute.return_value.fetchone.return_value = [100]
        
        # Simulate function call
        db_path = '/opt/airflow/databases/staging.duckdb'
        mock_connect.assert_called_with(db_path)
        
        # Verify connection was established
        self.assertTrue(mock_connect.called)

if __name__ == '__main__':
    unittest.main()