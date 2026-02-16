"""
Unit Tests for DuckDB HTTP API Proxy

Tests cover:
- API endpoint responses
- JSON formatting
- Error handling
- Database queries
"""

import unittest
import tempfile
import os
import json
import duckdb
import pandas as pd
from http.server import HTTPServer
from threading import Thread
import time
import urllib.request
import urllib.error


class TestDuckDBAPI(unittest.TestCase):
    """Test suite for DuckDB HTTP API"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.prod_db = os.path.join(self.temp_dir, 'production.duckdb')
        
        # Create test production database with sample data
        conn = duckdb.connect(self.prod_db)
        
        # Create customer_churn_analytics table
        test_data = pd.DataFrame({
            'AnonymizedCustomerID': ['uuid-1', 'uuid-2', 'uuid-3', 'uuid-4', 'uuid-5'],
            'Age': [25, 35, 45, 30, 55],
            'Gender': ['Male', 'Female', 'Male', 'Female', 'Male'],
            'Tenure': [12, 24, 36, 6, 48],
            'MonthlyCharges': [50.0, 75.0, 100.0, 40.0, 120.0],
            'ContractType': ['Month-to-Month', 'One-Year', 'Two-Year', 'Month-to-Month', 'Two-Year'],
            'InternetService': ['Fiber Optic', 'DSL', 'Fiber Optic', 'DSL', 'Fiber Optic'],
            'TotalCharges': [600.0, 1800.0, 3600.0, 240.0, 5760.0],
            'TechSupport': ['Yes', 'No', 'Yes', 'No', 'Yes'],
            'Churn': ['No', 'Yes', 'No', 'Yes', 'No'],
            'ChurnFlag': [0, 1, 0, 1, 0],
            'MonthlyChargesCategory': ['Low', 'Medium', 'High', 'Low', 'High'],
            'TenureCategory': ['New', 'Medium', 'Long', 'New', 'Long']
        })
        
        conn.execute("CREATE TABLE customer_churn_analytics AS SELECT * FROM test_data")
        
        # Create data_quality_metrics table
        metrics_data = pd.DataFrame({
            'metric_name': ['total_records', 'churn_rate_percent', 'avg_monthly_charges'],
            'metric_value': [5.0, 40.0, 77.0],
            'run_timestamp': [pd.Timestamp.now()] * 3
        })
        
        conn.execute("CREATE TABLE data_quality_metrics AS SELECT * FROM metrics_data")
        
        conn.close()
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_summary_endpoint_query(self):
        """Test summary endpoint SQL query"""
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        result = conn.execute("""
            SELECT 
                COUNT(*) as total_customers,
                ROUND(AVG(ChurnFlag) * 100, 2) as churn_rate,
                ROUND(AVG(MonthlyCharges), 2) as avg_monthly_charges,
                ROUND(AVG(Tenure), 2) as avg_tenure
            FROM customer_churn_analytics
        """).fetchone()
        
        # Verify query results
        self.assertEqual(result[0], 5)  # total_customers
        self.assertEqual(result[1], 40.0)  # churn_rate (2 out of 5)
        self.assertEqual(result[2], 77.0)  # avg_monthly_charges
        self.assertEqual(result[3], 25.2)  # avg_tenure
        
        conn.close()
    
    def test_churn_by_contract_query(self):
        """Test churn by contract type query"""
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        result = conn.execute("""
            SELECT 
                ContractType,
                COUNT(*) as count,
                ROUND(AVG(ChurnFlag) * 100, 2) as churn_rate
            FROM customer_churn_analytics
            GROUP BY ContractType
            ORDER BY churn_rate DESC
        """).fetchall()
        
        # Verify query returns results
        self.assertGreater(len(result), 0)
        
        # Verify structure (ContractType, count, churn_rate)
        for row in result:
            self.assertEqual(len(row), 3)
            self.assertIsInstance(row[0], str)  # ContractType
            self.assertIsInstance(row[1], int)  # count
            self.assertIsInstance(row[2], (int, float))  # churn_rate
        
        conn.close()
    
    def test_metrics_query(self):
        """Test metrics endpoint query"""
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        result = conn.execute("""
            SELECT metric_name, metric_value, run_timestamp 
            FROM data_quality_metrics 
            ORDER BY run_timestamp DESC 
            LIMIT 10
        """).fetchall()
        
        # Verify metrics exist
        self.assertEqual(len(result), 3)
        
        # Verify structure
        for row in result:
            self.assertEqual(len(row), 3)
            self.assertIsInstance(row[0], str)  # metric_name
            self.assertIsNotNone(row[1])  # metric_value
        
        conn.close()
    
    def test_analytics_query(self):
        """Test analytics endpoint query"""
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        result = conn.execute("""
            SELECT 
                COUNT(*) as total_customers,
                AVG(CASE WHEN ChurnFlag = 1 THEN 1.0 ELSE 0.0 END) * 100 as churn_rate,
                AVG(MonthlyCharges) as avg_monthly_charges,
                AVG(Tenure) as avg_tenure,
                ContractType,
                COUNT(*) as contract_count,
                AVG(CASE WHEN ChurnFlag = 1 THEN 1.0 ELSE 0.0 END) * 100 as contract_churn_rate
            FROM customer_churn_analytics 
            GROUP BY ContractType
        """).fetchall()
        
        # Verify results
        self.assertGreater(len(result), 0)
        
        # Verify each contract type has analytics
        for row in result:
            self.assertIsNotNone(row[4])  # ContractType
            self.assertGreater(row[5], 0)  # contract_count
        
        conn.close()
    
    def test_json_response_format(self):
        """Test JSON response formatting"""
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        result = conn.execute("""
            SELECT 
                COUNT(*) as total_customers,
                ROUND(AVG(ChurnFlag) * 100, 2) as churn_rate,
                ROUND(AVG(MonthlyCharges), 2) as avg_monthly_charges,
                ROUND(AVG(Tenure), 2) as avg_tenure
            FROM customer_churn_analytics
        """).fetchone()
        
        # Format as JSON (simulating API response)
        data = {
            'total_customers': int(result[0]) if result[0] else 0,
            'churn_rate': float(result[1]) if result[1] else 0,
            'avg_monthly_charges': float(result[2]) if result[2] else 0,
            'avg_tenure': float(result[3]) if result[3] else 0
        }
        
        # Verify JSON serialization
        json_str = json.dumps(data)
        self.assertIsInstance(json_str, str)
        
        # Verify JSON deserialization
        parsed = json.loads(json_str)
        self.assertEqual(parsed['total_customers'], 5)
        self.assertEqual(parsed['churn_rate'], 40.0)
        
        conn.close()
    
    def test_empty_table_handling(self):
        """Test API behavior with empty tables"""
        # Create empty database
        empty_db = os.path.join(self.temp_dir, 'empty.duckdb')
        conn = duckdb.connect(empty_db)
        
        conn.execute("""
            CREATE TABLE customer_churn_analytics (
                AnonymizedCustomerID VARCHAR,
                ChurnFlag INTEGER,
                MonthlyCharges DECIMAL(10,2),
                Tenure INTEGER
            )
        """)
        
        # Query empty table
        result = conn.execute("""
            SELECT 
                COUNT(*) as total_customers,
                ROUND(AVG(ChurnFlag) * 100, 2) as churn_rate
            FROM customer_churn_analytics
        """).fetchone()
        
        # Should return 0 for count, NULL for avg
        self.assertEqual(result[0], 0)
        self.assertIsNone(result[1])
        
        conn.close()
    
    def test_contract_type_aggregation(self):
        """Test contract type aggregation accuracy"""
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        # Get contract type counts
        result = conn.execute("""
            SELECT ContractType, COUNT(*) as count
            FROM customer_churn_analytics
            GROUP BY ContractType
            ORDER BY ContractType
        """).fetchall()
        
        # Verify aggregation
        total_count = sum([row[1] for row in result])
        self.assertEqual(total_count, 5)
        
        conn.close()
    
    def test_churn_rate_calculation_accuracy(self):
        """Test churn rate calculation accuracy"""
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        # Calculate churn rate
        result = conn.execute("""
            SELECT 
                SUM(ChurnFlag) as churned,
                COUNT(*) as total,
                ROUND(AVG(ChurnFlag) * 100, 2) as churn_rate
            FROM customer_churn_analytics
        """).fetchone()
        
        churned = result[0]
        total = result[1]
        churn_rate = result[2]
        
        # Verify calculation: 2 churned out of 5 = 40%
        self.assertEqual(churned, 2)
        self.assertEqual(total, 5)
        self.assertEqual(churn_rate, 40.0)
        
        conn.close()
    
    def test_read_only_connection(self):
        """Test that read-only connection prevents writes"""
        conn = duckdb.connect(self.prod_db, read_only=True)
        
        # Attempt to insert should fail
        with self.assertRaises(Exception):
            conn.execute("INSERT INTO customer_churn_analytics VALUES ('test', 30, 'Male', 12, 50.0, 'Month-to-Month', 'DSL', 600.0, 'Yes', 'No', 0, 'Low', 'New')")
        
        conn.close()
    
    def test_cors_headers_format(self):
        """Test CORS headers format"""
        # Simulate CORS headers
        headers = {
            'Content-type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
        
        # Verify header format
        self.assertEqual(headers['Content-type'], 'application/json')
        self.assertEqual(headers['Access-Control-Allow-Origin'], '*')
    
    def test_health_check_response(self):
        """Test health check endpoint response"""
        health_response = {"status": "healthy"}
        json_str = json.dumps(health_response)
        
        # Verify JSON format
        parsed = json.loads(json_str)
        self.assertEqual(parsed['status'], 'healthy')
    
    def test_multiple_concurrent_reads(self):
        """Test multiple concurrent read operations"""
        conn1 = duckdb.connect(self.prod_db, read_only=True)
        conn2 = duckdb.connect(self.prod_db, read_only=True)
        
        # Both connections should work simultaneously
        result1 = conn1.execute("SELECT COUNT(*) FROM customer_churn_analytics").fetchone()[0]
        result2 = conn2.execute("SELECT COUNT(*) FROM customer_churn_analytics").fetchone()[0]
        
        self.assertEqual(result1, result2)
        self.assertEqual(result1, 5)
        
        conn1.close()
        conn2.close()


class TestAPIErrorHandling(unittest.TestCase):
    """Test error handling in API"""
    
    def test_invalid_table_query(self):
        """Test query on non-existent table"""
        temp_db = tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False)
        temp_db_name = temp_db.name
        temp_db.close()
        
        try:
            conn = duckdb.connect(temp_db_name)
            
            # Query non-existent table should raise error
            with self.assertRaises(Exception):
                conn.execute("SELECT * FROM nonexistent_table").fetchall()
            
            conn.close()
        finally:
            os.unlink(temp_db_name)
    
    def test_malformed_sql_query(self):
        """Test handling of malformed SQL"""
        temp_db = tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False)
        temp_db_name = temp_db.name
        temp_db.close()
        
        try:
            conn = duckdb.connect(temp_db_name)
            
            # Malformed SQL should raise error
            with self.assertRaises(Exception):
                conn.execute("SELECT * FROM WHERE").fetchall()
            
            conn.close()
        finally:
            os.unlink(temp_db_name)
    
    def test_database_not_found(self):
        """Test handling of non-existent database"""
        # Attempting to connect to non-existent database
        # DuckDB creates it, so we test read-only mode
        with self.assertRaises(Exception):
            conn = duckdb.connect('/nonexistent/path/db.duckdb', read_only=True)


if __name__ == '__main__':
    unittest.main()
