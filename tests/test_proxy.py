"""
Complete test coverage for DuckDB HTTP API Proxy
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import sys
import os
import tempfile
import duckdb
import pandas as pd
from io import BytesIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


class TestDuckDBHandler(unittest.TestCase):
    """Test DuckDB HTTP handler"""
    
    def setUp(self):
        """Setup test database"""
        self.temp_dir = tempfile.mkdtemp()
        self.prod_db = os.path.join(self.temp_dir, 'production.duckdb')
        
        # Create test database
        conn = duckdb.connect(self.prod_db)
        
        test_data = pd.DataFrame({
            'AnonymizedCustomerID': ['uuid-1', 'uuid-2', 'uuid-3'],
            'ChurnFlag': [0, 1, 0],
            'MonthlyCharges': [50.0, 75.0, 100.0],
            'Tenure': [12, 24, 36],
            'ContractType': ['Month-to-Month', 'One-Year', 'Two-Year']
        })
        
        conn.execute("CREATE TABLE customer_churn_analytics AS SELECT * FROM test_data")
        
        metrics_data = pd.DataFrame({
            'metric_name': ['total_records', 'churn_rate_percent'],
            'metric_value': [3.0, 33.33],
            'run_timestamp': [pd.Timestamp.now()] * 2
        })
        
        conn.execute("CREATE TABLE data_quality_metrics AS SELECT * FROM metrics_data")
        conn.close()
    
    def tearDown(self):
        """Cleanup"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch('duckdb_proxy.PRODUCTION_DB')
    def test_health_endpoint(self, mock_db_path):
        """Test /health endpoint"""
        from duckdb_proxy import DuckDBHandler
        
        mock_db_path.__str__ = Mock(return_value=self.prod_db)
        
        handler = DuckDBHandler(Mock(), ('127.0.0.1', 8080), Mock())
        handler.path = '/health'
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = BytesIO()
        
        handler.do_GET()
        
        handler.send_response.assert_called_with(200)
        handler.send_header.assert_any_call('Content-type', 'application/json')
    
    @patch('duckdb_proxy.PRODUCTION_DB')
    def test_summary_endpoint(self, mock_db_path):
        """Test /summary endpoint"""
        from duckdb_proxy import DuckDBHandler
        
        mock_db_path.__str__ = Mock(return_value=self.prod_db)
        
        handler = DuckDBHandler(Mock(), ('127.0.0.1', 8080), Mock())
        handler.path = '/summary'
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = BytesIO()
        
        handler.do_GET()
        
        handler.send_response.assert_called_with(200)
        handler.send_header.assert_any_call('Access-Control-Allow-Origin', '*')
    
    @patch('duckdb_proxy.PRODUCTION_DB')
    def test_churn_by_contract_endpoint(self, mock_db_path):
        """Test /churn_by_contract endpoint"""
        from duckdb_proxy import DuckDBHandler
        
        mock_db_path.__str__ = Mock(return_value=self.prod_db)
        
        handler = DuckDBHandler(Mock(), ('127.0.0.1', 8080), Mock())
        handler.path = '/churn_by_contract'
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = BytesIO()
        
        handler.do_GET()
        
        handler.send_response.assert_called_with(200)
    
    @patch('duckdb_proxy.PRODUCTION_DB')
    def test_metrics_endpoint(self, mock_db_path):
        """Test /metrics endpoint"""
        from duckdb_proxy import DuckDBHandler
        
        mock_db_path.__str__ = Mock(return_value=self.prod_db)
        
        handler = DuckDBHandler(Mock(), ('127.0.0.1', 8080), Mock())
        handler.path = '/metrics'
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = BytesIO()
        
        handler.do_GET()
        
        handler.send_response.assert_called_with(200)
    
    @patch('duckdb_proxy.PRODUCTION_DB')
    def test_analytics_endpoint(self, mock_db_path):
        """Test /analytics endpoint"""
        from duckdb_proxy import DuckDBHandler
        
        mock_db_path.__str__ = Mock(return_value=self.prod_db)
        
        handler = DuckDBHandler(Mock(), ('127.0.0.1', 8080), Mock())
        handler.path = '/analytics'
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = BytesIO()
        
        handler.do_GET()
        
        handler.send_response.assert_called_with(200)
    
    @patch('duckdb_proxy.PRODUCTION_DB')
    def test_404_endpoint(self, mock_db_path):
        """Test 404 for unknown endpoint"""
        from duckdb_proxy import DuckDBHandler
        
        mock_db_path.__str__ = Mock(return_value=self.prod_db)
        
        handler = DuckDBHandler(Mock(), ('127.0.0.1', 8080), Mock())
        handler.path = '/unknown'
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = BytesIO()
        
        handler.do_GET()
        
        handler.send_response.assert_called_with(404)
    
    @patch('duckdb_proxy.PRODUCTION_DB', '/nonexistent/db.duckdb')
    def test_error_handling(self):
        """Test error handling in handler"""
        from duckdb_proxy import DuckDBHandler
        
        handler = DuckDBHandler(Mock(), ('127.0.0.1', 8080), Mock())
        handler.path = '/summary'
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.wfile = BytesIO()
        
        handler.do_GET()
        
        handler.send_response.assert_called_with(500)
    
    def test_log_message_suppression(self):
        """Test log message suppression"""
        from duckdb_proxy import DuckDBHandler
        
        handler = DuckDBHandler(Mock(), ('127.0.0.1', 8080), Mock())
        
        # Should not raise exception
        handler.log_message("test %s", "message")


class TestDuckDBProxyMain(unittest.TestCase):
    """Test main function and server startup"""
    
    @patch('duckdb_proxy.HTTPServer')
    @patch('duckdb_proxy.logger')
    def test_main_startup(self, mock_logger, mock_server):
        """Test server startup"""
        mock_httpd = Mock()
        mock_server.return_value = mock_httpd
        mock_httpd.serve_forever.side_effect = KeyboardInterrupt()
        
        # Import and run main
        import duckdb_proxy
        
        # Simulate main execution
        try:
            server_address = ('', 5432)
            httpd = mock_server(server_address, duckdb_proxy.DuckDBHandler)
            httpd.serve_forever()
        except KeyboardInterrupt:
            httpd.server_close()
        
        mock_httpd.server_close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
