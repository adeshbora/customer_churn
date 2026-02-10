#!/usr/bin/env python3
"""
Simple HTTP server to expose DuckDB data for Grafana.
Since Grafana needs PostgreSQL protocol, we'll use a simple HTTP API approach.
"""

import time
import duckdb
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DuckDBHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Parse the URL and query parameters
            parsed_url = urllib.parse.urlparse(self.path)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            # Connect to DuckDB
            conn = duckdb.connect('/opt/airflow/databases/production.duckdb')
            
            # Handle different endpoints
            if parsed_url.path == '/health':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "healthy"}).encode())
                
            elif parsed_url.path == '/metrics':
                # Get data quality metrics
                try:
                    result = conn.execute("""
                        SELECT metric_name, metric_value, run_timestamp 
                        FROM data_quality_metrics 
                        ORDER BY run_timestamp DESC 
                        LIMIT 10
                    """).fetchall()
                    
                    metrics = []
                    for row in result:
                        metrics.append({
                            'metric_name': row[0],
                            'metric_value': float(row[1]) if row[1] else 0,
                            'timestamp': str(row[2])
                        })
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps(metrics).encode())
                    
                except Exception as e:
                    logger.error(f"Error fetching metrics: {e}")
                    self.send_response(500)
                    self.end_headers()
                    
            elif parsed_url.path == '/analytics':
                # Get customer analytics data
                try:
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
                    
                    analytics = []
                    for row in result:
                        analytics.append({
                            'total_customers': int(row[0]),
                            'churn_rate': float(row[1]) if row[1] else 0,
                            'avg_monthly_charges': float(row[2]) if row[2] else 0,
                            'avg_tenure': float(row[3]) if row[3] else 0,
                            'contract_type': row[4],
                            'contract_count': int(row[5]),
                            'contract_churn_rate': float(row[6]) if row[6] else 0
                        })
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps(analytics).encode())
                    
                except Exception as e:
                    logger.error(f"Error fetching analytics: {e}")
                    self.send_response(500)
                    self.end_headers()
                    
            elif parsed_url.path == '/summary':
                # Get summary metrics
                try:
                    result = conn.execute("""
                        SELECT 
                            COUNT(*) as total_customers,
                            ROUND(AVG(ChurnFlag) * 100, 2) as churn_rate,
                            ROUND(AVG(MonthlyCharges), 2) as avg_monthly_charges,
                            ROUND(AVG(Tenure), 2) as avg_tenure
                        FROM customer_churn_analytics
                    """).fetchone()
                    
                    data = {
                        'total_customers': int(result[0]) if result[0] else 0,
                        'churn_rate': float(result[1]) if result[1] else 0,
                        'avg_monthly_charges': float(result[2]) if result[2] else 0,
                        'avg_tenure': float(result[3]) if result[3] else 0
                    }
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps(data).encode())
                    
                except Exception as e:
                    logger.error(f"Error fetching summary: {e}")
                    self.send_response(500)
                    self.end_headers()
                    
            elif parsed_url.path == '/churn_by_contract':
                # Get churn by contract type
                try:
                    result = conn.execute("""
                        SELECT 
                            ContractType,
                            COUNT(*) as count,
                            ROUND(AVG(ChurnFlag) * 100, 2) as churn_rate
                        FROM customer_churn_analytics
                        GROUP BY ContractType
                        ORDER BY churn_rate DESC
                    """).fetchall()
                    
                    data = [{
                        'contract_type': row[0],
                        'count': int(row[1]),
                        'churn_rate': float(row[2]) if row[2] else 0
                    } for row in result]
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps(data).encode())
                    
                except Exception as e:
                    logger.error(f"Error fetching churn by contract: {e}")
                    self.send_response(500)
                    self.end_headers()
                    
            else:
                self.send_response(404)
                self.end_headers()
                
            conn.close()
            
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            self.send_response(500)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress default logging
        pass

if __name__ == "__main__":
    server_address = ('', 5432)
    httpd = HTTPServer(server_address, DuckDBHandler)
    logger.info("DuckDB HTTP server started on port 5432")
    
    # Keep the server running
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server stopped")
        httpd.server_close()