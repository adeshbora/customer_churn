#!/usr/bin/env python3
"""
DuckDB HTTP API Proxy

Purpose: Expose DuckDB data via HTTP API for Grafana and other consumers.

Why HTTP API?
- Grafana doesn't natively support DuckDB
- HTTP is universal and firewall-friendly
- Enables multiple consumers (dashboards, apps, APIs)
- Decouples data access from storage

Architecture:
- Lightweight HTTP server (no external dependencies)
- Read-only access to production database (safety)
- JSON responses for easy consumption
- CORS enabled for browser-based clients

Endpoints:
- /health: Health check for monitoring
- /summary: Key business metrics (total customers, churn rate, etc.)
- /churn_by_contract: Churn analysis by contract type
- /metrics: Historical data quality metrics
- /analytics: Detailed analytics by contract type
"""

import duckdb
import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import logging

# Configure logging for observability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database path from environment variable
PRODUCTION_DB = os.getenv('PRODUCTION_DB_PATH', '/opt/airflow/databases/production.duckdb')


class DuckDBHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler for DuckDB API.
    
    Design: Each request opens a new connection (stateless)
    Why? DuckDB is embedded, connections are lightweight, and this ensures thread safety.
    """
    
    def do_GET(self):
        """
        Handle GET requests for different API endpoints.
        
        Pattern: Parse URL -> Query DuckDB -> Return JSON
        """
        try:
            # Parse URL and query parameters
            parsed_url = urllib.parse.urlparse(self.path)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            # Connect to DuckDB in read-only mode
            # Why read-only? Prevents accidental data modification via API
            conn = duckdb.connect(PRODUCTION_DB, read_only=True)
            
            # === ENDPOINT ROUTING ===
            # Each endpoint serves a specific analytical purpose
            
            if parsed_url.path == '/health':
                # Health check endpoint for monitoring and load balancers
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "healthy"}).encode())
                
            elif parsed_url.path == '/metrics':
                # Historical data quality metrics for trend analysis
                # Returns last 10 metric snapshots
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
                # Summary metrics endpoint for dashboard KPIs
                # Single query for performance (one table scan)
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
                # Churn analysis by contract type for segmentation
                # Ordered by churn rate to highlight risk segments
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
        """
        Override default logging to reduce noise.
        
        Why suppress? Default HTTP server logs every request.
        We use custom logger for important events only.
        """
        pass

if __name__ == "__main__":
    port = int(os.getenv('API_PORT', '5432'))
    server_address = ('', port)
    httpd = HTTPServer(server_address, DuckDBHandler)
    logger.info(f"DuckDB HTTP API server started on port {port}")
    logger.info("Available endpoints: /health, /summary, /churn_by_contract, /metrics, /analytics")
    
    # Keep the server running indefinitely
    # Graceful shutdown on Ctrl+C
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        httpd.server_close()
        logger.info("Server stopped")