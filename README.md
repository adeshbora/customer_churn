# Customer Churn ELT Pipeline

A complete ELT (Extract, Load, Transform) pipeline for customer churn data analysis using Apache Airflow, DuckDB, and Grafana.

## Architecture

- **Orchestration**: Apache Airflow for workflow management
- **Staging Database**: DuckDB for fast analytical processing
- **Production Database**: DuckDB optimized for reporting
- **Reporting**: Grafana for data visualization
- **Containerization**: Docker Compose for easy deployment

## Features

- ✅ **Hourly Data Ingestion**: Configurable schedule for processing CSV data
- ✅ **Data Quality**: Missing value handling with sensible defaults
- ✅ **PII Anonymization**: Customer ID anonymization using UUID
- ✅ **Data Transformation**: Derived fields for analytics (churn flags, categories)
- ✅ **Monitoring**: Data quality metrics and pipeline statistics
- ✅ **Reporting**: Interactive Grafana dashboards
- ✅ **Containerized**: Complete Docker setup for easy deployment

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available
- Ports 8080, 3000, and 5432 available

### 1. Clone and Setup

```bash
git clone <repository-url>
cd customer_churn
```

### 2. Start the Pipeline

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 3. Access Services

- **Airflow Web UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`
- **Grafana Dashboard**: http://localhost:3000
  - Username: `admin`
  - Password: `admin`

### 4. Trigger the Pipeline

1. Go to Airflow UI (http://localhost:8080)
2. Find the `customer_churn_elt_pipeline` DAG
3. Toggle it ON
4. Click "Trigger DAG" to run immediately

## Pipeline Overview

### Data Flow

```
CSV Data → Staging (DuckDB) → Transform → Production (DuckDB) → Grafana
```

### Pipeline Steps

1. **Extract & Load**: Read CSV data and load into staging DuckDB table
2. **Transform**: 
   - Handle missing values with defaults
   - Anonymize PII (Customer IDs)
   - Create derived analytics fields
   - Load into production table
3. **Quality Check**: Generate data quality metrics
4. **Reporting**: Visualize results in Grafana

### Transformations Applied

- **Missing Values**: Filled with median/mode/defaults
- **PII Anonymization**: Customer IDs replaced with UUIDs
- **Derived Fields**:
  - `ChurnFlag`: Binary churn indicator
  - `MonthlyChargesCategory`: Low/Medium/High buckets
  - `TenureCategory`: New/Medium/Long customer segments

## Configuration

### Schedule Configuration

The pipeline runs hourly by default. To change the schedule:

1. Edit `dags/customer_churn_elt_dag.py`
2. Modify the `schedule_interval` parameter:

```python
dag = DAG(
    'customer_churn_elt_pipeline',
    schedule_interval=timedelta(hours=1),  # Change this
    # ... other parameters
)
```

### Data Source Configuration

To use different CSV data:

1. Replace `data/customer_churn_data.csv` with your data
2. Update column mappings in the DAG if schema differs

## Monitoring & Observability

### Airflow Monitoring

- **DAG View**: Monitor pipeline execution status
- **Task Logs**: Detailed logs for each pipeline step
- **Gantt Chart**: Execution timeline visualization

### Data Quality Metrics

The pipeline tracks:
- Total record count
- Churn rate percentage
- Average monthly charges
- Average customer tenure

Access metrics via:
- Grafana dashboard
- Direct DuckDB queries
- Airflow task logs

### Grafana Dashboards

Pre-configured dashboards show:
- Key business metrics (churn rate, customer count)
- Churn analysis by contract type and services
- Customer segmentation by charges and tenure
- Data quality trends over time

## Database Schema

### Staging Table: `staging_customer_churn`
```sql
CustomerID INTEGER
Age INTEGER
Gender VARCHAR
Tenure INTEGER
MonthlyCharges DECIMAL(10,2)
ContractType VARCHAR
InternetService VARCHAR
TotalCharges DECIMAL(10,2)
TechSupport VARCHAR
Churn VARCHAR
load_timestamp TIMESTAMP
```

### Production Table: `customer_churn_analytics`
```sql
AnonymizedCustomerID VARCHAR
Age INTEGER
Gender VARCHAR
Tenure INTEGER
MonthlyCharges DECIMAL(10,2)
ContractType VARCHAR
InternetService VARCHAR
TotalCharges DECIMAL(10,2)
TechSupport VARCHAR
Churn VARCHAR
ChurnFlag INTEGER
MonthlyChargesCategory VARCHAR
TenureCategory VARCHAR
load_timestamp TIMESTAMP
processed_timestamp TIMESTAMP
```

## Troubleshooting

### Common Issues

1. **Services not starting**:
   ```bash
   docker-compose logs [service-name]
   ```

2. **Airflow DAG not appearing**:
   - Check DAG syntax: `docker-compose exec airflow-webserver airflow dags list`
   - Verify file permissions

3. **Database connection issues**:
   - Ensure DuckDB files are created in `databases/` directory
   - Check volume mounts in docker-compose.yml

4. **Grafana dashboard not loading**:
   - Verify datasource configuration
   - Check DuckDB proxy connection

### Logs and Debugging

```bash
# View all logs
docker-compose logs

# View specific service logs
docker-compose logs airflow-scheduler
docker-compose logs grafana

# Access container shell
docker-compose exec airflow-webserver bash
```

## Development

### Adding New Transformations

1. Edit `dags/customer_churn_elt_dag.py`
2. Modify the `transform_and_load_to_production` function
3. Add new derived fields or data quality checks

### Custom Dashboards

1. Create dashboard in Grafana UI
2. Export JSON configuration
3. Add to `grafana/provisioning/dashboards/`

### Scaling Considerations

- **Data Volume**: DuckDB handles millions of rows efficiently
- **Concurrency**: Adjust Airflow executor settings for parallel processing
- **Storage**: Monitor disk usage in `databases/` directory

## Tech Stack

- **Python 3.9**: Core language
- **Apache Airflow 2.7.1**: Workflow orchestration
- **DuckDB 0.9.1**: Analytical database
- **Grafana**: Data visualization
- **Docker & Docker Compose**: Containerization
- **PostgreSQL**: Airflow metadata store
