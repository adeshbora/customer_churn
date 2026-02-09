#!/bin/bash

echo "🚀 Starting Customer Churn ELT Pipeline..."

# Create necessary directories
mkdir -p databases

# Start all services
echo "📦 Starting Docker services..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."

# Wait for Airflow to be ready
echo "🔄 Waiting for Airflow webserver..."
until curl -f http://localhost:8080/health > /dev/null 2>&1; do
    echo "   Airflow not ready yet, waiting..."
    sleep 10
done

echo "✅ Airflow is ready!"

# Wait for Grafana to be ready
echo "🔄 Waiting for Grafana..."
until curl -f http://localhost:3000/api/health > /dev/null 2>&1; do
    echo "   Grafana not ready yet, waiting..."
    sleep 5
done

echo "✅ Grafana is ready!"

echo ""
echo "🎉 Customer Churn ELT Pipeline is now running!"
echo ""
echo "📊 Access your services:"
echo "   • Airflow Web UI: http://localhost:8080 (admin/admin)"
echo "   • Grafana Dashboard: http://localhost:3000 (admin/admin)"
echo ""
echo "🔧 Next steps:"
echo "   1. Go to Airflow UI and enable the 'customer_churn_elt_pipeline' DAG"
echo "   2. Trigger the DAG to run the pipeline"
echo "   3. View results in Grafana dashboard"
echo ""
echo "📋 To stop the pipeline: docker-compose down"
echo "📋 To view logs: docker-compose logs -f [service-name]"