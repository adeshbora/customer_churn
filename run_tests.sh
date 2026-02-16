#!/bin/bash

echo "🧪 Running Customer Churn Pipeline Tests..."
echo "=========================================="

# Install test dependencies
echo "📦 Installing test dependencies..."
pip install -q pytest pytest-cov faker duckdb pandas apache-airflow

# Run all tests with 100% coverage requirement
echo ""
echo "🔬 Running all tests with 100% coverage requirement..."
python -m pytest tests/ -v --cov=scripts --cov=dags --cov-report=html --cov-report=term-missing --cov-fail-under=100

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ All tests passed with 100% coverage!"
    echo "📊 Coverage report: htmlcov/index.html"
else
    echo ""
    echo "❌ Tests failed or coverage below 100%!"
    exit 1
fi

# Run specific test categories (optional)
echo ""
echo "📋 Test categories available:"
echo "  - Unit tests: pytest tests/test_elt_script.py -v"
echo "  - Integration tests: pytest tests/test_integration.py -v"
echo "  - API tests: pytest tests/test_api.py tests/test_proxy.py -v"
echo "  - Pipeline tests: pytest tests/test_pipeline.py -v"
echo "  - DAG tests: pytest tests/test_dag.py -v"