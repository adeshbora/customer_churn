#!/bin/bash

echo "🧪 Running Customer Churn Pipeline Tests..."

# Install test dependencies
pip install pytest pytest-cov

# Run tests with coverage
python -m pytest tests/ -v --cov=dags --cov-report=html --cov-report=term

echo "✅ Tests completed. Coverage report generated in htmlcov/"