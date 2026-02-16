# Test Documentation

## Test Coverage Overview

This project has comprehensive test coverage for all major components:

### Test Files

1. **test_elt_script.py** - Unit tests for ELT processing script
   - Extract and load functionality
   - Transform operations
   - Data quality metrics
   - Error handling
   - Edge cases (empty data, all nulls, single row)

2. **test_pipeline.py** - Unit tests for pipeline components
   - Data loading to staging
   - Missing value handling
   - PII anonymization
   - Derived field creation
   - Metrics calculation

3. **test_integration.py** - End-to-end integration tests
   - Complete pipeline flow
   - Data consistency across stages
   - Multi-stage transformations

4. **test_api.py** - API endpoint tests
   - SQL query accuracy
   - JSON response formatting
   - Error handling
   - Concurrent access

## Running Tests

### Run All Tests
```bash
chmod +x run_tests.sh
./run_tests.sh
```

### Run Specific Test Files
```bash
# ELT script tests
pytest tests/test_elt_script.py -v

# Pipeline tests
pytest tests/test_pipeline.py -v

# Integration tests
pytest tests/test_integration.py -v

# API tests
pytest tests/test_api.py -v
```

### Run Tests with Coverage
```bash
pytest tests/ --cov=scripts --cov=dags --cov-report=html
```

### Run Specific Test Cases
```bash
# Run single test
pytest tests/test_elt_script.py::TestCustomerChurnELT::test_extract_and_load_success -v

# Run tests matching pattern
pytest tests/ -k "test_transform" -v
```

## Test Coverage Metrics

Target coverage: **80%+**

### Coverage by Module

- **scripts/customer_churn_elt.py**: 90%+
  - All methods tested
  - Error paths covered
  - Edge cases handled

- **dags/customer_churn_elt_dag.py**: 75%+
  - Task functions tested
  - XCom communication verified

- **scripts/duckdb_proxy.py**: 85%+
  - All endpoints tested
  - Query accuracy verified
  - Error handling covered

## Test Categories

### Unit Tests
Test individual functions and methods in isolation.

**Examples:**
- `test_extract_and_load_success`
- `test_transform_missing_value_handling`
- `test_generate_data_quality_metrics`

### Integration Tests
Test complete workflows and component interactions.

**Examples:**
- `test_end_to_end_pipeline`
- `test_run_full_pipeline_success`

### Edge Case Tests
Test boundary conditions and error scenarios.

**Examples:**
- `test_empty_csv_file`
- `test_all_missing_values`
- `test_single_row_dataset`

## What's Tested

### Data Processing
- ✅ CSV extraction
- ✅ Staging table loading
- ✅ Missing value imputation
- ✅ PII anonymization (UUID generation)
- ✅ Derived field creation (ChurnFlag, categories)
- ✅ Production table loading
- ✅ Data consistency validation

### Data Quality
- ✅ Row count validation
- ✅ Null value handling
- ✅ Churn rate calculation
- ✅ Metric generation
- ✅ Metric persistence

### API Functionality
- ✅ Summary endpoint queries
- ✅ Churn by contract queries
- ✅ Metrics endpoint queries
- ✅ JSON response formatting
- ✅ CORS headers
- ✅ Read-only database access

### Error Handling
- ✅ File not found errors
- ✅ Empty dataset handling
- ✅ All null values handling
- ✅ Invalid SQL queries
- ✅ Database connection errors

### Idempotency
- ✅ Multiple runs produce same results
- ✅ Truncate and reload pattern
- ✅ No duplicate data

## Test Data

Tests use synthetic data with:
- Various missing value patterns
- Different customer segments
- Multiple contract types
- Churn and non-churn cases

## Continuous Integration

Tests should be run:
- Before committing code
- In CI/CD pipeline
- Before deployment
- After dependency updates

## Adding New Tests

When adding new functionality:

1. Write unit tests first (TDD)
2. Test happy path
3. Test error cases
4. Test edge cases
5. Verify coverage remains above 80%

### Test Template
```python
def test_new_feature(self):
    \"\"\"Test description\"\"\"
    # Arrange
    # ... setup test data
    
    # Act
    # ... execute function
    
    # Assert
    # ... verify results
```

## Troubleshooting Tests

### Tests Fail Locally
```bash
# Check dependencies
pip install -r requirements.txt

# Check Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Run with verbose output
pytest tests/ -vv
```

### Coverage Too Low
```bash
# See which lines aren't covered
pytest tests/ --cov=scripts --cov-report=term-missing

# Generate HTML report for detailed view
pytest tests/ --cov=scripts --cov-report=html
open htmlcov/index.html
```

### Slow Tests
```bash
# Run only fast tests
pytest tests/ -m "not slow"

# Show slowest tests
pytest tests/ --durations=10
```

## Test Maintenance

- Review and update tests when requirements change
- Remove obsolete tests
- Refactor duplicate test code
- Keep test data realistic but minimal
- Document complex test scenarios
