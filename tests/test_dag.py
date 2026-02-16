"""
Complete test coverage for Airflow DAG
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


class TestAirflowDAG(unittest.TestCase):
    """Test Airflow DAG structure and tasks"""
    
    @patch('customer_churn_elt_dag.CustomerChurnELT')
    def test_extract_and_load_task(self, mock_elt_class):
        """Test extract and load task function"""
        from customer_churn_elt_dag import extract_and_load_task
        
        # Setup mock
        mock_elt = Mock()
        mock_elt.extract_and_load.return_value = 100
        mock_elt_class.return_value = mock_elt
        
        # Create mock context
        mock_ti = Mock()
        context = {'task_instance': mock_ti}
        
        # Execute task
        result = extract_and_load_task(**context)
        
        # Verify
        self.assertEqual(result, 100)
        mock_elt.extract_and_load.assert_called_once()
        mock_ti.xcom_push.assert_called_once_with(key='staging_row_count', value=100)
    
    @patch('customer_churn_elt_dag.CustomerChurnELT')
    def test_transform_task(self, mock_elt_class):
        """Test transform task function"""
        from customer_churn_elt_dag import transform_task
        
        # Setup mock
        mock_elt = Mock()
        mock_elt.transform.return_value = 100
        mock_elt_class.return_value = mock_elt
        
        # Create mock context
        mock_ti = Mock()
        mock_ti.xcom_pull.return_value = 100
        context = {'task_instance': mock_ti}
        
        # Execute task
        result = transform_task(**context)
        
        # Verify
        self.assertEqual(result, 100)
        mock_elt.transform.assert_called_once()
        mock_ti.xcom_pull.assert_called_once_with(
            task_ids='extract_and_load_to_staging',
            key='staging_row_count'
        )
        mock_ti.xcom_push.assert_called_once_with(key='production_row_count', value=100)
    
    @patch('customer_churn_elt_dag.CustomerChurnELT')
    def test_generate_metrics_task(self, mock_elt_class):
        """Test metrics generation task function"""
        from customer_churn_elt_dag import generate_metrics_task
        
        # Setup mock
        mock_elt = Mock()
        mock_metrics = {'total_records': 100, 'churn_rate': 25.5}
        mock_elt.generate_data_quality_metrics.return_value = mock_metrics
        mock_elt_class.return_value = mock_elt
        
        # Create mock context
        mock_ti = Mock()
        context = {'task_instance': mock_ti}
        
        # Execute task
        result = generate_metrics_task(**context)
        
        # Verify
        self.assertEqual(result, mock_metrics)
        mock_elt.generate_data_quality_metrics.assert_called_once()
        mock_ti.xcom_push.assert_called_once_with(key='metrics', value=mock_metrics)
    
    def test_dag_structure(self):
        """Test DAG configuration and structure"""
        from customer_churn_elt_dag import dag, extract_load_task, transform_load_task, data_quality_task
        
        # Verify DAG exists
        self.assertIsNotNone(dag)
        self.assertEqual(dag.dag_id, 'customer_churn_elt_pipeline')
        
        # Verify tasks exist
        self.assertIsNotNone(extract_load_task)
        self.assertIsNotNone(transform_load_task)
        self.assertIsNotNone(data_quality_task)
        
        # Verify task IDs
        self.assertEqual(extract_load_task.task_id, 'extract_and_load_to_staging')
        self.assertEqual(transform_load_task.task_id, 'transform_and_load_to_production')
        self.assertEqual(data_quality_task.task_id, 'generate_data_quality_report')
    
    def test_dag_default_args(self):
        """Test DAG default arguments"""
        from customer_churn_elt_dag import default_args
        
        self.assertEqual(default_args['owner'], 'data-team')
        self.assertEqual(default_args['depends_on_past'], False)
        self.assertEqual(default_args['email_on_failure'], False)
        self.assertEqual(default_args['retries'], 2)
    
    def test_dag_schedule(self):
        """Test DAG schedule configuration"""
        from customer_churn_elt_dag import dag
        from datetime import timedelta
        
        self.assertEqual(dag.schedule_interval, timedelta(hours=1))
        self.assertEqual(dag.catchup, False)
        self.assertEqual(dag.max_active_runs, 1)
    
    def test_dag_tags(self):
        """Test DAG tags"""
        from customer_churn_elt_dag import dag
        
        self.assertIn('elt', dag.tags)
        self.assertIn('customer_churn', dag.tags)
        self.assertIn('analytics', dag.tags)
    
    def test_task_dependencies(self):
        """Test task dependency chain"""
        from customer_churn_elt_dag import extract_load_task, transform_load_task, data_quality_task
        
        # Verify dependencies
        self.assertIn(transform_load_task, extract_load_task.downstream_list)
        self.assertIn(data_quality_task, transform_load_task.downstream_list)


if __name__ == '__main__':
    unittest.main()
