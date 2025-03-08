import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.pipeline import DataPipeline, PerformanceMonitor

class TestDataPipeline(unittest.TestCase):
    
    def setUp(self):
        self.mock_db = MagicMock()
        self.pipeline = DataPipeline(self.mock_db)
    
    def test_infer_sql_types(self):
        # Create test DataFrame
        df = pd.DataFrame({
            'text_col': ['a', 'b', 'c'],
            'int_col': [1, 2, 3],
            'float_col': [1.1, 2.2, 3.3],
            'date_col': pd.to_datetime(['2021-01-01', '2021-01-02', '2021-01-03']),
            'bool_col': [True, False, True]
        })
        
        # Test type inference
        result = self.pipeline.infer_sql_types(df)
        
        # Verify results
        self.assertEqual(result['text_col'], 'TEXT')
        self.assertEqual(result['int_col'], 'INTEGER')
        self.assertEqual(result['float_col'], 'FLOAT')
        self.assertEqual(result['date_col'], 'TIMESTAMP')
        self.assertEqual(result['bool_col'], 'BOOLEAN')
    
    def test_validate_data(self):
        # Create test DataFrame with null values
        df = pd.DataFrame({
            'col1': ['a', None, 'c'],
            'col2': [1, 2, None],
            'col3': [None, None, None]
        })
        
        # Test validation
        result = self.pipeline.validate_data(df)
        
        # Verify null rows are removed
        self.assertEqual(len(result), 2)  # Row with all nulls should be removed
    
    @patch('pandas.read_csv')
    def test_process_csv(self, mock_read_csv):
        # Setup mock DataFrame
        mock_df = pd.DataFrame({
            'name': ['John', 'Jane'],
            'age': [30, 25]
        })
        mock_read_csv.return_value = [mock_df]  # Return as iterable for chunking
        
        # Test process_csv
        self.pipeline.process_csv('dummy.csv', 'test_table')
        
        # Verify table creation and data insertion
        self.mock_db.create_table.assert_called_once()
        self.mock_db.bulk_insert.assert_called_once()
        
        # Verify table creation was only done once
        self.assertTrue(self.pipeline.table_created)

class TestPerformanceMonitor(unittest.TestCase):
    
    def test_performance_metrics(self):
        # Create monitor
        monitor = PerformanceMonitor()
        
        # Update with some data
        monitor.update(100)
        monitor.update(150)
        
        # Get metrics
        metrics = monitor.get_metrics()
        
        # Verify metrics
        self.assertEqual(metrics['records_processed'], 250)
        self.assertEqual(metrics['chunks_processed'], 2)
        self.assertGreater(metrics['elapsed_time'], 0)
        self.assertGreater(metrics['records_per_second'], 0)

if __name__ == '__main__':
    unittest.main()

