import unittest
from unittest.mock import patch, MagicMock
from src.database import DatabaseConnection

class TestDatabaseConnection(unittest.TestCase):
    
    @patch('psycopg2.connect')
    def test_connection(self, mock_connect):
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Test connection
        db_config = {'host': 'localhost', 'port': '5432', 'database': 'test', 'user': 'user', 'password': 'pass'}
        db = DatabaseConnection(db_config)
        
        # Verify connection was tested
        mock_connect.assert_called_once_with(**db_config)
        mock_cursor.execute.assert_called_with("SELECT 1")
    
    @patch('psycopg2.connect')
    def test_create_table(self, mock_connect):
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Test create table
        db_config = {'host': 'localhost', 'port': '5432', 'database': 'test', 'user': 'user', 'password': 'pass'}
        db = DatabaseConnection(db_config)
        
        columns = {'name': 'TEXT', 'age': 'INTEGER'}
        db.create_table('test_table', columns)
        
        # Verify SQL execution
        mock_cursor.execute.assert_called_with(
            "CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT, age INTEGER)"
        )
        mock_conn.commit.assert_called()
    
    @patch('psycopg2.connect')
    def test_bulk_insert(self, mock_connect):
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Test bulk insert
        db_config = {'host': 'localhost', 'port': '5432', 'database': 'test', 'user': 'user', 'password': 'pass'}
        db = DatabaseConnection(db_config)
        
        data = [('John', 30), ('Jane', 25)]
        columns = ['name', 'age']
        
        with patch('psycopg2.extras.execute_values') as mock_execute_values:
            db.bulk_insert('test_table', data, columns)
            
            # Verify execute_values was called
            mock_execute_values.assert_called_once()
            mock_conn.commit.assert_called()

if __name__ == '__main__':
    unittest.main()

