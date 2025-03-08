import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
import logging

class DatabaseConnection:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Test connection on initialization
        self._test_connection()
    
    def _test_connection(self):
        """Test database connection on initialization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    self.logger.info("Database connection successful")
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            raise

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.config)
            yield conn
        except psycopg2.Error as e:
            self.logger.error(f"Database error: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

    def create_table(self, table_name, columns):
        """Create a table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {', '.join([f'{name} {dtype}' for name, dtype in columns.items()])}
        )
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_query)
                    conn.commit()
                    self.logger.info(f"Table '{table_name}' created or already exists")
        except Exception as e:
            self.logger.error(f"Error creating table: {str(e)}")
            raise

    def bulk_insert(self, table_name, data, columns):
        """Insert multiple rows into a table"""
        if not data:
            self.logger.warning("No data to insert")
            return
            
        insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, insert_query, data)
                    conn.commit()
                    self.logger.info(f"Inserted {len(data)} rows into '{table_name}'")
        except Exception as e:
            self.logger.error(f"Error inserting data: {str(e)}")
            raise
    
    def execute_query(self, query, params=None):
        """Execute a custom query"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params or ())
                    conn.commit()
                    self.logger.info("Query executed successfully")
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise

