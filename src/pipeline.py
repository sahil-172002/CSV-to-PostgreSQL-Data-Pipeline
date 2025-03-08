import pandas as pd
from typing import List, Dict, Any, Callable, Optional
import logging
import time

class DataPipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass

class PerformanceMonitor:
    """Monitor performance metrics during pipeline execution"""
    def __init__(self):
        self.start_time = time.time()
        self.records_processed = 0
        self.chunks_processed = 0
        
    def update(self, chunk_size: int):
        """Update metrics after processing a chunk"""
        self.records_processed += chunk_size
        self.chunks_processed += 1
        
    def get_metrics(self):
        """Get current performance metrics"""
        elapsed_time = time.time() - self.start_time
        records_per_second = self.records_processed / elapsed_time if elapsed_time > 0 else 0
        return {
            'elapsed_time': elapsed_time,
            'records_processed': self.records_processed,
            'chunks_processed': self.chunks_processed,
            'records_per_second': records_per_second
        }

class DataPipeline:
    """Pipeline for processing CSV data and loading into PostgreSQL"""
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        self.table_created = False

    def infer_sql_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infer PostgreSQL column types from pandas DataFrame"""
        type_mapping = {
            'object': 'TEXT',
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN'
        }
        
        return {
            col: type_mapping.get(str(dtype), 'TEXT')
            for col, dtype in df.dtypes.items()
        }

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean data before insertion"""
        # Remove rows with all NULL values
        df = df.dropna(how='all')
        
        # Convert date strings to datetime
        date_columns = df.select_dtypes(include=['object']).columns
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='ignore')
            except Exception:
                pass
        
        return df

    def safe_process_chunk(self, chunk: pd.DataFrame, table_name: str):
        """Safely process a chunk of data"""
        try:
            # Validate data
            chunk = self.validate_data(chunk)
            
            # Convert DataFrame to list of tuples
            columns = chunk.columns.tolist()
            values = [tuple(row) for row in chunk.values]
            
            # Bulk insert data
            self.db.bulk_insert(table_name, values, columns)
            
        except Exception as e:
            raise DataPipelineError(f"Error processing chunk: {str(e)}")

    def process_csv(self, 
                   file_path: str, 
                   table_name: str, 
                   chunk_size: int = 1000,
                   validation_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
                   monitor: Optional[PerformanceMonitor] = None):
        """Process CSV file and load into PostgreSQL"""
        if monitor is None:
            monitor = PerformanceMonitor()
            
        self.logger.info(f"Starting CSV import from {file_path} to table {table_name}")
        
        try:
            # Read CSV in chunks
            for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
                self.logger.info(f"Processing chunk {i+1}...")
                
                # Clean column names
                chunk.columns = [col.lower().replace(' ', '_') for col in chunk.columns]
                
                # Create table if it doesn't exist
                if not self.table_created:
                    column_types = self.infer_sql_types(chunk)
                    self.db.create_table(table_name, column_types)
                    self.table_created = True
                
                # Apply custom validation if provided
                if validation_func:
                    chunk = validation_func(chunk)
                else:
                    chunk = self.validate_data(chunk)
                
                # Process the chunk
                self.safe_process_chunk(chunk, table_name)
                
                # Update performance metrics
                monitor.update(len(chunk))
            
            # Log performance metrics
            metrics = monitor.get_metrics()
            self.logger.info(
                f"Import completed in {metrics['elapsed_time']:.2f} seconds. "
                f"Processed {metrics['records_processed']} records at "
                f"{metrics['records_per_second']:.2f} records/second."
            )
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error processing CSV: {str(e)}")
            raise

