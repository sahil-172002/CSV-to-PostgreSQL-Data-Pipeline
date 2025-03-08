from src.config import DB_CONFIG
from src.database import DatabaseConnection
from src.pipeline import DataPipeline

def basic_example():
    """Basic example of using the pipeline"""
    # Initialize database connection
    db = DatabaseConnection(DB_CONFIG)
    
    # Create pipeline
    pipeline = DataPipeline(db)
    
    # Process CSV file
    pipeline.process_csv(
        file_path='data/sample.csv',
        table_name='employees',
        chunk_size=1000
    )

if __name__ == "__main__":
    basic_example()

