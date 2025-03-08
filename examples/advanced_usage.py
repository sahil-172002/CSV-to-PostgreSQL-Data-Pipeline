from src.config import DB_CONFIG
from src.database import DatabaseConnection
from src.pipeline import DataPipeline, PerformanceMonitor

def custom_validation(df):
    """Custom validation function"""
    # Remove rows with missing required fields
    df = df.dropna(subset=['employee_id', 'email'])
    
    # Validate email format
    df = df[df['email'].str.contains('@', na=False)]
    
    return df

def advanced_example():
    """Advanced example with custom validation and performance monitoring"""
    # Initialize database connection
    db = DatabaseConnection(DB_CONFIG)
    
    # Create pipeline
    pipeline = DataPipeline(db)
    
    # Create performance monitor
    monitor = PerformanceMonitor()
    
    # Process CSV file with custom validation
    metrics = pipeline.process_csv(
        file_path='data/sample.csv',
        table_name='employees',
        chunk_size=1000,
        validation_func=custom_validation,
        monitor=monitor
    )
    
    # Print performance metrics
    print(f"Processed {metrics['records_processed']} records in {metrics['elapsed_time']:.2f} seconds")
    print(f"Average speed: {metrics['records_per_second']:.2f} records/second")

if __name__ == "__main__":
    advanced_example()

