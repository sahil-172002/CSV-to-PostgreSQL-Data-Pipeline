from src.config import DB_CONFIG
from src.database import DatabaseConnection
from src.pipeline import DataPipeline, PerformanceMonitor
import logging
import argparse
import os

def parse_arguments():
    parser = argparse.ArgumentParser(description='CSV to PostgreSQL Data Pipeline')
    parser.add_argument('--file', type=str, required=True, help='Path to CSV file')
    parser.add_argument('--table', type=str, required=True, help='Target table name')
    parser.add_argument('--chunk-size', type=int, default=1000, help='Chunk size for processing')
    return parser.parse_args()

def main():
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Validate file exists
        if not os.path.exists(args.file):
            logging.error(f"File not found: {args.file}")
            return
        
        # Initialize database connection
        db = DatabaseConnection(DB_CONFIG)
        
        # Create pipeline
        pipeline = DataPipeline(db)
        
        # Create performance monitor
        monitor = PerformanceMonitor()
        
        # Process CSV file
        logging.info(f"Starting CSV import from {args.file} to table {args.table}")
        
        metrics = pipeline.process_csv(
            file_path=args.file,
            table_name=args.table,
            chunk_size=args.chunk_size,
            monitor=monitor
        )
        
        logging.info(f"Import summary:")
        logging.info(f"- Total records: {metrics['records_processed']}")
        logging.info(f"- Total time: {metrics['elapsed_time']:.2f} seconds")
        logging.info(f"- Processing speed: {metrics['records_per_second']:.2f} records/second")
        
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

