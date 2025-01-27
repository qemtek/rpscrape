#!/usr/bin/env python3
"""
Process racing data from S3 CSV files to AWS Glue table.
Uses AWS Wrangler for efficient S3 to Glue processing.
"""

import os
import sys
import datetime as dt
import awswrangler as wr
import pandas as pd
from typing import List, Dict
import boto3
import logging
from datetime import datetime
import botocore
from typing import Literal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from settings import (
    S3_BUCKET, 
    AWS_GLUE_DB, 
    AWS_RPSCRAPE_TABLE_NAME, 
    SCHEMA_COLUMNS,
    OUTPUT_COLS,
    boto3_session,
    COL_DTYPES
)

class ProcessingStats:
    def __init__(self):
        self.files_processed = 0
        self.rows_processed = 0
        self.input_bytes = 0
        self.output_bytes = 0
        self.start_time = datetime.now()
        self.files_not_found = []
    
    def log_batch(self, num_files: int, num_rows: int, input_size: int, output_size: int):
        self.files_processed += num_files
        self.rows_processed += num_rows
        self.input_bytes += input_size
        self.output_bytes += output_size
    
    def log_file_not_found(self, file_path: str):
        self.files_not_found.append(file_path)
    
    def get_summary(self) -> Dict:
        duration = (datetime.now() - self.start_time).total_seconds()
        return {
            'files_processed': self.files_processed,
            'rows_processed': self.rows_processed,
            'input_size_mb': round(self.input_bytes / (1024 * 1024), 2),
            'output_size_mb': round(self.output_bytes / (1024 * 1024), 2),
            'compression_ratio': round(self.output_bytes / self.input_bytes, 2) if self.input_bytes > 0 else 0,
            'duration_seconds': round(duration, 2),
            'rows_per_second': round(self.rows_processed / duration, 2) if duration > 0 else 0,
            'files_not_found': len(self.files_not_found)
        }

def get_unprocessed_files() -> List[str]:
    """Get list of unprocessed CSV files from S3"""
    s3_client = boto3_session.client('s3')
    stats = ProcessingStats()
    
    # List all CSV files in the data/dates prefix
    try:
        all_files = wr.s3.list_objects(f"s3://{S3_BUCKET}/data/dates/", suffix='.csv', boto3_session=boto3_session)
        logger.info(f"Found {len(all_files)} total CSV files")
    except Exception as e:
        logger.error(f"Error listing objects: {str(e)}")
        return []
    
    unprocessed = []
    for file_path in all_files:
        try:
            # Extract just the key part (remove s3://bucket/ prefix if present)
            key = file_path.replace(f"s3://{S3_BUCKET}/", "") if file_path.startswith("s3://") else file_path
            
            # Check if file has been processed by looking at its metadata
            response = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
            metadata = response.get('Metadata', {})
            if 'processed' not in metadata:
                unprocessed.append(key)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                stats.log_file_not_found(file_path)
                logger.warning(f"File not found: {file_path}")
            else:
                logger.error(f"Error checking file {file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error checking file {file_path}: {str(e)}")
    
    if stats.files_not_found:
        logger.warning(f"Total files not found: {len(stats.files_not_found)}")
        logger.warning("First few missing files:")
        for file_path in stats.files_not_found[:5]:
            logger.warning(f"  - {file_path}")
    
    return unprocessed

def process_files(file_paths: List[str], batch_size: int = 200, mode: Literal["append", "overwrite", "overwrite_partitions"]="append"):
    """Process CSV files from S3 and write to Glue table"""
    if not file_paths:
        logger.info("No new files to process")
        return
    
    stats = ProcessingStats()
    logger.info(f"Processing {len(file_paths)} files in batches of {batch_size}")
    
    # Process files in batches to manage memory
    for i in range(0, len(file_paths), batch_size):
        batch = file_paths[i:i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(file_paths) + batch_size - 1)//batch_size}")
        
        try:
            # Get input file sizes
            s3_client = boto3_session.client('s3')
            input_size = sum(
                s3_client.head_object(Bucket=S3_BUCKET, Key=file_path)['ContentLength']
                for file_path in batch
            )
            
            # Extract country from file paths
            countries = [path.split('/')[2] for path in batch]  # data/dates/gb/... -> gb
            logger.info(f"Processing files for countries: {set(countries)}")
            
            # Read batch of CSV files directly from S3
            df = wr.s3.read_csv(
                path=[f"s3://{S3_BUCKET}/{file_path}" for file_path in batch],
                boto3_session=boto3_session,
            )
            
            # Add country based on file path
            df['country'] = pd.Series(countries * (len(df) // len(batch) + 1))[:len(df)]
            
            # Convert date strings to timestamps and extract date parts
            df['date'] = pd.to_datetime(df['date'])
            
            # Add created_at timestamp in UTC
            df['created_at'] = dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            
            # Ensure all required columns are present
            for col in OUTPUT_COLS:
                if col not in df.columns:
                    df[col] = None
            
            # Select and order columns
            df = df[OUTPUT_COLS]
            
            # Write to Glue table with date partitioning
            output_path = f"s3://{S3_BUCKET}/glue_tables/{AWS_RPSCRAPE_TABLE_NAME}/"
            wr.s3.to_parquet(
                df=df,
                path=output_path,
                dataset=True,
                mode=mode,
                database=AWS_GLUE_DB,
                table=AWS_RPSCRAPE_TABLE_NAME,
                boto3_session=boto3_session,
                partition_cols=['country', 'date'],  # Partition by date components and country
                compression='snappy',  # Add compression for cost savings
                dtype=COL_DTYPES
            )
            
            # Get output size (approximate since we can't easily get the exact parquet size)
            output_size = df.memory_usage(deep=True).sum()
            
            # Update statistics
            stats.log_batch(
                num_files=len(batch),
                num_rows=len(df),
                input_size=input_size,
                output_size=output_size
            )
            
            # Mark files as processed
            for file_path in batch:
                mark_file_processed(file_path)
            
            logger.info(f"Successfully processed batch: {len(df)} rows from {len(batch)} files")
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}", exc_info=True)
            continue
    
    # Log final statistics
    summary = stats.get_summary()
    logger.info("Processing completed! Summary:")
    logger.info(f"Files processed: {summary['files_processed']}")
    logger.info(f"Rows processed: {summary['rows_processed']}")
    logger.info(f"Input size: {summary['input_size_mb']} MB")
    logger.info(f"Output size: {summary['output_size_mb']} MB")
    logger.info(f"Compression ratio: {summary['compression_ratio']}")
    logger.info(f"Duration: {summary['duration_seconds']} seconds")
    logger.info(f"Processing speed: {summary['rows_per_second']} rows/second")
    logger.info(f"Files not found: {summary['files_not_found']}")

def mark_file_processed(file_path: str):
    """Mark file as processed using S3 object metadata"""
    s3_client = boto3_session.client('s3')
    try:
        # Copy object to itself with new metadata (S3 doesn't allow direct metadata update)
        s3_client.copy_object(
            Bucket=S3_BUCKET,
            CopySource={'Bucket': S3_BUCKET, 'Key': file_path},
            Key=file_path,
            Metadata={'processed': 'true', 'processed_at': dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')},
            MetadataDirective='REPLACE'
        )
    except Exception as e:
        logger.error(f"Error marking file {file_path} as processed: {str(e)}")

def main():
    """Main function to process new files from S3 to Glue"""
    logger.info("Starting S3 to Glue processing...")
    
    # Get list of unprocessed files
    unprocessed_files = get_unprocessed_files()
    logger.info(f"Found {len(unprocessed_files)} unprocessed files")
    MODE = os.getenv("MODE", "append")
    
    # Process files in batches
    process_files(unprocessed_files, mode=MODE)

if __name__ == "__main__":
    main()
