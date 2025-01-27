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
    AWS_GLUE_DB,
    AWS_RPSCRAPE_TABLE_NAME,
    S3_BUCKET,
    S3_GLUE_PATH,
    boto3_session,
    COL_DTYPES,
    OUTPUT_COLS,
    SCHEMA_COLUMNS
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

def get_unprocessed_files(max_files: int = None) -> List[str]:
    """Get list of unprocessed CSV files from S3
    
    Args:
        max_files: Maximum number of files to return. If None, return all unprocessed files.
    """
    s3_client = boto3_session.client('s3')
    unprocessed = []
    stats = ProcessingStats()
    
    # Use paginator to avoid loading all files at once
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix='data/dates/'
    )
    
    for page in page_iterator:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            # Stop if we've reached max_files
            if max_files and len(unprocessed) >= max_files:
                logger.info(f"Reached maximum files limit: {max_files}")
                return unprocessed
                
            key = obj['Key']
            if not key.endswith('.csv'):
                continue
                
            try:
                # Check if file is already processed
                response = s3_client.head_object(
                    Bucket=S3_BUCKET,
                    Key=key
                )
                metadata = response.get('Metadata', {})
                if metadata.get('processed') == 'true':
                    continue
                    
                unprocessed.append(key)
                
            except s3_client.exceptions.ClientError as e:
                logger.error(f"Error checking file {key}: {str(e)}")
                stats.log_file_not_found(key)
                continue
    
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
            # Process each file in the batch
            dfs = []
            total_input_size = 0
            s3_client = boto3_session.client('s3')
            
            for file_path in batch:
                try:
                    # Get file size
                    response = s3_client.head_object(Bucket=S3_BUCKET, Key=file_path)
                    file_size = response['ContentLength']
                    total_input_size += file_size
                    
                    # Extract country from file path
                    country = file_path.split('/')[2]  # data/dates/gb/... -> gb
                    logger.info(f"Processing file for country: {country}, size: {file_size/1024:.2f}KB")
                    
                    # Read CSV file using awswrangler
                    df = wr.s3.read_csv(
                        path=f"s3://{S3_BUCKET}/{file_path}",
                        boto3_session=boto3_session
                    )
                    
                    # Add country and date
                    df['country'] = country
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # Replace '-' with None for numeric columns
                    numeric_cols = [col for col, dtype in SCHEMA_COLUMNS.items() 
                                 if dtype in ('int', 'double') and col in df.columns]
                    logger.info(f"Processing numeric columns for file {file_path}")
                    
                    for col in numeric_cols:
                        # Convert to string first to handle mixed types
                        df[col] = df[col].astype(str)
                        df[col] = df[col].replace('-', None)
                        
                        # Convert back to float first (handles both int and double)
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Convert data types
                    try:
                        for col, dtype in SCHEMA_COLUMNS.items():
                            if col in df.columns:
                                try:
                                    if dtype == 'int':
                                        df[col] = df[col].astype('Int64')  # Use nullable integer type
                                    elif dtype == 'double':
                                        df[col] = df[col].astype('float64')
                                    elif dtype == 'timestamp':
                                        df[col] = pd.to_datetime(df[col])
                                    else:
                                        df[col] = df[col].astype(str)
                                except Exception as e:
                                    logger.error(f"Error converting column {col} to type {dtype}: {str(e)}")
                                    raise
                    except Exception as e:
                        logger.error(f"Error during type conversion: {str(e)}")
                        raise
                    
                    dfs.append(df)
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {str(e)}")
                    continue
            
            if not dfs:
                logger.warning("No valid files in batch")
                continue
                
            # Combine all DataFrames
            df = pd.concat(dfs, ignore_index=True)
            
            # Ensure all required columns are present
            for col in OUTPUT_COLS:
                if col not in df.columns:
                    df[col] = None
            
            # Select and order columns
            df = df[OUTPUT_COLS]
            
            # Write to Glue table with date partitioning
            wr.s3.to_parquet(
                df=df,
                path=S3_GLUE_PATH,
                dataset=True,
                mode=mode,
                database=AWS_GLUE_DB,
                table=AWS_RPSCRAPE_TABLE_NAME,
                boto3_session=boto3_session,
                partition_cols=['country', 'date'],  # Partition by date components and country
                compression='snappy',  # Add compression for cost savings
                dtype=SCHEMA_COLUMNS
            )
            
            # Update stats
            stats.log_batch(
                num_files=len(dfs),
                num_rows=len(df),
                input_size=total_input_size,
                output_size=0  # We don't track output size for now
            )
            
            # Mark files as processed
            for file_path in batch:
                try:
                    mark_file_processed(file_path)
                except Exception as e:
                    logger.error(f"Error marking file {file_path} as processed: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            continue
        
        # Log progress
        summary = stats.get_summary()
        logger.info(f"Progress: {summary['files_processed']} files, {summary['rows_processed']} rows")
    
    # Log final statistics
    summary = stats.get_summary()
    logger.info("Processing completed! Summary:")
    logger.info(f"Files processed: {summary['files_processed']}")
    logger.info(f"Rows processed: {summary['rows_processed']}")
    logger.info(f"Input size: {summary['input_size_mb']} MB")
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
    """Main function to process new files from S3 to Glue
    
    Environment Variables:
        MODE: The mode to use when writing to Glue table. One of:
            - append: Add new data to existing table (default)
            - overwrite: Replace entire table with new data
            - overwrite_partitions: Replace specific partitions with new data
        BATCH_SIZE: Number of files to process in each batch (default: 200)
        MAX_FILES: Maximum number of files to process. If not set, process all files.
    """
    logger.info("Starting S3 to Glue processing...")
    
    # Get max files from environment
    max_files = os.getenv("MAX_FILES")
    if max_files:
        max_files = int(max_files)
        logger.info(f"Will process up to {max_files} files")
    
    # Get list of unprocessed files
    unprocessed_files = get_unprocessed_files(max_files=max_files)
    logger.info(f"Found {len(unprocessed_files)} unprocessed files")
    
    # Get batch size from environment
    batch_size = int(os.getenv("BATCH_SIZE", "200"))
    logger.info(f"Using batch size: {batch_size}")
    
    # Get and validate MODE from environment
    valid_modes = ["append", "overwrite", "overwrite_partitions"]
    mode = os.getenv("MODE", "append").lower()
    
    if mode not in valid_modes:
        logger.error(f"Invalid MODE '{mode}'. Must be one of: {', '.join(valid_modes)}")
        sys.exit(1)
    
    logger.info(f"Using mode: {mode}")
    
    # Process files in batches
    process_files(unprocessed_files, batch_size=batch_size, mode=mode)

if __name__ == "__main__":
    main()
