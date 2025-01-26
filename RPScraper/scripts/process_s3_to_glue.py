#!/usr/bin/env python3
"""
Process racing data from S3 CSV files to AWS Glue table.
Uses AWS Wrangler for efficient S3 to Glue processing.
"""

import os
import datetime as dt
import awswrangler as wr
import pandas as pd
from typing import List
import boto3

from settings import (
    S3_BUCKET, 
    AWS_GLUE_DB, 
    AWS_RPSCRAPE_TABLE_NAME, 
    SCHEMA_COLUMNS,
    OUTPUT_COLS,
    boto3_session
)

def get_unprocessed_files() -> List[str]:
    """Get list of unprocessed CSV files from S3"""
    s3_client = boto3_session.client('s3')
    
    # List all CSV files in the raw_data prefix
    all_files = wr.s3.list_objects(f"s3://{S3_BUCKET}/raw_data/", suffix='.csv', boto3_session=boto3_session)
    unprocessed = []
    
    for file_path in all_files:
        try:
            # Check if file has been processed by looking at its metadata
            response = s3_client.head_object(Bucket=S3_BUCKET, Key=file_path)
            metadata = response.get('Metadata', {})
            if 'processed' not in metadata:
                unprocessed.append(file_path)
        except Exception as e:
            print(f"Error checking file {file_path}: {str(e)}")
            continue
    
    return unprocessed

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
        print(f"Error marking file {file_path} as processed: {str(e)}")

def process_files(file_paths: List[str], batch_size: int = 50):
    """Process CSV files from S3 and write to Glue table"""
    if not file_paths:
        print("No new files to process")
        return
    
    print(f"Processing {len(file_paths)} files in batches of {batch_size}")
    
    # Process files in batches to manage memory
    for i in range(0, len(file_paths), batch_size):
        batch = file_paths[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(file_paths) + batch_size - 1)//batch_size}")
        
        try:
            # Read batch of CSV files directly from S3
            df = wr.s3.read_csv(
                path=batch,
                boto3_session=boto3_session,
                dtype=SCHEMA_COLUMNS
            )
            
            # Add created_at timestamp in UTC
            df['created_at'] = dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            
            # Ensure all required columns are present
            for col in OUTPUT_COLS:
                if col not in df.columns:
                    df[col] = None
            
            # Select and order columns
            df = df[OUTPUT_COLS]
            
            # Write to Glue table
            wr.s3.to_parquet(
                df=df,
                path=f"s3://{S3_BUCKET}/glue_tables/{AWS_RPSCRAPE_TABLE_NAME}/",
                dataset=True,
                mode='append',
                database=AWS_GLUE_DB,
                table=AWS_RPSCRAPE_TABLE_NAME,
                boto3_session=boto3_session
            )
            
            # Mark files as processed
            for file_path in batch:
                mark_file_processed(file_path)
            
            print(f"Successfully processed and marked {len(batch)} files")
            
        except Exception as e:
            print(f"Error processing batch: {str(e)}")
            # Continue with next batch instead of failing completely
            continue

def main():
    """Main function to process new files from S3 to Glue"""
    # Get unprocessed files
    unprocessed_files = get_unprocessed_files()
    print(f"Found {len(unprocessed_files)} unprocessed files")
    
    # Process files
    process_files(unprocessed_files)

if __name__ == "__main__":
    main()
