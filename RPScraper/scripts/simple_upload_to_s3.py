#!/usr/bin/env python3
# Simple upload script that matches the processing in full_refresh.py

import os
import pandas as pd
import awswrangler as wr
import datetime as dt
from pathlib import Path
import logging
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Literal

from settings import (
    PROJECT_DIR, 
    boto3_session, 
    S3_BUCKET, 
    AWS_GLUE_DB,
    AWS_RPSCRAPE_TABLE_NAME,
    S3_GLUE_PATH,
    COL_DTYPES,
    OUTPUT_COLS,
    SCHEMA_COLUMNS
)
from utils.general import clean_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def upload_to_s3(local_path, country):
    """Upload file to S3 using the same logic as full_refresh.py"""
    if not os.path.exists(local_path):
        return False
        
    s3_key = f"data/dates/{country}/{os.path.basename(local_path)}"
    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    
    try:
        df = pd.read_csv(local_path)
        df['created_at'] = pd.Timestamp.now()
        wr.s3.to_csv(df, s3_path, index=False, boto3_session=boto3_session)
        print(f"Uploaded {local_path} to {s3_path}")
        return s3_key, df
    except Exception as e:
        print(f"Error uploading {local_path} to S3: {str(e)}")
        return None, None


def process_dataframe_for_glue(df, country):
    """Process dataframe to prepare it for Glue using the same logic as process_s3_to_glue.py"""
    if df is None or df.empty:
        return None
    
    try:
        # Add country and date
        df['country'] = country
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].apply(lambda x: x.year)
        df['pos'] = df['pos'].astype(str)
        df['pattern'] = df['pattern'].astype(str)
        df['prize'] = df['prize'].astype(str)
        df['created_at'] = datetime.now(timezone.utc).isoformat()

        # Clean data using the same function as process_s3_to_glue.py
        df = clean_data(df, country=country)

        # Replace '-' with None for numeric columns
        numeric_cols = [col for col, dtype in SCHEMA_COLUMNS.items() 
                     if dtype in ('int', 'double') and col in df.columns]
        
        for col in numeric_cols:
            # Convert to string first to handle mixed types
            df[col] = df[col].astype(str)
            df[col] = df[col].replace('-', None)
            # Convert back to float first (handles both int and double)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Convert data types
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
        
        # Ensure all required columns are present
        for col in OUTPUT_COLS:
            if col not in df.columns:
                df[col] = None
        
        # Select and order columns
        df = df[OUTPUT_COLS]
        
        return df
    
    except Exception as e:
        logger.error(f"Error processing dataframe: {str(e)}")
        return None


def upload_to_glue(dataframes, mode="append"):
    """Upload processed dataframes to Glue"""
    if not dataframes or all(df is None for df in dataframes):
        logger.warning("No valid dataframes to upload to Glue")
        return False
    
    # Filter out None values and empty dataframes
    valid_dfs = [df for df in dataframes if df is not None and not df.empty]
    
    if not valid_dfs:
        logger.warning("No valid dataframes to upload to Glue")
        return False
    
    try:
        # Combine all DataFrames
        combined_df = pd.concat(valid_dfs, ignore_index=True)
        
        # Deduplicate based on race_id and horse_id, keeping the latest version
        initial_len = len(combined_df)
        combined_df = combined_df.loc[combined_df[['race_id', 'horse_id']].drop_duplicates(keep='last').index, :].reset_index(drop=True)
        if len(combined_df) != initial_len:
            logger.warning(f"Removed {initial_len - len(combined_df)} duplicate rows based on race_id and horse_id")
        
        # Write to Glue table with date partitioning
        wr.s3.to_parquet(
            df=combined_df,
            path=S3_GLUE_PATH,
            dataset=True,
            mode=mode,
            database=AWS_GLUE_DB,
            table=AWS_RPSCRAPE_TABLE_NAME,
            partition_cols=['country', 'date'],
            boto3_session=boto3_session,
            compression='snappy',
            dtype=SCHEMA_COLUMNS
        )
        
        logger.info(f"Successfully uploaded {len(combined_df)} rows to Glue table {AWS_RPSCRAPE_TABLE_NAME}")
        return True
    
    except Exception as e:
        logger.error(f"Error uploading to Glue: {str(e)}")
        return False


def mark_file_processed(file_path):
    """Mark file as processed using S3 object metadata"""
    try:
        s3_client = boto3_session.client('s3')
        s3_client.copy_object(
            Bucket=S3_BUCKET,
            CopySource={'Bucket': S3_BUCKET, 'Key': file_path},
            Key=file_path,
            Metadata={'processed': 'true'},
            MetadataDirective='REPLACE'
        )
        logger.info(f"Marked file as processed: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error marking file as processed: {str(e)}")
        return False


def process_all_data(mode="append"):
    """Process all data files in the data/dates directory and upload to S3 and Glue"""
    countries = ['gb', 'ire', 'fr']  # Add any other countries you want to process
    
    logger.info(f"Processing all data files with mode={mode}")
    
    s3_keys = []
    dataframes = []
    
    for country in countries:
        data_dir = f"{PROJECT_DIR}/data/dates/{country}"
        
        # Ensure the directory exists
        if not os.path.exists(data_dir):
            logger.warning(f"Directory does not exist: {data_dir}")
            continue
            
        # Get all CSV files in the directory
        files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        
        if not files:
            logger.warning(f"No CSV files found for {country}")
            continue
            
        logger.info(f"Found {len(files)} files for {country}")
        
        # Upload each file to S3 and prepare for Glue
        for filename in files:
            local_file_path = os.path.join(data_dir, filename)
            logger.info(f"Uploading file for {country} - {filename}")
            
            # Upload to S3
            s3_key, df = upload_to_s3(local_file_path, country)
            
            if s3_key:
                s3_keys.append(s3_key)
                
                # Process dataframe for Glue
                processed_df = process_dataframe_for_glue(df, country)
                if processed_df is not None:
                    dataframes.append(processed_df)
    
    # Upload processed dataframes to Glue
    if dataframes:
        logger.info(f"Uploading {len(dataframes)} processed dataframes to Glue")
        success = upload_to_glue(dataframes, mode=mode)
        
        # Mark files as processed if upload was successful
        if success:
            for s3_key in s3_keys:
                mark_file_processed(s3_key)
    else:
        logger.warning("No dataframes to upload to Glue")


if __name__ == "__main__":
    # Get mode from environment variable or use default
    mode = os.getenv("MODE", "overwrite_partitions").lower()
    valid_modes = ["append", "overwrite", "overwrite_partitions"]
    
    if mode not in valid_modes:
        logger.error(f"Invalid MODE '{mode}'. Must be one of: {', '.join(valid_modes)}")
        mode = "append"
        logger.info(f"Using default mode: {mode}")
    
    process_all_data(mode=mode)
