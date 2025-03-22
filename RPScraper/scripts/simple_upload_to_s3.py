#!/usr/bin/env python3
# Simple upload script that matches the processing in full_refresh.py

import os
import pandas as pd
import awswrangler as wr
import datetime as dt
from pathlib import Path

from settings import PROJECT_DIR, boto3_session, S3_BUCKET


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
        return True
    except Exception as e:
        print(f"Error uploading {local_path} to S3: {str(e)}")
        return False


def process_all_data():
    """Process all data files in the data/dates directory"""
    countries = ['gb', 'ire', 'fr']  # Add any other countries you want to process
    
    print(f"Processing all data files")
    
    for country in countries:
        data_dir = f"{PROJECT_DIR}/data/dates/{country}"
        
        # Ensure the directory exists
        if not os.path.exists(data_dir):
            print(f"Directory does not exist: {data_dir}")
            continue
            
        # Get all CSV files in the directory
        files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        
        if not files:
            print(f"No CSV files found for {country}")
            continue
            
        print(f"Found {len(files)} files for {country}")
        
        # Upload each file
        for filename in files:
            local_file_path = os.path.join(data_dir, filename)
            print(f"Uploading file for {country} - {filename}")
            upload_to_s3(local_file_path, country)


if __name__ == "__main__":
    process_all_data()
