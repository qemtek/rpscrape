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


def process_yesterday_data():
    """Process yesterday's data for all countries"""
    # Get yesterday's date in the format YYYY/MM/DD
    yesterday_slash = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y/%m/%d')
    # Convert to the format used in filenames (YYYY_MM_DD)
    yesterday_underscore = yesterday_slash.replace('/', '_')
    
    countries = ['gb', 'ire']  # Same countries as in run_daily_updates.sh
    
    print(f"Processing data for date: {yesterday_slash}")
    
    for country in countries:
        local_file_path = f"{PROJECT_DIR}/data/dates/{country}/{yesterday_underscore}.csv"
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        if os.path.exists(local_file_path):
            print(f"Uploading file for {country} - {yesterday_slash}")
            upload_to_s3(local_file_path, country)
        else:
            print(f"File does not exist for {country} - {yesterday_slash}: {local_file_path}")


if __name__ == "__main__":
    process_yesterday_data()
