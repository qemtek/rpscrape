# Downloads all files from rpscrape and stores them in the data folder and S3

import datetime as dt
import subprocess
import awswrangler as wr
import pandas as pd
import os
from pathlib import Path
from collections import defaultdict

from settings import PROJECT_DIR, boto3_session, S3_BUCKET


def run_rpscrape(country, date):
    try:
        subprocess.call(f'PYTHONPATH=/app/RPScraper python /app/RPScraper/scripts/rpscrape.py -d {date} -r {country}', shell=True)
    except EOFError:
        pass


def get_existing_s3_files():
    """Get all existing files in S3 bucket organized by country"""
    existing_files = defaultdict(set)
    try:
        # List all objects in the data prefix
        all_files = wr.s3.list_objects(f"s3://{S3_BUCKET}/data/", boto3_session=boto3_session)
        
        # Organize files by country
        for s3_key in all_files:
            if s3_key.startswith('data/'):
                parts = s3_key.split('/')
                if len(parts) == 3:  # data/country/filename
                    country = parts[1]
                    filename = parts[2]
                    existing_files[country].add(filename)
        
        print(f"Found existing files in S3 for countries: {list(existing_files.keys())}")
        for country, files in existing_files.items():
            print(f"{country}: {len(files)} files")
            
    except Exception as e:
        print(f"Error listing S3 files: {str(e)}")
    
    return existing_files


def upload_to_s3(local_path, country):
    """Upload file to S3"""
    if not os.path.exists(local_path):
        return
        
    s3_key = f"data/{country}/{os.path.basename(local_path)}"
    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    
    try:
        print(f"Uploading {local_path} to {s3_path}")
        wr.s3.upload(local_path=local_path, path=s3_path, boto3_session=boto3_session)
    except Exception as e:
        print(f"Error uploading {local_path} to S3: {str(e)}")


if __name__ == "__main__":
    # Get configuration from environment variables
    date_today = dt.datetime.today().date()
    start_date = pd.to_datetime(os.getenv('START_DATE', '2008-05-28')).date()
    end_date = pd.to_datetime(os.getenv('END_DATE', (date_today - dt.timedelta(days=1)).strftime('%Y-%m-%d'))).date()
    countries = os.getenv('COUNTRIES', 'gb,ire').lower().split(',')
    force = os.getenv('FORCE', '').lower() in ('true', '1', 'yes')  # Default to False

    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")
    print(f"Countries: {countries}")
    print(f"Force mode: {'enabled' if force else 'disabled'}")

    # Get existing files from S3 at startup
    existing_s3_files = get_existing_s3_files() if not force else defaultdict(set)

    # Find the number of days between the start and end dates
    delta = end_date - start_date

    for country in countries:
        country = country.strip()  # Remove any whitespace
        country_files = existing_s3_files.get(country, set())
        print(f"\nProcessing {country} - {len(country_files)} existing files found")
        
        for i in range(delta.days + 1):
            day = (start_date + dt.timedelta(days=i)).strftime(format='%Y/%m/%d')
            filename = f"{str(day).replace('/', '_')}.csv"
            local_file_path = f"{PROJECT_DIR}/data/{country}/{filename}"
            
            if i % 100 == 0:
                print(f"Processing {country} - {day}")
                
            try:
                # Check if file already exists in S3 (skip if force is enabled)
                if not force and filename in country_files:
                    if i % 100 == 0:  # Only print skip message occasionally to reduce output
                        print(f"File already exists in S3 for {country} - {day}, skipping...")
                    continue
                    
                # Ensure the directory exists
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                
                # Run scraper
                run_rpscrape(country, day)
                
                # Upload to S3
                upload_to_s3(local_file_path, country)
                
            except Exception as e:
                print(f"Couldn't process data for {country} on {day}: {str(e)}")
