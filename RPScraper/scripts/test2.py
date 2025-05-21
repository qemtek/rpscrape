import awswrangler as wr
import boto3
import datetime as dt
import pandas as pd
import subprocess
import os
import glob
from pathlib import Path

from settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

DATABASE = 'finish-time-predict'

boto3_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='eu-west-1'
)

def delete_local_data_in_range(start_date, end_date, countries=None):
    """
    Delete local CSV files within a specified date range for given countries
    """
    if countries is None:
        countries = ['gb', 'ire']
    
    # Convert dates to datetime for comparison
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # Get the data directory
    data_dir = Path(__file__).parent.parent / 'data'
    
    for country in countries:
        # Find all CSV files for this country
        csv_files = glob.glob(str(data_dir / f"{country}*.csv"))
        for csv_file in csv_files:
            try:
                # Read the file
                df = pd.read_csv(csv_file)
                if 'date' not in df.columns:
                    print(f"Warning: No date column in {csv_file}")
                    continue
                
                # Convert date column to datetime
                df['date'] = pd.to_datetime(df['date'])
                
                # Filter out rows in the specified date range
                df = df[~((df['date'] >= start_dt) & (df['date'] <= end_dt))]
                
                if len(df) > 0:
                    # Save the filtered data back to the file
                    df.to_csv(csv_file, index=False)
                else:
                    # If no data left, delete the file
                    os.remove(csv_file)
                    print(f"Deleted empty file: {csv_file}")
                
            except Exception as e:
                print(f"Error processing {csv_file}: {str(e)}")
    
    print(f"Deleted local data between {start_date} and {end_date} for countries: {countries}")

def delete_data_in_range(start_date, end_date, countries=None):
    """
    Delete data within a specified date range for given countries from Athena
    """
    if countries is None:
        countries = ['gb', 'ire']
    
    countries_str = "'" + "','".join(countries) + "'"
    delete_query = f"""
    DELETE FROM rpscrape 
    WHERE date BETWEEN date('{start_date}') AND date('{end_date}')
    AND country IN ({countries_str})
    """
    
    response = wr.athena.start_query_execution(
        sql=delete_query,
        database=DATABASE,
        boto3_session=boto3_session
    )
    print(f"Deleted data between {start_date} and {end_date} for countries: {countries}")
    return response

def get_date_range(start_date, end_date):
    """
    Generate a list of dates between start_date and end_date
    """
    d1 = pd.to_datetime(start_date)
    d2 = pd.to_datetime(end_date)
    return [d1 + dt.timedelta(days=x) for x in range((d2-d1).days + 1)]

def run_rpscrape(country, date):
    """
    Run the rpscrape script for a specific country and date
    """
    subprocess.call(f'cd RPScraper && PYTHONPATH=. python3 scripts/rpscrape.py -d {date} -r {country}', shell=True)
    print(f'Finished scraping {country} - {date}')

def repopulate_data(start_date, end_date, countries=None):
    """
    Repopulate data for specified date range and countries
    """
    if countries is None:
        countries = ['gb', 'ire']
    
    dates = get_date_range(start_date, end_date)
    
    for country in countries:
        for date in dates:
            formatted_date = str(date.date()).replace('-', '/')
            run_rpscrape(country, formatted_date)

def refresh_data_for_range(start_date, end_date, countries=None):
    """
    Main function to delete and repopulate data for a date range
    """
    # Delete existing data locally and in Athena
    delete_local_data_in_range(start_date, end_date, countries)
    delete_data_in_range(start_date, end_date, countries)
    
    # Repopulate data
    repopulate_data(start_date, end_date, countries)

if __name__ == "__main__":
    # Specify date range for repopulation
    start_date = '2025-03-16'
    end_date = '2025-05-16'
    
    refresh_data_for_range(
        start_date=start_date,
        end_date=end_date,
        countries=['gb', 'ire', 'fr']
    )
