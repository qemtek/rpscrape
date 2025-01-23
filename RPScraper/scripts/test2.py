import awswrangler as wr
import boto3
import datetime as dt
import pandas as pd
import subprocess
from pathlib import Path

from settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

DATABASE = 'finish-time-predict'

boto3_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='eu-west-1'
)

def delete_data_in_range(start_date, end_date, countries=None):
    """
    Delete data within a specified date range for given countries
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
    # Delete existing data
    delete_data_in_range(start_date, end_date, countries)
    
    # Repopulate data
    repopulate_data(start_date, end_date, countries)

if __name__ == "__main__":
    # Specify date range for repopulation
    start_date = '2024-10-01'  # October 1st, 2024
    end_date = '2025-01-22'    # January 22nd, 2025
    
    refresh_data_for_range(
        start_date=start_date,
        end_date=end_date,
        countries=['gb', 'ire']
    )
