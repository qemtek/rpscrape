import awswrangler as wr
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import sys
from pathlib import Path
import boto3

# Add parent directory to path to import settings
sys.path.append(str(Path(__file__).parent.parent.parent))
from RPScraper.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

DATABASE = 'finish-time-predict'

boto3_session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='eu-west-1'
)

def check_duplicates():
    print("\nChecking for duplicates...")
    query = """
    WITH duplicate_check AS (
        SELECT 
            date,
            race_id,
            horse_id,
            COUNT(*) as count
        FROM rpscrape
        WHERE date >= DATE('2020-01-01') 
        AND date <= DATE('2025-01-22')
        GROUP BY date, race_id, horse_id
        HAVING COUNT(*) > 1
    )
    SELECT 
        date_format(date, '%Y-%m-%d') as race_date,
        race_id,
        horse_id,
        count as duplicate_count
    FROM duplicate_check
    ORDER BY date, count DESC;
    """
    
    df = wr.athena.read_sql_query(
        sql=query,
        database=DATABASE,
        boto3_session=boto3_session
    )
    
    if df.empty:
        print("No duplicates found!")
    else:
        print("\nFound duplicates:")
        print("=" * 80)
        print(f"{'Date':<12} {'Race ID':<20} {'Horse ID':<20} {'Count':<10}")
        print("-" * 80)
        for _, row in df.iterrows():
            print(f"{row['race_date']:<12} {row['race_id']:<20} {row['horse_id']:<20} {row['duplicate_count']:<10}")
        
        # Get total number of duplicates
        total_duplicates = df['duplicate_count'].sum() - len(df)  # Subtract original entries
        print(f"\nTotal duplicate entries: {total_duplicates}")

def analyze_missing_rpr():
    """
    Analyze missing RPR values by day between specified dates
    """
    query = """
    WITH daily_stats AS (
        SELECT 
            date_format(date, '%Y-%m-%d') as race_date,
            COUNT(*) as total_races,
            COUNT(CASE WHEN rpr IS NULL OR CAST(rpr AS VARCHAR) = '' THEN 1 END) as missing_rpr
        FROM rpscrape
        WHERE date >= DATE('2020-01-01') 
        AND date <= DATE('2025-01-22')
        GROUP BY date_format(date, '%Y-%m-%d')
        ORDER BY race_date
    )
    SELECT 
        race_date,
        total_races,
        missing_rpr,
        ROUND(CAST(missing_rpr AS DOUBLE) / CAST(total_races AS DOUBLE) * 100, 2) as missing_percentage
    FROM daily_stats
    ORDER BY race_date;
    """
    
    print("Executing query to analyze RPR values...")
    df = wr.athena.read_sql_query(
        sql=query,
        database=DATABASE,
        boto3_session=boto3_session
    )
    
    # Display results
    print("\nRPR Analysis Results:")
    print("=" * 80)
    print(f"{'Date':<12} {'Total Races':<15} {'Missing RPR':<15} {'Missing %':<10}")
    print("-" * 80)
    
    for _, row in df.iterrows():
        print(f"{row['race_date']:<12} {row['total_races']:<15} {row['missing_rpr']:<15} {row['missing_percentage']:.2f}%")
    
    # Calculate overall statistics
    total_races = df['total_races'].sum()
    total_missing = df['missing_rpr'].sum()
    overall_missing_pct = (total_missing / total_races) * 100
    
    print("\nOverall Statistics:")
    print(f"Total Races: {total_races}")
    print(f"Total Missing RPR: {total_missing}")
    print(f"Overall Missing Percentage: {overall_missing_pct:.2f}%")

if __name__ == "__main__":
    # First check if there's any data
    print("Checking for data in the table...")
    query = "SELECT COUNT(*) as count FROM rpscrape"
    df = wr.athena.read_sql_query(
        sql=query,
        database=DATABASE,
        boto3_session=boto3_session
    )
    print(f"Total rows in table: {df['count'].iloc[0]}")
    
    check_duplicates()
    analyze_missing_rpr()
