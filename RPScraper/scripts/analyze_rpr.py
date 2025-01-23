import awswrangler as wr
import boto3
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import sys
from pathlib import Path

# Add parent directory to path to import settings
sys.path.append(str(Path(__file__).parent.parent.parent))
from RPScraper.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_GLUE_DB

DATABASE = AWS_GLUE_DB

boto3_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='eu-west-1'
)

def analyze_missing_rpr(start_date='2024-07-01', end_date='2025-01-23'):
    """
    Analyze missing RPR values by month between specified dates
    """
    query = f"""
    WITH monthly_stats AS (
        SELECT 
            date_trunc('month', date) as month,
            COUNT(*) as total_records,
            COUNT(CASE WHEN rpr IS NULL OR CAST(rpr as VARCHAR) = '' THEN 1 END) as missing_rpr
        FROM rpscrape
        WHERE date BETWEEN date('{start_date}') AND date('{end_date}')
        GROUP BY date_trunc('month', date)
        ORDER BY month
    )
    SELECT 
        month,
        total_records,
        missing_rpr,
        ROUND(CAST(missing_rpr AS DOUBLE) / NULLIF(total_records, 0) * 100, 2) as missing_percentage
    FROM monthly_stats
    """
    
    df = wr.athena.read_sql_query(
        query,
        database=DATABASE,
        boto3_session=boto3_session
    )
    
    # Format the results
    df['month'] = pd.to_datetime(df['month']).dt.strftime('%Y-%m')
    
    # Print tabular results
    print("\nMissing RPR Analysis by Month:")
    print("=" * 80)
    print(f"{'Month':<10} {'Total Records':<15} {'Missing RPR':<15} {'Missing %':<10}")
    print("-" * 80)
    for _, row in df.iterrows():
        print(f"{row['month']:<10} {row['total_records']:<15} {row['missing_rpr']:<15} {row['missing_percentage']:.2f}%")
    
    # Create a bar plot
    plt.figure(figsize=(12, 6))
    plt.bar(df['month'], df['missing_percentage'])
    plt.title('Percentage of Missing RPR Values by Month')
    plt.xlabel('Month')
    plt.ylabel('Missing RPR (%)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('missing_rpr_analysis.png')
    print("\nPlot saved as 'missing_rpr_analysis.png'")
    
    return df

def analyze_daily_rpr(start_date='2025-01-01', end_date='2025-01-23'):
    """
    Analyze missing RPR values by day between specified dates
    """
    query = f"""
    WITH daily_stats AS (
        SELECT 
            date,
            COUNT(*) as total_records,
            COUNT(CASE WHEN rpr IS NULL OR CAST(rpr as VARCHAR) = '' THEN 1 END) as missing_rpr
        FROM rpscrape
        WHERE date BETWEEN date('{start_date}') AND date('{end_date}')
        GROUP BY date
        ORDER BY date
    )
    SELECT 
        date,
        total_records,
        missing_rpr,
        ROUND(CAST(missing_rpr AS DOUBLE) / NULLIF(total_records, 0) * 100, 2) as missing_percentage
    FROM daily_stats
    """
    
    df = wr.athena.read_sql_query(
        query,
        database=DATABASE,
        boto3_session=boto3_session
    )
    
    # Format the results
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Print tabular results
    print("\nMissing RPR Analysis by Day (2025):")
    print("=" * 80)
    print(f"{'Date':<12} {'Total Records':<15} {'Missing RPR':<15} {'Missing %':<10}")
    print("-" * 80)
    for _, row in df.iterrows():
        print(f"{row['date']:<12} {row['total_records']:<15} {row['missing_rpr']:<15} {row['missing_percentage']:.2f}%")
    
    # Create a bar plot
    plt.figure(figsize=(15, 6))
    plt.bar(df['date'], df['missing_percentage'])
    plt.title('Percentage of Missing RPR Values by Day (2025)')
    plt.xlabel('Date')
    plt.ylabel('Missing RPR (%)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('missing_rpr_daily_2025.png')
    print("\nPlot saved as 'missing_rpr_daily_2025.png'")
    
    return df

if __name__ == "__main__":
    # Run monthly analysis
    analyze_missing_rpr()
    
    # Run daily analysis for 2025
    analyze_daily_rpr()
