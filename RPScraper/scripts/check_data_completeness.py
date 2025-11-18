#!/usr/bin/env python3
"""
Check data completeness by analyzing race counts per day.

This script identifies dates with suspiciously low race counts that might
indicate scraping failures. It uses statistical analysis to detect anomalies.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging
import pandas as pd

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from settings import boto3_session, AWS_GLUE_DB
import awswrangler as wr

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def analyze_race_counts(start_date: str, end_date: str, country: str = 'gb'):
    """
    Analyze race counts per day to identify potential missing data.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        country: Country code
    """

    query = f"""
    SELECT
        date,
        COUNT(DISTINCT race_id) as race_count,
        COUNT(*) as runner_count,
        MIN(created_at) as first_created,
        MAX(created_at) as last_created
    FROM rpscrape
    WHERE date >= DATE('{start_date}')
        AND date <= DATE('{end_date}')
        AND country = '{country}'
    GROUP BY date
    ORDER BY date ASC
    """

    logger.info(f'Analyzing data from {start_date} to {end_date} for {country.upper()}...')

    try:
        df = wr.athena.read_sql_query(query, database=AWS_GLUE_DB, boto3_session=boto3_session)

        if df.empty:
            logger.warning(f'No data found for {country} in date range {start_date} to {end_date}')
            return

        # Calculate statistics
        mean_races = df['race_count'].mean()
        std_races = df['race_count'].std()
        median_races = df['race_count'].median()

        logger.info(f'\nRace Count Statistics:')
        logger.info(f'  Mean: {mean_races:.1f}')
        logger.info(f'  Median: {median_races:.0f}')
        logger.info(f'  Std Dev: {std_races:.1f}')
        logger.info(f'  Min: {df["race_count"].min()}')
        logger.info(f'  Max: {df["race_count"].max()}')

        # Flag suspicious dates (less than mean - 2*std)
        threshold = max(5, mean_races - 2 * std_races)
        suspicious = df[df['race_count'] < threshold]

        if not suspicious.empty:
            logger.warning(f'\n{"="*80}')
            logger.warning(f'SUSPICIOUS DATES (< {threshold:.0f} races):')
            logger.warning(f'{"="*80}')

            for _, row in suspicious.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                logger.warning(f"  {date_str}: {row['race_count']} races, {row['runner_count']} runners")
                logger.warning(f"    Created: {row['first_created']}")

            logger.warning(f'\nThese dates may have missing data. Recommend manual review.')
            logger.warning(f'To investigate: check local CSV files or run find_missing_races.py')

        else:
            logger.info(f'\n✓ No suspicious dates found - all dates have reasonable race counts')

        # Check for missing dates
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        scraped_dates = set(df['date'].dt.date)
        missing_dates = [d.date() for d in all_dates if d.date() not in scraped_dates]

        if missing_dates:
            logger.warning(f'\n{"="*80}')
            logger.warning(f'MISSING DATES (no data at all):')
            logger.warning(f'{"="*80}')
            for date in missing_dates:
                # Check if it's a day with typically no racing (some days have legitimately no races)
                logger.warning(f'  {date}')

            logger.warning(f'\nTotal: {len(missing_dates)} dates with no data')

    except Exception as e:
        logger.error(f'Error analyzing data: {str(e)}')
        raise


def check_local_files(start_date: str, end_date: str, country: str = 'gb', data_dir: str = 'data'):
    """
    Check local CSV files for missing dates.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        country: Country code
        data_dir: Data directory
    """
    logger.info(f'\nChecking local CSV files in {data_dir}/dates/{country}...')

    csv_dir = Path(data_dir) / 'dates' / country

    if not csv_dir.exists():
        logger.error(f'Directory not found: {csv_dir}')
        return

    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    current = start
    missing = []
    small = []

    while current <= end:
        date_str = current.strftime('%Y_%m_%d')
        csv_path = csv_dir / f'{date_str}.csv'

        if not csv_path.exists():
            missing.append(current.strftime('%Y-%m-%d'))
        else:
            # Check file size
            size = csv_path.stat().st_size
            if size < 1000:  # Less than 1KB is suspiciously small
                small.append((current.strftime('%Y-%m-%d'), size))

        current += timedelta(days=1)

    if missing:
        logger.warning(f'\nMissing CSV files: {len(missing)} dates')
        for date in missing[:10]:  # Show first 10
            logger.warning(f'  {date}')
        if len(missing) > 10:
            logger.warning(f'  ... and {len(missing) - 10} more')

    if small:
        logger.warning(f'\nSuspiciously small CSV files:')
        for date, size in small:
            logger.warning(f'  {date}: {size} bytes')

    if not missing and not small:
        logger.info('✓ All local CSV files present and reasonably sized')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Check data completeness')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--country', default='gb', help='Country code (default: gb)')
    parser.add_argument('--check-local', action='store_true', help='Also check local CSV files')
    parser.add_argument('--data-dir', default='data', help='Data directory (default: data)')

    args = parser.parse_args()

    # Check database
    analyze_race_counts(args.start_date, args.end_date, args.country)

    # Check local files if requested
    if args.check_local:
        check_local_files(args.start_date, args.end_date, args.country, args.data_dir)
