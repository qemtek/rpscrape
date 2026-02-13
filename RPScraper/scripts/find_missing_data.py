#!/usr/bin/env python3
"""
Find and fill missing data in the Glue table for a specific country.

Queries Athena to find dates that are missing from the Glue table,
scrapes those dates, and uploads the data to S3 and Glue.

Usage:
    python scripts/find_missing_data.py --country usa --start-date 2008-05-01
    python scripts/find_missing_data.py --country usa --start-date 2008-05-01 --dry-run
    python scripts/find_missing_data.py --country usa --start-date 2008-05-01 --end-date 2024-12-31
"""

import argparse
import datetime as dt
import logging
import os
import subprocess
import sys
from pathlib import Path

import awswrangler as wr
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from settings import (
    PROJECT_DIR,
    boto3_session,
    AWS_GLUE_DB,
    AWS_RPSCRAPE_TABLE_NAME,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def get_existing_dates(country):
    """Query Athena for existing dates for a country"""
    query = f"SELECT DISTINCT date FROM {AWS_RPSCRAPE_TABLE_NAME} WHERE country = '{country}'"

    logger.info(f"Querying Athena for existing {country} dates...")
    df = wr.athena.read_sql_query(
        query,
        database=AWS_GLUE_DB,
        boto3_session=boto3_session
    )
    dates = set(pd.to_datetime(df['date']).dt.date)
    logger.info(f"Found {len(dates)} existing dates for {country}")
    return dates


def find_missing_dates(country, start_date, end_date):
    """Find dates missing from the Glue table"""
    existing_dates = get_existing_dates(country)

    all_dates = [
        start_date + dt.timedelta(days=x)
        for x in range((end_date - start_date).days + 1)
    ]

    missing = [d for d in all_dates if d not in existing_dates]
    logger.info(f"Found {len(missing)} missing dates out of {len(all_dates)} total")
    return missing


def run_rpscrape(country, date_str):
    """Run the scraper for a single date"""
    try:
        result = subprocess.run(
            f'cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR} python3 scripts/rpscrape.py -d {date_str} -r {country}',
            shell=True,
            capture_output=False,
            text=True
        )
        return result.returncode
    except Exception as e:
        logger.error(f"Error scraping {country} {date_str}: {e}")
        return 1


def run_upload_to_glue(country):
    """Upload scraped data to Glue using overwrite_partitions mode"""
    cmd = ["python3", f"{PROJECT_DIR}/scripts/simple_upload_to_s3.py"]

    env = os.environ.copy()
    env["MODE"] = "overwrite_partitions"
    env["PYTHONPATH"] = PROJECT_DIR
    env["COUNTRIES"] = country

    logger.info(f"Uploading to Glue with MODE=overwrite_partitions, COUNTRIES={country}")
    result = subprocess.run(cmd, env=env, cwd=PROJECT_DIR)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description='Find and fill missing data in Glue')
    parser.add_argument('--country', required=True, help='Country code (e.g., usa, gb, ire, fr, aus)')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default=None, help='End date (YYYY-MM-DD, default: yesterday)')
    parser.add_argument('--dry-run', action='store_true', help='Only show missing dates, do not scrape')
    parser.add_argument('--skip-upload', action='store_true', help='Skip the Glue upload step')

    args = parser.parse_args()

    country = args.country.lower()
    start_date = dt.datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = (
        dt.datetime.strptime(args.end_date, '%Y-%m-%d').date()
        if args.end_date
        else dt.date.today() - dt.timedelta(days=1)
    )

    logger.info(f"Country: {country}")
    logger.info(f"Date range: {start_date} to {end_date}")

    # Step 1: Find missing dates
    missing_dates = find_missing_dates(country, start_date, end_date)

    if not missing_dates:
        logger.info("No missing dates found!")
        return 0

    if len(missing_dates) <= 20:
        for d in missing_dates:
            logger.info(f"  {d}")
    else:
        for d in missing_dates[:10]:
            logger.info(f"  {d}")
        logger.info(f"  ... and {len(missing_dates) - 10} more")

    if args.dry_run:
        logger.info("Dry run complete - not scraping")
        return 0

    # Step 2: Scrape missing dates
    logger.info(f"Scraping {len(missing_dates)} missing dates...")

    for i, date in enumerate(missing_dates):
        date_str = date.strftime('%Y/%m/%d')
        logger.info(f"[{i+1}/{len(missing_dates)}] Scraping {country} - {date_str}")
        run_rpscrape(country, date_str)

    # Step 3: Upload to Glue
    if not args.skip_upload:
        logger.info("Uploading to Glue...")
        exit_code = run_upload_to_glue(country)
        if exit_code != 0:
            logger.error(f"Upload failed with exit code {exit_code}")
            return exit_code

    logger.info("Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
