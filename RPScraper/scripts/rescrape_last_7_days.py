#!/usr/bin/env python3
"""
Safely re-scrape and regenerate the last N days of racing data.

This script:
1. Calculates the date range (last N days)
2. CLEANS local data directory (removes ALL existing CSV files)
3. Deletes existing data from Glue table for those dates
4. Re-scrapes data for those dates
5. Uploads ONLY those dates to S3 and Glue
6. Cleans up local directory

SAFETY FEATURES:
- Validates date ranges before processing
- Shows exactly what will be deleted/regenerated
- Dry-run mode to preview without making changes
- Extensive logging
- Atomic operations where possible
"""

import os
import sys
import datetime as dt
import pandas as pd
import subprocess
import glob
import logging
import argparse
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from settings import (
    PROJECT_DIR,
    AWS_GLUE_DB,
    AWS_RPSCRAPE_TABLE_NAME,
    boto3_session
)
import awswrangler as wr

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'{PROJECT_DIR}/logs/rescrape_{dt.datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)


def calculate_date_range(days_back: int) -> Tuple[dt.date, dt.date]:
    """
    Calculate the date range for re-scraping.

    Args:
        days_back: Number of days to go back from today

    Returns:
        Tuple of (start_date, end_date)
    """
    end_date = dt.date.today() - dt.timedelta(days=1)  # Yesterday
    start_date = end_date - dt.timedelta(days=days_back - 1)

    logger.info(f"Calculated date range: {start_date} to {end_date} ({days_back} days)")
    return start_date, end_date


def validate_date_range(start_date: dt.date, end_date: dt.date) -> bool:
    """
    Validate that date range is safe and reasonable.

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        True if valid, False otherwise
    """
    # Check dates are in correct order
    if start_date > end_date:
        logger.error(f"Start date {start_date} is after end date {end_date}")
        return False

    # Check we're not regenerating more than 30 days
    days_diff = (end_date - start_date).days + 1
    if days_diff > 30:
        logger.error(f"Date range spans {days_diff} days. Maximum allowed is 30 days.")
        logger.error("Use full_refresh.py for larger regenerations.")
        return False

    # Check we're not regenerating future dates
    if end_date > dt.date.today():
        logger.error(f"End date {end_date} is in the future")
        return False

    # Check we're not regenerating very old data
    earliest_allowed = dt.date(2008, 1, 1)
    if start_date < earliest_allowed:
        logger.error(f"Start date {start_date} is before {earliest_allowed}")
        return False

    logger.info(f"✓ Date range validation passed: {days_diff} days")
    return True


def clean_local_data_directory(countries: List[str], dry_run: bool = False) -> int:
    """
    Remove ALL CSV files from local data/dates directories.

    This is critical to ensure we only upload the newly scraped data.

    Args:
        countries: List of country codes
        dry_run: If True, only show what would be deleted

    Returns:
        Number of files that would be/were deleted
    """
    logger.info("=" * 80)
    logger.info("STEP 1: CLEAN LOCAL DATA DIRECTORY")
    logger.info("=" * 80)

    total_files = 0

    for country in countries:
        data_dir = Path(PROJECT_DIR) / 'data' / 'dates' / country

        if not data_dir.exists():
            logger.info(f"Directory doesn't exist: {data_dir}")
            continue

        # Find all CSV files
        csv_files = list(data_dir.glob('*.csv'))

        if csv_files:
            logger.warning(f"Found {len(csv_files)} existing CSV files in {data_dir}")
            total_files += len(csv_files)

            if dry_run:
                logger.info(f"  [DRY RUN] Would delete {len(csv_files)} files")
                if len(csv_files) <= 10:
                    for f in csv_files:
                        logger.info(f"    - {f.name}")
                else:
                    for f in csv_files[:5]:
                        logger.info(f"    - {f.name}")
                    logger.info(f"    ... and {len(csv_files) - 5} more")
            else:
                logger.info(f"  Deleting {len(csv_files)} files...")
                for csv_file in csv_files:
                    csv_file.unlink()
                logger.info(f"  ✓ Deleted {len(csv_files)} files from {country}")
        else:
            logger.info(f"✓ No files to clean in {country}")

    if total_files > 0:
        if dry_run:
            logger.warning(f"[DRY RUN] Would delete {total_files} total files across all countries")
        else:
            logger.info(f"✓ Cleaned {total_files} total files across all countries")
    else:
        logger.info("✓ Local directories already clean")

    return total_files


def delete_from_glue(start_date: dt.date, end_date: dt.date, countries: List[str], dry_run: bool = False) -> int:
    """
    Delete data from Glue table for specified date range.

    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        countries: List of country codes
        dry_run: If True, only show what would be deleted

    Returns:
        Number of rows deleted (or would be deleted)
    """
    logger.info("=" * 80)
    logger.info("STEP 2: DELETE FROM GLUE TABLE")
    logger.info("=" * 80)

    countries_str = "'" + "','".join(countries) + "'"

    # First, count how many rows will be deleted
    count_query = f"""
    SELECT COUNT(*) as row_count
    FROM {AWS_RPSCRAPE_TABLE_NAME}
    WHERE date >= DATE('{start_date}')
      AND date <= DATE('{end_date}')
      AND country IN ({countries_str})
    """

    try:
        logger.info(f"Counting rows to delete...")
        df_count = wr.athena.read_sql_query(
            count_query,
            database=AWS_GLUE_DB,
            boto3_session=boto3_session
        )
        rows_to_delete = int(df_count['row_count'].values[0])

        logger.info(f"Found {rows_to_delete:,} rows to delete")
        logger.info(f"  Date range: {start_date} to {end_date}")
        logger.info(f"  Countries: {countries}")

        if rows_to_delete == 0:
            logger.info("✓ No rows to delete (date range may have no data)")
            return 0

        if dry_run:
            logger.warning(f"[DRY RUN] Would delete {rows_to_delete:,} rows from Glue")
            return rows_to_delete

        # Execute delete
        delete_query = f"""
        DELETE FROM {AWS_RPSCRAPE_TABLE_NAME}
        WHERE date >= DATE('{start_date}')
          AND date <= DATE('{end_date}')
          AND country IN ({countries_str})
        """

        logger.info(f"Executing DELETE query...")
        logger.info(f"Query: {delete_query}")

        query_id = wr.athena.start_query_execution(
            sql=delete_query,
            database=AWS_GLUE_DB,
            boto3_session=boto3_session
        )

        logger.info(f"Delete query submitted: {query_id}")

        # Wait for query to complete
        logger.info("Waiting for delete to complete...")
        wr.athena.wait_query(query_execution_id=query_id, boto3_session=boto3_session)

        logger.info(f"✓ Deleted {rows_to_delete:,} rows from Glue table")
        return rows_to_delete

    except Exception as e:
        logger.error(f"✗ Error deleting from Glue: {e}")
        raise


def scrape_date_range(start_date: dt.date, end_date: dt.date, countries: List[str], dry_run: bool = False) -> int:
    """
    Scrape data for specified date range.

    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        countries: List of country codes
        dry_run: If True, skip actual scraping

    Returns:
        Number of days scraped
    """
    logger.info("=" * 80)
    logger.info("STEP 3: SCRAPE DATA")
    logger.info("=" * 80)

    # Generate date list
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += dt.timedelta(days=1)

    total_scrapes = len(dates) * len(countries)
    logger.info(f"Will scrape {len(dates)} days × {len(countries)} countries = {total_scrapes} scrapes")

    if dry_run:
        logger.warning(f"[DRY RUN] Would scrape:")
        for country in countries:
            logger.info(f"  {country}: {start_date} to {end_date}")
        return len(dates)

    # Scrape each date for each country
    completed = 0
    failed = 0

    for country in countries:
        logger.info(f"Scraping {country}...")

        for date in dates:
            formatted_date = date.strftime('%Y/%m/%d')

            try:
                # Run rpscrape
                cmd = f'cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR} python3 scripts/rpscrape.py -d {formatted_date} -r {country}'
                logger.info(f"  {country} - {formatted_date}")

                result = subprocess.run(
                    cmd,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout per date
                )

                if result.returncode == 0:
                    completed += 1
                else:
                    logger.warning(f"    Warning: scrape returned code {result.returncode}")
                    logger.warning(f"    stderr: {result.stderr[:200]}")
                    failed += 1

            except subprocess.TimeoutExpired:
                logger.error(f"    ✗ Timeout scraping {country} - {formatted_date}")
                failed += 1
            except Exception as e:
                logger.error(f"    ✗ Error scraping {country} - {formatted_date}: {e}")
                failed += 1

    logger.info(f"✓ Scraping complete: {completed} successful, {failed} failed")
    return len(dates)


def verify_local_files(start_date: dt.date, end_date: dt.date, countries: List[str]) -> bool:
    """
    Verify that ONLY the expected files exist locally.

    Args:
        start_date: Expected start date
        end_date: Expected end date
        countries: List of country codes

    Returns:
        True if only expected files exist, False otherwise
    """
    logger.info("=" * 80)
    logger.info("STEP 4: VERIFY LOCAL FILES")
    logger.info("=" * 80)

    # Generate expected filenames
    expected_dates = []
    current_date = start_date
    while current_date <= end_date:
        expected_dates.append(current_date)
        current_date += dt.timedelta(days=1)

    expected_count = len(expected_dates) * len(countries)
    logger.info(f"Expecting {len(expected_dates)} dates × {len(countries)} countries = {expected_count} files")

    all_good = True
    actual_count = 0

    for country in countries:
        data_dir = Path(PROJECT_DIR) / 'data' / 'dates' / country

        if not data_dir.exists():
            logger.error(f"✗ Directory missing: {data_dir}")
            all_good = False
            continue

        csv_files = list(data_dir.glob('*.csv'))
        actual_count += len(csv_files)

        # Check each expected date
        for date in expected_dates:
            filename = date.strftime('%Y_%m_%d.csv')
            file_path = data_dir / filename

            if not file_path.exists():
                logger.warning(f"  Missing: {country}/{filename}")
                all_good = False

        # Check for unexpected files
        expected_filenames = {date.strftime('%Y_%m_%d.csv') for date in expected_dates}
        for csv_file in csv_files:
            if csv_file.name not in expected_filenames:
                logger.error(f"  ✗ Unexpected file: {country}/{csv_file.name}")
                logger.error(f"    This file should not exist! Local directory should only have files from {start_date} to {end_date}")
                all_good = False

    if actual_count == expected_count and all_good:
        logger.info(f"✓ Verification passed: Found exactly {expected_count} expected files, no unexpected files")
        return True
    else:
        logger.error(f"✗ Verification failed:")
        logger.error(f"  Expected: {expected_count} files")
        logger.error(f"  Found: {actual_count} files")
        return False


def upload_to_glue(dry_run: bool = False) -> bool:
    """
    Upload scraped data to S3 and Glue using simple_upload_to_s3.py

    Args:
        dry_run: If True, skip actual upload

    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 80)
    logger.info("STEP 5: UPLOAD TO S3 AND GLUE")
    logger.info("=" * 80)

    if dry_run:
        logger.warning("[DRY RUN] Would run: python3 scripts/simple_upload_to_s3.py")
        return True

    try:
        cmd = f'cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR} python3 scripts/simple_upload_to_s3.py'
        logger.info(f"Running: {cmd}")

        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minute timeout
        )

        # Log output
        if result.stdout:
            for line in result.stdout.split('\n'):
                if line.strip():
                    logger.info(f"  {line}")

        if result.stderr:
            for line in result.stderr.split('\n'):
                if line.strip():
                    logger.warning(f"  stderr: {line}")

        if result.returncode == 0:
            logger.info("✓ Upload completed successfully")
            return True
        else:
            logger.error(f"✗ Upload failed with return code {result.returncode}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("✗ Upload timed out after 30 minutes")
        return False
    except Exception as e:
        logger.error(f"✗ Error during upload: {e}")
        return False


def cleanup_local_files(countries: List[str], dry_run: bool = False):
    """
    Clean up local CSV files after successful upload.

    Args:
        countries: List of country codes
        dry_run: If True, skip cleanup
    """
    logger.info("=" * 80)
    logger.info("STEP 6: CLEANUP")
    logger.info("=" * 80)

    if dry_run:
        logger.info("[DRY RUN] Would clean up local files")
        return

    clean_local_data_directory(countries, dry_run=False)


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Safely re-scrape and regenerate the last N days of racing data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Re-scrape last 7 days for GB and IRE (dry run)
  python3 rescrape_last_7_days.py --days 7 --countries gb,ire --dry-run

  # Re-scrape last 7 days for all countries (actual run)
  python3 rescrape_last_7_days.py --days 7 --countries gb,ire,fr

  # Re-scrape last 14 days
  python3 rescrape_last_7_days.py --days 14 --countries gb,ire,fr
        """
    )

    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days to regenerate (default: 7, max: 30)'
    )

    parser.add_argument(
        '--countries',
        type=str,
        default='gb,ire,fr',
        help='Comma-separated list of country codes (default: gb,ire,fr)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be done without making any changes'
    )

    parser.add_argument(
        '--skip-cleanup',
        action='store_true',
        help='Skip final cleanup of local files (for debugging)'
    )

    parser.add_argument(
        '--yes',
        '-y',
        action='store_true',
        help='Skip confirmation prompt (for automated/Docker execution)'
    )

    args = parser.parse_args()

    # Parse countries
    countries = [c.strip().lower() for c in args.countries.split(',')]

    # Print configuration
    logger.info("=" * 80)
    logger.info("RESCRAPE LAST N DAYS - CONFIGURATION")
    logger.info("=" * 80)
    logger.info(f"Days to regenerate: {args.days}")
    logger.info(f"Countries: {countries}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Skip cleanup: {args.skip_cleanup}")
    logger.info("=" * 80)

    if args.dry_run:
        logger.warning("")
        logger.warning("*** DRY RUN MODE ***")
        logger.warning("No changes will be made. This is a preview only.")
        logger.warning("")

    try:
        # Calculate date range
        start_date, end_date = calculate_date_range(args.days)

        # Validate
        if not validate_date_range(start_date, end_date):
            logger.error("Date range validation failed. Aborting.")
            return 1

        # Confirm with user if not dry run and not auto-confirmed
        if not args.dry_run and not args.yes:
            logger.warning("")
            logger.warning("=" * 80)
            logger.warning("⚠️  WARNING: DESTRUCTIVE OPERATION")
            logger.warning("=" * 80)
            logger.warning(f"This will DELETE and re-scrape data for:")
            logger.warning(f"  Dates: {start_date} to {end_date} ({args.days} days)")
            logger.warning(f"  Countries: {countries}")
            logger.warning("")
            logger.warning("This operation:")
            logger.warning("  1. Deletes existing data from Glue table")
            logger.warning("  2. Re-scrapes data from Racing Post")
            logger.warning("  3. Uploads new data to S3 and Glue")
            logger.warning("")
            response = input("Type 'yes' to continue: ")
            if response.lower() != 'yes':
                logger.info("Operation cancelled by user")
                return 0

        # Execute workflow
        clean_local_data_directory(countries, dry_run=args.dry_run)
        delete_from_glue(start_date, end_date, countries, dry_run=args.dry_run)
        scrape_date_range(start_date, end_date, countries, dry_run=args.dry_run)

        if not args.dry_run:
            # Verify files before upload
            if not verify_local_files(start_date, end_date, countries):
                logger.error("")
                logger.error("✗ File verification failed!")
                logger.error("Local directory contains unexpected files.")
                logger.error("Aborting to prevent uploading wrong data.")
                logger.error("")
                logger.error("Please investigate and clean the local directory manually:")
                logger.error(f"  {PROJECT_DIR}/data/dates/")
                return 1

            # Upload
            if not upload_to_glue(dry_run=args.dry_run):
                logger.error("Upload failed. Local files preserved for debugging.")
                return 1

            # Cleanup
            if not args.skip_cleanup:
                cleanup_local_files(countries, dry_run=args.dry_run)

        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ OPERATION COMPLETE")
        logger.info("=" * 80)

        if args.dry_run:
            logger.info("This was a dry run. No changes were made.")
            logger.info("Remove --dry-run flag to execute for real.")
        else:
            logger.info(f"Successfully regenerated {args.days} days of data")
            logger.info(f"Date range: {start_date} to {end_date}")
            logger.info(f"Countries: {countries}")

        return 0

    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("✗ OPERATION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    # Create logs directory if it doesn't exist
    os.makedirs(f'{PROJECT_DIR}/logs', exist_ok=True)
    sys.exit(main())