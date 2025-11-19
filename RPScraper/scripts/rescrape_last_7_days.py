#!/usr/bin/env python3
"""
Safely re-scrape and regenerate the last N days of racing data using partition overwrites.

This script:
1. Calculates the date range (last N days)
2. Scrapes data for those dates (with retry logic for HTTP 406 blocks)
3. Uploads to Glue using overwrite_partitions mode (no duplicates)

The partition-based approach:
- No need to delete from Glue first
- Atomic per-partition replacement
- Handles duplicates automatically
- Works with non-transactional Hive tables
"""

import os
import sys
import datetime as dt
import subprocess
import logging
import argparse
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from settings import PROJECT_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def calculate_date_range(days: int) -> Tuple[dt.date, dt.date]:
    """Calculate the start and end dates for the last N days"""
    end_date = dt.date.today() - dt.timedelta(days=1)  # Yesterday
    start_date = end_date - dt.timedelta(days=days - 1)
    return start_date, end_date


def run_full_refresh(start_date: dt.date, end_date: dt.date, countries: List[str], dry_run: bool = False) -> int:
    """
    Run full_refresh.py to scrape the date range.

    Returns: exit code (0 = success)
    """
    logger.info("="*80)
    logger.info("STEP 1: SCRAPE DATA")
    logger.info("="*80)

    countries_str = ",".join(countries)
    cmd = [
        "python3", f"{PROJECT_DIR}/scripts/full_refresh.py"
    ]

    # full_refresh.py reads from environment variables, not command-line args
    env = os.environ.copy()
    env["START_DATE"] = start_date.strftime("%Y-%m-%d")
    env["END_DATE"] = end_date.strftime("%Y-%m-%d")
    env["COUNTRIES"] = countries_str
    env["FORCE"] = "true"  # Always force overwrite of local files
    env["PYTHONPATH"] = PROJECT_DIR

    logger.info(f"Running: START_DATE={env['START_DATE']} END_DATE={env['END_DATE']} COUNTRIES={env['COUNTRIES']} FORCE=true {' '.join(cmd)}")

    if dry_run:
        logger.warning("[DRY RUN] Would run scrape command")
        return 0

    result = subprocess.run(cmd, env=env, cwd=PROJECT_DIR)
    return result.returncode


def run_upload_to_glue(dry_run: bool = False) -> int:
    """
    Upload scraped data to Glue using overwrite_partitions mode.

    Returns: exit code (0 = success)
    """
    logger.info("="*80)
    logger.info("STEP 2: UPLOAD TO GLUE (overwrite_partitions mode)")
    logger.info("="*80)

    cmd = [
        "python3", f"{PROJECT_DIR}/scripts/simple_upload_to_s3.py"
    ]

    env = os.environ.copy()
    env["MODE"] = "overwrite_partitions"
    env["PYTHONPATH"] = PROJECT_DIR

    logger.info(f"Running: MODE=overwrite_partitions {' '.join(cmd)}")

    if dry_run:
        logger.warning("[DRY RUN] Would upload to Glue with overwrite_partitions mode")
        return 0

    result = subprocess.run(cmd, env=env, cwd=PROJECT_DIR)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description='Re-scrape last N days of racing data')
    parser.add_argument('--days', type=int, default=7, help='Number of days to regenerate (default: 7)')
    parser.add_argument('--countries', type=str, default='gb,ire,fr', help='Comma-separated list of countries')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--yes', '-y', action='store_true', help='Skip confirmation prompt')

    args = parser.parse_args()

    countries = [c.strip() for c in args.countries.split(',')]
    start_date, end_date = calculate_date_range(args.days)

    # Display configuration
    logger.info("="*80)
    logger.info("RESCRAPE LAST N DAYS - CONFIGURATION")
    logger.info("="*80)
    logger.info(f"Days to regenerate: {args.days}")
    logger.info(f"Countries: {countries}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("="*80)

    if args.dry_run:
        logger.warning("")
        logger.warning("*** DRY RUN MODE ***")
        logger.warning("No changes will be made. This is a preview only.")
        logger.warning("")

    logger.info(f"Calculated date range: {start_date} to {end_date} ({args.days} days)")

    # Confirm with user unless --yes flag
    if not args.yes and not args.dry_run:
        response = input(f"\nThis will re-scrape and overwrite data for {args.days} days ({start_date} to {end_date}). Continue? [y/N] ")
        if response.lower() != 'y':
            logger.info("Aborted by user")
            return 1

    # Step 1: Scrape data
    exit_code = run_full_refresh(start_date, end_date, countries, args.dry_run)
    if exit_code != 0:
        logger.error(f"Scraping failed with exit code {exit_code}")
        return exit_code

    # Step 2: Upload to Glue with partition overwrites
    exit_code = run_upload_to_glue(args.dry_run)
    if exit_code != 0:
        logger.error(f"Upload to Glue failed with exit code {exit_code}")
        return exit_code

    logger.info("")
    logger.info("="*80)
    logger.info("✓ OPERATION COMPLETE")
    logger.info("="*80)

    if args.dry_run:
        logger.info("This was a dry run. No changes were made.")
        logger.info("Remove --dry-run flag to execute for real.")
    else:
        logger.info(f"Successfully re-scraped and uploaded {args.days} days of data")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Countries: {', '.join(countries)}")
        logger.info("")
        logger.info("Data has been updated in Glue using partition overwrites.")
        logger.info("No duplicates created. Old partition data was replaced atomically.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
