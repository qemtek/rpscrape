#!/usr/bin/env python3
"""
Retry failed races from failure logs.
This script reads *_failures.log files and attempts to rescrape those races.
"""

import os
import sys
import csv
import re
from pathlib import Path
import logging

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))

from rpscrape import scrape_races, writer_csv, writer_gzip, settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_failure_log(log_path):
    """Parse a failure log file and extract URLs"""
    urls = []
    with open(log_path, 'r') as f:
        content = f.read()
        # Extract URLs using regex
        url_matches = re.findall(r'URL: (https://www\.racingpost\.com/[^\s]+)', content)
        urls.extend(url_matches)
    return urls


def retry_failed_races(failure_log_path, code='gb'):
    """Retry races from a failure log"""
    if not os.path.exists(failure_log_path):
        logger.error(f'Failure log not found: {failure_log_path}')
        return

    logger.info(f'Reading failure log: {failure_log_path}')
    failed_urls = parse_failure_log(failure_log_path)

    if not failed_urls:
        logger.info('No failed URLs found in log')
        return

    logger.info(f'Found {len(failed_urls)} failed races to retry')

    # Extract folder and file info from log path
    # Example: data/dates/gb/2025_11_17_failures.log
    parts = Path(failure_log_path).parts
    folder_name = f'{parts[-3]}/{parts[-2]}'  # dates/gb
    file_name = Path(failure_log_path).stem.replace('_failures', '')  # 2025_11_17

    # Determine file extension and writer
    if settings.toml.get('gzip_output', False):
        file_extension = 'csv.gz'
        file_writer = writer_gzip
    else:
        file_extension = 'csv'
        file_writer = writer_csv

    # Retry scraping
    logger.info(f'Retrying {len(failed_urls)} races...')
    new_failures = scrape_races(
        failed_urls,
        folder_name,
        f'{file_name}_retry',
        file_extension,
        code,
        file_writer
    )

    if not new_failures:
        logger.info('✓ All retries successful!')
        # Archive the original failure log
        archive_path = failure_log_path.replace('_failures.log', '_failures_resolved.log')
        os.rename(failure_log_path, archive_path)
        logger.info(f'Archived failure log to: {archive_path}')
    else:
        logger.warning(f'✗ {len(new_failures)} races still failing after retry')


def find_and_retry_all_failures(data_dir='data'):
    """Find all failure logs and retry them"""
    failure_logs = []

    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('_failures.log'):
                failure_logs.append(os.path.join(root, file))

    if not failure_logs:
        logger.info('No failure logs found')
        return

    logger.info(f'Found {len(failure_logs)} failure logs')

    for log_path in failure_logs:
        logger.info(f'\n{"="*80}')
        retry_failed_races(log_path)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Retry failed race scrapes')
    parser.add_argument('--log', help='Specific failure log file to retry')
    parser.add_argument('--all', action='store_true', help='Retry all failure logs in data directory')
    parser.add_argument('--code', default='gb', help='Country code (default: gb)')

    args = parser.parse_args()

    if args.all:
        find_and_retry_all_failures()
    elif args.log:
        retry_failed_races(args.log, args.code)
    else:
        parser.print_help()
