#!/usr/bin/env python3
"""
Find missing races by comparing Racing Post's race schedule against scraped data.

This script:
1. Fetches the race schedule from Racing Post for a given date range
2. Compares against local CSV files or Glue database
3. Identifies missing races that should be retried
4. Generates failure log files for the retry script
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import csv
import logging
from typing import List, Set, Dict
import requests
from lxml import html as lxml_html
from orjson import loads

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.header import RandomHeader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

random_header = RandomHeader()


def get_expected_races_for_date(date_str: str, country: str = 'gb') -> Set[str]:
    """
    Fetch expected race URLs from Racing Post for a given date.

    Args:
        date_str: Date in YYYY-MM-DD format
        country: Country code (gb, ire, fr, etc.)

    Returns:
        Set of race URLs
    """
    # Map country codes to Racing Post regions
    region_map = {
        'gb': 'gb',
        'ire': 'ire',
        'fr': 'fr',
        'us': 'usa',
    }

    region = region_map.get(country.lower(), country.lower())

    # Format date for Racing Post URL (YYYY-MM-DD)
    rp_date = date_str

    # Fetch the results page for this date
    url = f'https://www.racingpost.com/results/{rp_date}'

    logger.info(f'Fetching race schedule for {date_str} from {url}')

    try:
        r = requests.get(url, headers=random_header.header(), timeout=10)

        if r.status_code != 200:
            logger.error(f'HTTP {r.status_code} when fetching {url}')
            return set()

        # Parse the JSON data containing race results
        doc = lxml_html.fromstring(r.content)

        # Find the script tag containing the race data
        # Racing Post embeds race data in a JavaScript variable
        scripts = doc.xpath('//script[contains(text(), "PRELOADED_STATE")]')

        race_urls = set()

        # Try extracting race URLs from links
        # Racing Post has links like /results/{course_id}/{course_name}/{date}/{race_id}
        race_links = doc.xpath('//a[contains(@href, "/results/")]/@href')

        for link in race_links:
            # Filter for actual race result links (contain race_id at end)
            parts = link.split('/')
            if len(parts) >= 5 and parts[1] == 'results':
                try:
                    # Check if last part is a race_id (numeric)
                    race_id = int(parts[-1])
                    if date_str in link:
                        full_url = f'https://www.racingpost.com{link}'
                        race_urls.add(full_url)
                except ValueError:
                    continue

        logger.info(f'Found {len(race_urls)} races for {date_str}')
        return race_urls

    except Exception as e:
        logger.error(f'Error fetching races for {date_str}: {str(e)}')
        return set()


def get_scraped_race_ids(csv_path: str) -> Set[str]:
    """
    Extract race IDs from a scraped CSV file.

    Args:
        csv_path: Path to CSV file

    Returns:
        Set of race IDs (as strings)
    """
    race_ids = set()

    if not os.path.exists(csv_path):
        return race_ids

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'race_id' in row:
                    race_ids.add(row['race_id'])
    except Exception as e:
        logger.error(f'Error reading {csv_path}: {str(e)}')

    return race_ids


def find_missing_races_for_date(date_str: str, country: str = 'gb', data_dir: str = 'data') -> List[Dict]:
    """
    Find missing races for a specific date.

    Args:
        date_str: Date in YYYY-MM-DD format
        country: Country code
        data_dir: Base data directory

    Returns:
        List of dicts with missing race info
    """
    # Get expected races from Racing Post
    expected_urls = get_expected_races_for_date(date_str, country)

    if not expected_urls:
        logger.warning(f'No races found for {date_str} - might be no racing or fetch failed')
        return []

    # Get scraped races from CSV
    csv_filename = date_str.replace('-', '_') + '.csv'
    csv_path = f'{data_dir}/dates/{country}/{csv_filename}'

    scraped_race_ids = get_scraped_race_ids(csv_path)

    # Find missing races
    missing = []
    for url in expected_urls:
        race_id = url.split('/')[-1]
        if race_id not in scraped_race_ids:
            missing.append({
                'url': url,
                'race_id': race_id,
                'date': date_str,
                'country': country
            })

    logger.info(f'{date_str}: Expected {len(expected_urls)} races, scraped {len(scraped_race_ids)}, missing {len(missing)}')

    return missing


def create_failure_log(missing_races: List[Dict], output_path: str):
    """Create a failure log file for missing races"""
    if not missing_races:
        return

    with open(output_path, 'w') as f:
        f.write(f'Failed to scrape {len(missing_races)} races (detected retroactively):\n\n')
        for race in missing_races:
            f.write(f"URL: {race['url']}\n")
            f.write(f"Reason: Not found in scraped data (possible HTTP 406 or parse failure)\n")
            f.write(f"Attempts: 0 (needs retry)\n\n")

    logger.info(f'Created failure log: {output_path}')


def scan_date_range(start_date: str, end_date: str, country: str = 'gb', data_dir: str = 'data'):
    """
    Scan a date range for missing races.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        country: Country code
        data_dir: Base data directory
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    current = start
    total_missing = 0

    while current <= end:
        date_str = current.strftime('%Y-%m-%d')

        missing = find_missing_races_for_date(date_str, country, data_dir)

        if missing:
            total_missing += len(missing)

            # Create failure log
            csv_filename = date_str.replace('-', '_')
            failure_log_path = f'{data_dir}/dates/{country}/{csv_filename}_failures.log'
            create_failure_log(missing, failure_log_path)

        current += timedelta(days=1)

    logger.info(f'\n{"="*80}')
    logger.info(f'SUMMARY: Found {total_missing} missing races across date range')
    logger.info(f'{"="*80}')

    if total_missing > 0:
        logger.info('\nTo retry missing races, run:')
        logger.info(f'  python3 scripts/retry_failed_races.py --all')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Find missing races by comparing expected vs scraped')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--country', default='gb', help='Country code (default: gb)')
    parser.add_argument('--data-dir', default='data', help='Data directory (default: data)')

    args = parser.parse_args()

    scan_date_range(args.start_date, args.end_date, args.country, args.data_dir)
