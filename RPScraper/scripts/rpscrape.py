import gzip
import os
import sys
import json
import random
import time
import logging
from datetime import datetime, timezone

from dataclasses import dataclass

from lxml import html
from orjson import loads

from utils.argparser import ArgParser
from utils.completer import Completer
from utils.network import NetworkClient, Persistent406Error
from utils.race import Race, VoidRaceError, RaceParseError
from utils.rpscrape_settings import Settings
from utils.update import Update

from utils.course import course_name, courses
from utils.lxml_funcs import xpath

settings = Settings()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Global network client using curl_cffi with browser impersonation
client = NetworkClient(timeout=14)


@dataclass
class RaceList:
    course_id: str
    course_name: str
    url: str


def check_for_update():
    update = Update()

    if update.available():
        choice = input('Update available. Do you want to update? Y/N ')
        if choice.lower() != 'y': return

        if update.pull_latest():
            print('Updated successfully.')
        else:
            print('Failed to update.')

        sys.exit()


def get_race_urls(tracks, years, code):
    urls = set()

    url_course = 'https://www.racingpost.com:443/profile/course/filter/results'
    url_result = 'https://www.racingpost.com/results'

    race_lists = []

    for track in tracks:
        for year in years:
            race_list = RaceList(*track, f'{url_course}/{track[0].lower()}/{year}/{code}/all-races')
            race_lists.append(race_list)

    for race_list in race_lists:
        status, r = client.get(race_list.url)
        if status != 200:
            logging.warning(f'Failed to get race list for {race_list.course_name}: HTTP {status}')
            continue

        data = loads(r.text).get('data', {})
        races = data.get('principleRaceResults', [])

        if races:
            for race in races:
                race_date = race["raceDatetime"][:10]
                race_id = race["raceInstanceUid"]
                url = f'{url_result}/{race_list.course_id}/{race_list.course_name}/{race_date}/{race_id}'
                urls.add(url.replace(' ', '-').replace("'", ''))

    return sorted(list(urls))


def parse_race_details_from_url(url):
    """
    Extract race details from URL.
    Example: https://www.racingpost.com/results/11/cheltenham/2025-11-15/905542
    Returns: {'course_id': '11', 'course': 'cheltenham', 'date': '2025-11-15', 'race_id': '905542'}
    """
    try:
        parts = url.replace('https://www.racingpost.com/results/', '').split('/')
        if len(parts) >= 4:
            return {
                'course_id': parts[0],
                'course': parts[1],
                'date': parts[2],
                'race_id': parts[3]
            }
    except:
        pass
    return {'course_id': '', 'course': '', 'date': '', 'race_id': ''}


def get_race_urls_date(dates, region):
    urls = set()

    days = [f'https://www.racingpost.com/results/{d}' for d in dates]

    course_ids = {course[0] for course in courses(region)}

    for day in days:
        status, r = client.get(day)
        if status != 200:
            logging.warning(f'Failed to get results for {day}: HTTP {status}')
            continue

        doc = html.fromstring(r.content)

        races = xpath(doc, 'a', 'link-listCourseNameLink')

        for race in races:
            if race.attrib['href'].split('/')[2] in course_ids:
                urls.add('https://www.racingpost.com' + race.attrib['href'])

    return sorted(list(urls))


def is_likely_rate_limited(failed_races):
    """
    Detect if failures are likely due to rate limiting based on error patterns.

    Returns True if:
    - More than 50% of failures are HTTP 406 errors
    - Failures are clustered in time (all within 2 minutes)
    """
    if not failed_races:
        return False

    # Check if >50% of failures are HTTP 406
    http_406_count = sum(1 for f in failed_races if f.get('error_type') == 'HTTP_406')
    if http_406_count / len(failed_races) > 0.5:
        return True

    # Check if failures are clustered in time (within 2 minutes)
    if len(failed_races) >= 3:
        try:
            timestamps = [datetime.fromisoformat(f['timestamp']) for f in failed_races]
            timestamps.sort()
            time_span = (timestamps[-1] - timestamps[0]).total_seconds()
            if time_span < 120:  # All failures within 2 minutes
                return True
        except (KeyError, ValueError):
            pass

    return False


def analyze_error_pattern(failed_races):
    """
    Categorize the error pattern to understand failure type.

    Returns one of:
    - 'none': No failures
    - 'all_rate_limited': 100% HTTP 406 errors
    - 'mostly_rate_limited': >80% HTTP 406 errors
    - 'mixed_with_rate_limiting': 50-80% HTTP 406 errors
    - 'other_errors': <50% HTTP 406 errors (likely real issues)
    """
    if not failed_races:
        return 'none'

    http_406_count = sum(1 for f in failed_races if f.get('error_type') == 'HTTP_406')
    total = len(failed_races)

    if http_406_count == 0:
        return 'other_errors'
    elif http_406_count == total:
        return 'all_rate_limited'
    elif http_406_count / total > 0.8:
        return 'mostly_rate_limited'
    elif http_406_count / total >= 0.5:
        return 'mixed_with_rate_limiting'
    else:
        return 'other_errors'


def scrape_races(races, folder_name, file_name, file_extension, code, file_writer):
    out_dir = f'data/{folder_name}/{code}'

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    file_path = f'{out_dir}/{file_name}.{file_extension}'
    failure_log_path = f'{out_dir}/{file_name}_failures.json'
    metadata_path = f'{out_dir}/{file_name}_metadata.json'

    # Track failures and successes for metadata
    failed_races = []
    successful_races = 0
    void_races = 0
    total_races = len(races)

    scrape_timestamp = datetime.now(timezone.utc).isoformat()

    # Exponential backoff tracking for rate limiting
    consecutive_406_errors = 0
    base_delay = 1.0  # Base delay between races in seconds
    current_delay = base_delay
    max_backoff_delay = 60.0  # Max delay after consecutive 406s

    with file_writer(file_path) as csv:
        csv.write(settings.csv_header + '\n')

        for url in races:
            race_details = parse_race_details_from_url(url)

            try:
                # NetworkClient handles retries and browser rotation internally
                # Raises Persistent406Error after all retry attempts fail
                status, r = client.get(url)

                # Check for HTTP error status (non-406 errors)
                if status != 200:
                    logging.error(f'HTTP {status} for {url}')
                    failed_races.append({
                        'race_id': race_details.get('race_id', ''),
                        'course': race_details.get('course', ''),
                        'course_id': race_details.get('course_id', ''),
                        'date': race_details.get('date', ''),
                        'country': code,
                        'url': url,
                        'error_type': f'HTTP_{status}',
                        'error_message': f'HTTP {status}',
                        'attempts': 7,  # NetworkClient default retries
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                    continue

                doc = html.fromstring(r.content)
                race = Race(client, url, doc, code, settings.fields)

                # Success - write data
                for row in race.csv_data:
                    csv.write(row + '\n')
                successful_races += 1

                # Reset backoff on success
                if consecutive_406_errors > 0:
                    logging.info(f'✓ Success after {consecutive_406_errors} consecutive 406 errors, resetting backoff')
                    consecutive_406_errors = 0
                    current_delay = base_delay

            except Persistent406Error:
                # NetworkClient exhausted all retries for 406 errors
                logging.error(f'Persistent HTTP 406 for {url} after all retry attempts')
                consecutive_406_errors += 1
                failed_races.append({
                    'race_id': race_details.get('race_id', ''),
                    'course': race_details.get('course', ''),
                    'course_id': race_details.get('course_id', ''),
                    'date': race_details.get('date', ''),
                    'country': code,
                    'url': url,
                    'error_type': 'HTTP_406',
                    'error_message': 'Rate limited (HTTP 406) after all retries',
                    'attempts': 7,  # NetworkClient default retries
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })

            except VoidRaceError:
                # Void races are expected - don't count as failure
                void_races += 1

            except RaceParseError as e:
                logging.error(f'Parse error for {url}: {str(e)}')
                failed_races.append({
                    'race_id': race_details.get('race_id', ''),
                    'course': race_details.get('course', ''),
                    'course_id': race_details.get('course_id', ''),
                    'date': race_details.get('date', ''),
                    'country': code,
                    'url': url,
                    'error_type': 'PARSE_ERROR',
                    'error_message': f'Parse error: {str(e)}',
                    'attempts': 1,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })

            except Exception as e:
                logging.error(f'Unexpected error for {url}: {str(e)}')
                failed_races.append({
                    'race_id': race_details.get('race_id', ''),
                    'course': race_details.get('course', ''),
                    'course_id': race_details.get('course_id', ''),
                    'date': race_details.get('date', ''),
                    'country': code,
                    'url': url,
                    'error_type': 'EXCEPTION',
                    'error_message': f'Unexpected: {str(e)}',
                    'attempts': 1,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })

            # Exponential backoff: increase delay after consecutive 406 errors
            if consecutive_406_errors > 0:
                # Calculate backoff delay: starts at 5s, doubles each time, caps at max
                current_delay = min(5 * (2 ** (consecutive_406_errors - 1)), max_backoff_delay)

                # Add randomization (±20%) to avoid patterns
                jitter = current_delay * 0.2 * (random.random() - 0.5) * 2
                actual_delay = current_delay + jitter

                logging.warning(f'⏸  Rate limited ({consecutive_406_errors} consecutive 406s), backing off for {actual_delay:.1f}s')
                time.sleep(actual_delay)
            else:
                # Normal delay between races
                time.sleep(base_delay)

    # Analyze rate limiting patterns
    http_406_count = sum(1 for f in failed_races if f.get('error_type') == 'HTTP_406')
    rate_limited = is_likely_rate_limited(failed_races)
    error_pattern = analyze_error_pattern(failed_races)

    # Create metadata about this scrape
    metadata = {
        'scrape_timestamp': scrape_timestamp,
        'file_name': file_name,
        'country': code,
        'total_races_discovered': total_races,
        'successful_races': successful_races,
        'void_races': void_races,
        'failed_races': len(failed_races),
        'completeness_pct': round((successful_races / total_races * 100) if total_races > 0 else 0, 2),

        # Rate limiting detection
        'http_406_errors': http_406_count,
        'likely_rate_limited': rate_limited,
        'error_pattern': error_pattern
    }

    # Save metadata
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    # Write failure log as JSON if there were any failures
    if failed_races:
        with open(failure_log_path, 'w') as f:
            json.dump({
                'scrape_timestamp': scrape_timestamp,
                'total_failures': len(failed_races),
                'failures': failed_races
            }, f, indent=2)
        print(f'WARNING: {len(failed_races)} races failed to scrape. See {failure_log_path}')

    print(f'Finished scraping.\n{file_name}.{file_extension} saved in rpscrape/{out_dir.lstrip("../")}')
    print(f'Metadata: {successful_races}/{total_races} races scraped successfully ({metadata["completeness_pct"]}%)')
    if void_races > 0:
        print(f'Void races: {void_races}')
    if failed_races:
        print(f'Failed races: {len(failed_races)}')
        print(f'  - HTTP 406 errors: {http_406_count}')
        print(f'  - Error pattern: {error_pattern}')
        print(f'  - Likely rate limited: {rate_limited}')

    return {
        'metadata': metadata,
        'failures': failed_races
    }


def writer_csv(file_path):
    return open(file_path, 'w', encoding='utf-8')


def writer_gzip(file_path):
    return gzip.open(file_path, 'wt', encoding='utf-8')


def main():
    if settings.toml is None:
        sys.exit()

    if settings.toml['auto_update']:
        check_for_update()

    file_extension = 'csv'
    file_writer = writer_csv

    if settings.toml.get('gzip_output', False):
        file_extension = 'csv.gz'
        file_writer = writer_gzip

    parser = ArgParser()

    if len(sys.argv) > 1:
        args = parser.parse_args(sys.argv[1:])

        if args.date:
            folder_name = 'dates/' + args.region
            file_name = args.date.replace('/', '_')
            races = get_race_urls_date(parser.dates, args.region)
        else:
            folder_name = args.region if args.region else course_name(args.course)
            file_name = args.year
            races = get_race_urls(parser.tracks, parser.years, args.type)

        scrape_races(races, folder_name, file_name, file_extension, args.type, file_writer)
    else:
        if sys.platform == 'linux':
            import readline
            completions = Completer()
            readline.set_completer(completions.complete)
            readline.parse_and_bind('tab: complete')

        while True:
            args = input('[rpscrape]> ').lower().strip()
            args = parser.parse_args_interactive([arg.strip() for arg in args.split()])

            if args:
                if 'dates' in args:
                    races = get_race_urls_date(args['dates'], args['region'])
                else:
                    races = get_race_urls(args['tracks'], args['years'], args['type'])

                scrape_races(races, args['folder_name'], args['file_name'], file_extension, args['type'], file_writer)


if __name__ == '__main__':
    main()
