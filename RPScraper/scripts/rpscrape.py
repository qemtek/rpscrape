import gzip
import requests
import os
import sys
import json
from datetime import datetime, timezone

from dataclasses import dataclass

from lxml import html
from orjson import loads

from utils.argparser import ArgParser
from utils.completer import Completer
from utils.header import RandomHeader
from utils.race import Race, VoidRaceError, RaceParseError
from utils.rpscrape_settings import Settings
from utils.update import Update
import time
import logging

from utils.course import course_name, courses
from utils.lxml_funcs import xpath

settings = Settings()
random_header = RandomHeader()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


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
        r = requests.get(race_list.url, headers=random_header.header())
        races = loads(r.text)['data']['principleRaceResults']

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
        r = requests.get(day, headers=random_header.header())
        doc = html.fromstring(r.content)

        races = xpath(doc, 'a', 'link-listCourseNameLink')

        for race in races:
            if race.attrib['href'].split('/')[2] in course_ids:
                urls.add('https://www.racingpost.com' + race.attrib['href'])

    return sorted(list(urls))


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

    with file_writer(file_path) as csv:
        csv.write(settings.csv_header + '\n')

        for url in races:
            max_retries = 3
            retry_delay = 2  # seconds
            race_details = parse_race_details_from_url(url)

            for attempt in range(max_retries):
                try:
                    r = requests.get(url, headers=random_header.header())

                    # Check for HTTP error status
                    if r.status_code != 200:
                        if attempt < max_retries - 1:
                            logging.warning(f'HTTP {r.status_code} for {url}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})')
                            time.sleep(retry_delay)
                            continue
                        else:
                            logging.error(f'HTTP {r.status_code} for {url} after {max_retries} attempts')
                            failed_races.append({
                                'race_id': race_details.get('race_id', ''),
                                'course': race_details.get('course', ''),
                                'course_id': race_details.get('course_id', ''),
                                'date': race_details.get('date', ''),
                                'country': code,
                                'url': url,
                                'error_type': f'HTTP_{r.status_code}',
                                'error_message': f'HTTP {r.status_code}',
                                'attempts': max_retries,
                                'timestamp': datetime.now(timezone.utc).isoformat()
                            })
                            break

                    doc = html.fromstring(r.content)
                    race = Race(url, doc, code, settings.fields)

                    # Success - write data and break retry loop
                    for row in race.csv_data:
                        csv.write(row + '\n')
                    successful_races += 1
                    break

                except VoidRaceError:
                    # Void races are expected - don't retry
                    void_races += 1
                    break

                except RaceParseError as e:
                    # Parse errors (likely HTTP 406) - retry with backoff
                    if attempt < max_retries - 1:
                        logging.warning(f'Parse error for {url}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries}): {str(e)}')
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                    else:
                        logging.error(f'Failed to parse {url} after {max_retries} attempts: {str(e)}')
                        failed_races.append({
                            'race_id': race_details.get('race_id', ''),
                            'course': race_details.get('course', ''),
                            'course_id': race_details.get('course_id', ''),
                            'date': race_details.get('date', ''),
                            'country': code,
                            'url': url,
                            'error_type': 'HTTP_406',
                            'error_message': f'Parse error (likely HTTP 406): {str(e)}',
                            'attempts': max_retries,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                        break

                except Exception as e:
                    # Unexpected errors - log and retry
                    if attempt < max_retries - 1:
                        logging.warning(f'Unexpected error for {url}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries}): {str(e)}')
                        time.sleep(retry_delay)
                        continue
                    else:
                        logging.error(f'Unexpected error for {url} after {max_retries} attempts: {str(e)}')
                        failed_races.append({
                            'race_id': race_details.get('race_id', ''),
                            'course': race_details.get('course', ''),
                            'course_id': race_details.get('course_id', ''),
                            'date': race_details.get('date', ''),
                            'country': code,
                            'url': url,
                            'error_type': 'EXCEPTION',
                            'error_message': f'Unexpected: {str(e)}',
                            'attempts': max_retries,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                        break

            # Rate limiting: delay between each race request to avoid HTTP 406 errors
            time.sleep(1)

    # Create metadata about this scrape
    metadata = {
        'scrape_timestamp': scrape_timestamp,
        'file_name': file_name,
        'country': code,
        'total_races_discovered': total_races,
        'successful_races': successful_races,
        'void_races': void_races,
        'failed_races': len(failed_races),
        'completeness_pct': round((successful_races / total_races * 100) if total_races > 0 else 0, 2)
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
