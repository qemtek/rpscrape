import datetime
import json
import re

from typing import Any, Callable, Dict, List, Optional, Tuple


RACE_TYPE_MAP = {
    'X': 'Flat',
    'F': 'Flat',
    'C': 'Chase',
    'H': 'Hurdle',
    'B': 'NH Flat',
    'U': 'Chase',
    'W': 'NH Flat',
    'A': 'Flat',
    'N': 'NH Flat',
    'S': 'Flat',
    'P': 'Flat',
}

DEFAULT_RACE_TYPE = 'Flat'


def extract_next_data(page_html: str) -> Optional[Dict[str, Any]]:
    match = re.search(
        r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
        page_html,
        re.DOTALL,
    )
    if not match:
        return None

    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def _initial_state(data: Dict[str, Any]) -> Dict[str, Any]:
    return (
        data.get('props', {})
        .get('pageProps', {})
        .get('initialState', {})
    )


def extract_meetings(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _initial_state(data).get('raceCards', {}).get('meetings', [])


def extract_race_page(
    data: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[List[Dict[str, Any]]]]:
    container = _initial_state(data).get('racePage', {}).get('data', {})
    race = container.get('race')
    runners = container.get('runners')
    if not race:
        return None, None
    return race, runners or []


def get_pattern(race_name: str) -> str:
    if not race_name:
        return ''

    regex_group = r'(\(|\s)((G|g)rade|(G|g)roup) (\d|[A-Ca-c]|I*)(\)|\s)'
    match = re.search(regex_group, race_name)

    if match:
        return f'{match.groups()[1]} {match.groups()[4]}'.title()

    if any(x in race_name.lower() for x in {'listed race', '(listed'}):
        return 'Listed'

    return ''


def parse_id_from_url(url: Optional[str]) -> Optional[int]:
    if not url:
        return None
    match = re.search(r'/profile/\w+/(\d+)/', url)
    return int(match.group(1)) if match else None


def parse_colour_sex(colour_sex: Optional[str]) -> Tuple[str, str, str]:
    if not colour_sex:
        return '', '', ''

    parts = colour_sex.strip().split()
    if len(parts) < 2:
        return colour_sex.strip(), '', ''

    colour = ' '.join(parts[:-1])
    sex_code = parts[-1]
    sex_names = {
        'c': 'Colt',
        'f': 'Filly',
        'g': 'Gelding',
        'm': 'Mare',
        'h': 'Horse',
        'r': 'Ridgling',
    }
    return colour, sex_names.get(sex_code.lower(), sex_code), sex_code


def parse_form(form_data: Optional[List[Dict[str, Any]]]) -> str:
    if not form_data:
        return ''
    return ''.join(item.get('figure', '') for item in form_data)


def num(value: Any) -> Optional[int]:
    if value is None or value == '-' or value == '':
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _start_time(race: Dict[str, Any]) -> str:
    if race.get('startTime'):
        return race['startTime']

    race_datetime = race.get('raceDatetime')
    if not race_datetime:
        return ''

    try:
        return datetime.datetime.fromisoformat(
            race_datetime.replace('Z', '+00:00')
        ).strftime('%H:%M')
    except ValueError:
        return ''


def _race_date(race: Dict[str, Any], target_date: str) -> str:
    race_datetime = race.get('raceDatetime')
    if not race_datetime:
        return target_date

    try:
        return datetime.datetime.fromisoformat(
            race_datetime.replace('Z', '+00:00')
        ).strftime('%Y-%m-%d')
    except ValueError:
        return target_date


def _race_type(race: Dict[str, Any]) -> str:
    if race.get('raceTypeDesc'):
        return race['raceTypeDesc']
    return RACE_TYPE_MAP.get(race.get('raceType'), DEFAULT_RACE_TYPE)


def map_next_race(
    race: Dict[str, Any],
    target_date: str,
    url: str,
    meeting_country: str,
    region_lookup: Callable[[str], str],
    surface_lookup: Callable[[str], str],
) -> Dict[str, Any]:
    course_id = _to_int(race.get('courseId') or race.get('courseUid'))
    course_name = race.get('courseStyleName') or race.get('meetingName') or ''

    if course_name == 'Belmont At The Big A':
        course_id = 255
        course_name = 'Aqueduct'

    country_code = race.get('countryCode') or meeting_country or ''
    if course_id is not None:
        try:
            region = region_lookup(str(course_id))
        except Exception:
            region = country_code
    else:
        region = country_code

    race_name = race.get('raceTitle') or ''
    pattern = get_pattern(race_name.lower())
    race_class = _to_int(race.get('raceClass'))
    if race_class is None and pattern:
        race_class = 1

    rating_band = race.get('officialRatingBandDesc') or None
    going = race.get('going') or ''

    return {
        'href': url,
        'race_id': _to_int(race.get('raceId')),
        'date': _race_date(race, target_date),
        'off_time': _start_time(race),
        'course_id': course_id,
        'course': course_name,
        'course_detail': race.get('stalls') or race.get('straightRoundJubileeCode') or '',
        'region': region,
        'race_name': race_name,
        'race_type': _race_type(race),
        'distance_f': race.get('distanceFurlongs') or race.get('distanceFurlongRounded'),
        'distance_y': race.get('distanceYards') or race.get('distanceYard'),
        'distance_round': race.get('distanceRounded') or None,
        'distance': race.get('distance') or race.get('distanceRounded') or None,
        'pattern': pattern,
        'race_class': race_class,
        'age_band': race.get('agesAllowed') or None,
        'rating_band': rating_band,
        'prize': race.get('formattedTotalPrizeMoney') or None,
        'field_size': race.get('numberOfRunners') or race.get('declaredRunners'),
        'handicap': bool(
            rating_band
            or race.get('raceHandicapDesc')
            or ('handicap' in race_name.lower())
        ),
        'going': going,
        'surface': race.get('surfaceType') or surface_lookup(going),
        'runners': [],
    }


def map_next_runner(
    runner: Dict[str, Any],
    name_cleaner: Callable[[str], str],
) -> Dict[str, Any]:
    colour, sex, sex_code = parse_colour_sex(runner.get('colorSex'))
    silk_url = runner.get('silkImage') or ''
    silk_match = re.search(r'/svg/(.+?)\.svg', silk_url)

    return {
        'age': runner.get('age'),
        'breeder': None,
        'breeder_id': None,
        'claim': runner.get('weightAllowanceLbs'),
        'colour': colour,
        'comment': runner.get('diomed'),
        'dam': name_cleaner(runner.get('damName') or ''),
        'dam_id': parse_id_from_url(runner.get('damUrl')),
        'dam_region': runner.get('damCountry'),
        'damsire': name_cleaner(runner.get('damsireName') or ''),
        'damsire_id': parse_id_from_url(runner.get('damsireUrl')),
        'damsire_region': runner.get('damsireCountry'),
        'dob': None,
        'draw': runner.get('draw') if runner.get('draw') else None,
        'form': parse_form(runner.get('formFiguresData')),
        'gelding_first_time': runner.get('geldingFirstTime', False),
        'headgear': runner.get('horseHeadGear'),
        'headgear_first': runner.get('horseHeadGearFirstTime', False),
        'horse_id': runner.get('horseId'),
        'jockey': name_cleaner(runner.get('jockeyName') or ''),
        'jockey_allowance': runner.get('weightAllowanceLbs'),
        'jockey_id': runner.get('jockeyId'),
        'last_run': runner.get('daysSinceLastRun'),
        'lbs': runner.get('weightCarried'),
        'medical': [],
        'name': name_cleaner(runner.get('horseName') or ''),
        'non_runner': runner.get('nonRunner', False),
        'number': runner.get('startNumber'),
        'ofr': num(runner.get('officialRatingToday')),
        'owner': name_cleaner(runner.get('ownerName') or ''),
        'owner_id': runner.get('ownerId'),
        'prev_owners': [],
        'prev_trainers': [],
        'profile': None,
        'quotes': [],
        'region': runner.get('countryOrigin') or '',
        'reserve': runner.get('irishReserve', False),
        'rpr': num(runner.get('rpPostmark')),
        'sex': sex,
        'sex_code': sex_code,
        'silk_path': silk_match.group(1) if silk_match else '',
        'silk_url': silk_url,
        'sire': name_cleaner(runner.get('sireName') or ''),
        'sire_id': parse_id_from_url(runner.get('sireUrl')),
        'sire_region': runner.get('sireCountry'),
        'spotlight': runner.get('spotlight'),
        'stable_tour': [],
        'stats': {},
        'trainer': name_cleaner(runner.get('trainerName') or ''),
        'trainer_14_days': None,
        'trainer_id': runner.get('trainerId'),
        'trainer_location': None,
        'trainer_rtf': runner.get('trainerRtf'),
        'ts': num(runner.get('rpTopspeed')),
        'wind_surgery_first': runner.get('windSurgery'),
        'wind_surgery_second': None,
    }
