import datetime as dt
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from lxml import html


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'RPScraper' / 'scripts'
sys.path.insert(0, str(SCRIPTS_DIR))

import rpscrape
from utils.race import Race, RaceParseError, extract_race_result


class Response:
    def __init__(self, payload):
        self.content = json.dumps(payload).encode('utf-8')


def result_document(result):
    payload = {
        'props': {
            'pageProps': {
                'initialState': {
                    'raceResult': {'data': result},
                }
            }
        }
    }
    return html.fromstring(
        '<html><body>'
        f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'
        '</body></html>'
    )


class ResultDiscoveryTests(unittest.TestCase):
    def test_discovers_completed_results_for_requested_region(self):
        payload = {
            'meetings': [
                {
                    'venueUid': 101,
                    'courseKey': 'worcester',
                    'races': [
                        {
                            'raceId': '924811',
                            'currentRaceStatus': 'result',
                            'raceType': 'Chase',
                            'ratingBand': '0-115',
                        },
                        {
                            'raceId': '924812',
                            'currentRaceStatus': 'pre-race',
                        },
                    ],
                },
                {
                    'venueUid': 206,
                    'courseKey': 'deauville',
                    'races': [
                        {'raceId': '927231', 'currentRaceStatus': 'result'},
                    ],
                },
            ]
        }

        with patch.object(rpscrape.client, 'get', return_value=(200, Response(payload))):
            races = rpscrape.get_race_urls_date([dt.date(2026, 8, 23)], 'gb')

        self.assertEqual(
            races,
            [
                (
                    'https://www.racingpost.com/results/101/worcester/2026-08-23/924811',
                    payload['meetings'][0]['races'][0],
                )
            ],
        )

    def test_rejects_changed_meetings_schema(self):
        with patch.object(
            rpscrape.client,
            'get',
            return_value=(200, Response({'unexpected': []})),
        ):
            with self.assertRaisesRegex(RuntimeError, 'Invalid Racing Post meetings'):
                rpscrape.get_race_urls_date([dt.date(2026, 8, 23)], 'gb')


class CurrentResultParserTests(unittest.TestCase):
    def setUp(self):
        self.result = {
            'raceId': 924811,
            'raceDatetime': '2026-08-23T14:08:00+01:00',
            'courseUid': 101,
            'courseName': 'Worcester',
            'header': {
                'raceTitle': "Example Mares' Handicap Chase (Listed Race)",
                'raceTypeCode': 'C',
                'raceClass': '4',
                'agesAllowed': '4yo+',
                'distanceShort': '2m7f',
                'distanceYard': 5060,
                'going': 'Good',
                'prizes': [
                    {'position': 1, 'formatted': '£5,281'},
                    {'position': 2, 'formatted': '£2,430'},
                ],
            },
            'details': {
                'numberOfRunners': 3,
                'winningTime': '5m 52.04s',
            },
            'runners': [
                {
                    'outcomeCode': '1',
                    'saddleClothNo': 3,
                    'horseUid': 7015774,
                    'horseName': 'Axel Bleue',
                    'horseSuffix': None,
                    'age': 6,
                    'weightStones': 11,
                    'weightPounds': 10,
                    'weightCarriedLbs': 164,
                    'headgear': 'tp',
                    'odds': '5/2F',
                    'jockeyName': 'James Bowen',
                    'jockeyUrl': '/profile/jockey/96830/james-bowen/',
                    'trainerName': 'Mickey Bowen',
                    'trainerUrl': '/profile/trainer/28905/mickey-bowen/',
                    'ownerName': 'Miss Jayne Brace',
                    'ownerUrl': '/profile/owner/60661/miss-jayne-brace/',
                    'officialRating': '109',
                    'rpRating': '115',
                    'topspeed': '89',
                    'silkUrl': 'https://example.test/silk.svg',
                    'pedigree': {
                        'colourSex': 'bl g',
                        'sireName': 'Axxos',
                        'sireSuffix': '(GER)',
                        'sireUrl': '/profile/horse/679645/axxos/',
                        'damName': 'Sula Bleue',
                        'damSuffix': None,
                        'damUrl': '/profile/horse/7015787/sula-bleue/',
                        'damSireName': 'Sulamani',
                        'damSireUrl': '/profile/horse/556063/sulamani/',
                    },
                    'comment': {
                        'comment': 'Led, stayed on',
                        'bettingMovements': 'op 9/4',
                    },
                },
                {
                    'outcomeCode': '2',
                    'saddleClothNo': 2,
                    'beatenDistance': 'shd',
                    'beatenDistanceToWinner': None,
                    'distanceToWinnerNative': 0.1,
                    'horseUid': 4183371,
                    'horseName': 'Ras Kassar',
                    'horseSuffix': '(FR)',
                    'age': 7,
                    'weightStones': 11,
                    'weightPounds': 8,
                    'weightCarriedLbs': 162,
                    'headgear': 'ht',
                    'odds': '7/2',
                    'jockeyName': 'Rian Corcoran',
                    'jockeyUrl': '/profile/jockey/103118/rian-corcoran/',
                    'trainerName': 'David Pipe',
                    'trainerUrl': '/profile/trainer/10157/david-pipe/',
                    'ownerName': 'David Pipe Racing Club',
                    'ownerUrl': '/profile/owner/283404/david-pipe-racing-club/',
                    'officialRating': '112',
                    'rpRating': '118',
                    'topspeed': '92',
                    'pedigree': {'colourSex': 'ro g'},
                    'comment': None,
                },
                {
                    'outcomeCode': 'PU',
                    'saddleClothNo': 7,
                    'horseUid': 5057495,
                    'horseName': 'Camino Rocio',
                    'horseSuffix': '(IRE)',
                    'age': 8,
                    'weightStones': 11,
                    'weightPounds': 6,
                    'weightCarriedLbs': 160,
                    'headgear': 'p',
                    'odds': '7/1',
                    'jockeyName': 'Harry Skelton',
                    'jockeyUrl': '/profile/jockey/85218/harry-skelton/',
                    'trainerName': 'Dan Skelton',
                    'trainerUrl': '/profile/trainer/16270/dan-skelton/',
                    'ownerName': 'Dan Skelton',
                    'ownerUrl': '/profile/owner/189542/dan-skelton/',
                    'officialRating': '105',
                    'rpRating': '–',
                    'topspeed': '–',
                    'pedigree': {'colourSex': 'b g'},
                    'comment': {'comment': 'Pulled up', 'bettingMovements': None},
                },
            ],
        }

    def test_extracts_current_result_payload(self):
        self.assertEqual(extract_race_result(result_document(self.result)), self.result)

    def test_maps_current_result_to_existing_output_contract(self):
        race = Race(
            None,
            'https://www.racingpost.com/results/101/worcester/2026-08-23/924811',
            result_document(self.result),
            ['date', 'course_id', 'race_id', 'type', 'rating_band', 'horse_id'],
            race_metadata={'raceType': 'Chase', 'ratingBand': '0-115'},
        )

        self.assertEqual(race.race_info.date, '2026-08-23')
        self.assertEqual(race.race_info.off, '14:08')
        self.assertEqual(race.race_info.region, 'GB')
        self.assertEqual(race.race_info.race_type, 'Chase')
        self.assertEqual(race.race_info.race_class, 'Class 4')
        self.assertEqual(race.race_info.pattern, 'Listed')
        self.assertEqual(race.race_info.rating_band, '0-115')
        self.assertEqual(race.race_info.age_band, '4yo+')
        self.assertEqual(race.race_info.sex_rest, 'M')
        self.assertEqual(race.race_info.dist_m, '4627')
        self.assertEqual(race.runner_info.pos, ['1', '2', 'PU'])
        self.assertEqual(race.runner_info.btn, ['0', '0.1', '-'])
        self.assertEqual(race.runner_info.ovr_btn, ['0', '0.1', '-'])
        self.assertEqual(race.runner_info.horse[0], 'Axel Bleue (GB)')
        self.assertEqual(race.runner_info.sex, ['G', 'G', 'G'])
        self.assertEqual(race.runner_info.dec, ['3.50', '4.50', '8.00'])
        self.assertEqual(race.runner_info.prize, ['5281', '2430', ''])
        self.assertEqual(race.runner_info.sire[0], 'Axxos (GER)')
        self.assertEqual(race.runner_info.dam[0], 'Sula Bleue (GB)')
        self.assertEqual(race.runner_info.damsire[0], 'Sulamani')
        self.assertEqual(race.runner_info.comment[0], 'Led - stayed on(op 9/4)')
        self.assertEqual(race.runner_info.time, ['5:52.04', '5:52.06', '-'])
        self.assertEqual(race.runner_info.secs, ['352.04', '352.06', '-'])
        self.assertEqual(len(race.csv_data), 3)

    def test_missing_current_and_legacy_payload_fails_without_retry_loop(self):
        with self.assertRaisesRegex(RaceParseError, 'No result data found'):
            Race(
                None,
                'https://www.racingpost.com/results/101/worcester/2026-08-23/924811',
                html.fromstring('<html><body></body></html>'),
                [],
            )


if __name__ == '__main__':
    unittest.main()
