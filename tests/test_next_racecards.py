import json
import unittest

from RPScraper.scripts.utils.next_racecards import (
    extract_meetings,
    extract_next_data,
    extract_race_page,
    map_next_race,
    map_next_runner,
)


class NextRacecardTests(unittest.TestCase):
    def test_extracts_next_data_from_script_tag(self):
        payload = {'props': {'pageProps': {'initialState': {'raceCards': {'meetings': []}}}}}
        html = (
            '<html><head></head><body>'
            f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'
            '</body></html>'
        )

        self.assertEqual(extract_next_data(html), payload)

    def test_extracts_meetings_and_race_page_data(self):
        data = {
            'props': {
                'pageProps': {
                    'initialState': {
                        'raceCards': {'meetings': [{'courseName': 'Kempton', 'races': []}]},
                        'racePage': {
                            'data': {
                                'race': {'raceId': 123},
                                'runners': [{'horseName': 'Example'}],
                            }
                        },
                    }
                }
            }
        }

        self.assertEqual(extract_meetings(data), [{'courseName': 'Kempton', 'races': []}])
        self.assertEqual(
            extract_race_page(data),
            ({'raceId': 123}, [{'horseName': 'Example'}]),
        )

    def test_maps_next_race_to_existing_output_shape(self):
        race = {
            'raceId': 123,
            'courseId': '1079',
            'courseStyleName': 'Kempton',
            'countryCode': 'GB',
            'raceTitle': 'Example Handicap',
            'raceType': 'F',
            'raceTypeDesc': 'Flat',
            'startTime': '18:30',
            'distanceFurlongs': 8.0,
            'distanceYards': 0,
            'raceClass': '4',
            'agesAllowed': '3yo+',
            'officialRatingBandDesc': '0-85',
            'formattedTotalPrizeMoney': 'GBP 10,000',
            'numberOfRunners': 12,
            'going': 'Standard',
        }

        mapped = map_next_race(
            race,
            target_date='2026-05-20',
            url='https://www.racingpost.com/racecards/1079/kempton/2026-05-20/123',
            meeting_country='GB',
            region_lookup=lambda course_id: 'GB',
            surface_lookup=lambda going: 'AW',
        )

        self.assertEqual(mapped['race_id'], 123)
        self.assertEqual(mapped['course'], 'Kempton')
        self.assertEqual(mapped['region'], 'GB')
        self.assertEqual(mapped['race_type'], 'Flat')
        self.assertEqual(mapped['field_size'], 12)
        self.assertTrue(mapped['handicap'])
        self.assertEqual(mapped['surface'], 'AW')

    def test_maps_next_runner_to_existing_output_shape(self):
        runner = {
            'horseName': ' Example Horse ',
            'horseId': 456,
            'startNumber': 3,
            'draw': 7,
            'age': 4,
            'colorSex': 'b g',
            'countryOrigin': 'IRE',
            'formFiguresData': [{'figure': '1'}, {'figure': '2'}],
            'rpPostmark': '89',
            'rpTopspeed': '-',
            'officialRatingToday': '82',
            'jockeyName': ' Example Jockey ',
            'jockeyId': 12,
            'trainerName': ' Example Trainer ',
            'trainerId': 34,
            'weightCarried': 135,
            'horseHeadGear': 'p',
            'horseHeadGearFirstTime': True,
            'sireName': ' Sire ',
            'sireUrl': '/profile/horse/111/sire',
            'damName': ' Dam ',
            'damUrl': '/profile/horse/222/dam',
            'ownerName': ' Owner ',
            'ownerId': 55,
        }

        mapped = map_next_runner(runner, name_cleaner=lambda value: value.strip().lower())

        self.assertEqual(mapped['name'], 'example horse')
        self.assertEqual(mapped['horse_id'], 456)
        self.assertEqual(mapped['draw'], 7)
        self.assertEqual(mapped['sex'], 'Gelding')
        self.assertEqual(mapped['sex_code'], 'g')
        self.assertEqual(mapped['form'], '12')
        self.assertEqual(mapped['rpr'], 89)
        self.assertIsNone(mapped['ts'])
        self.assertEqual(mapped['ofr'], 82)
        self.assertEqual(mapped['sire_id'], 111)
        self.assertEqual(mapped['dam_id'], 222)


if __name__ == '__main__':
    unittest.main()
