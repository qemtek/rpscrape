#!/usr/bin/env python3
"""Debug script to test Race object initialization with problematic URL"""

import sys
sys.path.insert(0, 'scripts')

from lxml import html
import requests
from utils.race import Race, VoidRaceError
from utils.header import RandomHeader

random_header = RandomHeader()

# Problematic URLs from the error
urls = [
    'https://www.racingpost.com/results/1353/newcastle-aw/2025-11-17/906635',
    'https://www.racingpost.com/results/1353/newcastle-aw/2025-11-17/906636',
]

# Get the fields from settings
import settings

for url in urls:
    print(f"\n{'='*80}")
    print(f"Testing URL: {url}")
    print('='*80)

    try:
        # Fetch the page the same way the scraper does
        r = requests.get(url, headers=random_header.header())
        doc = html.fromstring(r.content)

        print(f"Status: {r.status_code}")
        print(f"Content length: {len(r.content)} bytes")

        # Test the xpath before creating Race object
        result_info_list = doc.xpath('//div[@class="rp-raceInfo"]/ul/li')
        print(f"\nXPath test: Found {len(result_info_list)} elements")

        if result_info_list:
            print("First element:")
            print(html.tostring(result_info_list[0], encoding='unicode', pretty_print=True)[:300])

        # Now try to create the Race object
        print("\nCreating Race object...")
        code = 'flat'  # Assuming flat racing
        race = Race(url, doc, code, settings.fields)
        print("✓ Race object created successfully")
        print(f"  Race ID: {race.race_info.get('race_id')}")
        print(f"  Course: {race.race_info.get('course')}")
        print(f"  Runners: {race.race_info.get('ran')}")

    except VoidRaceError:
        print("✗ Race is void")
    except IndexError as e:
        print(f"✗ IndexError: {str(e)}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
