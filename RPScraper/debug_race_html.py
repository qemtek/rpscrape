#!/usr/bin/env python3
"""Debug script to inspect HTML structure of problematic race page"""

import sys
sys.path.insert(0, 'scripts')

from lxml import html
from utils.network import NetworkClient

client = NetworkClient(timeout=14)

url = 'https://www.racingpost.com/results/1353/newcastle-aw/2025-11-17/906635'

print(f"Fetching {url}...")

try:
    status, response = client.get(url)
    print(f"Status code: {status}")

    if status == 200:
        doc = html.fromstring(response.content)

        # Try the xpath that's failing
        print("\n=== Testing original xpath ===")
        result_info = doc.xpath('//div[@class="rp-raceInfo"]/ul/li')
        print(f"Found {len(result_info)} elements with xpath: //div[@class=\"rp-raceInfo\"]/ul/li")

        # Look for variations
        print("\n=== Looking for rp-raceInfo divs ===")
        race_info_divs = doc.xpath('//div[contains(@class, "rp-raceInfo")]')
        print(f"Found {len(race_info_divs)} divs containing 'rp-raceInfo'")
        for i, div in enumerate(race_info_divs[:3]):
            print(f"\nDiv {i}: {html.tostring(div, encoding='unicode')[:500]}")

        # Look for any div with race info
        print("\n=== Looking for race info sections ===")
        all_divs = doc.xpath('//div[@class]')
        race_related = [d for d in all_divs if 'race' in d.get('class', '').lower()]
        print(f"Found {len(race_related)} divs with 'race' in class name")
        for div in race_related[:5]:
            class_name = div.get('class', '')
            print(f"  - {class_name}")

        # Check if race is void/abandoned
        print("\n=== Checking for void/abandoned indicators ===")
        void_indicators = doc.xpath('//*[contains(text(), "void") or contains(text(), "Void") or contains(text(), "abandoned") or contains(text(), "Abandoned")]')
        print(f"Found {len(void_indicators)} elements with void/abandoned text")
        for elem in void_indicators[:3]:
            print(f"  - {elem.text_content()[:100]}")

    else:
        print(f"Failed to fetch page: {status}")
        print(response.text[:500])

except Exception as e:
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()
