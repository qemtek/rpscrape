#!/usr/bin/env python3
"""Debug specific race 906636 to see why it wasn't scraped"""

import sys
sys.path.insert(0, 'scripts')

from lxml import html
import requests
from utils.header import RandomHeader

random_header = RandomHeader()

url = 'https://www.racingpost.com/results/1353/newcastle-aw/2025-11-17/906636'

print(f"Fetching {url}...")

# Make 5 requests to see if we get different results
for i in range(5):
    print(f"\n{'='*80}")
    print(f"Request {i+1}/5")
    print('='*80)

    r = requests.get(url, headers=random_header.header())
    doc = html.fromstring(r.content)

    print(f"Status: {r.status_code}")
    print(f"Content length: {len(r.content)} bytes")

    # Test the problematic xpath
    result_info_list = doc.xpath('//div[@class="rp-raceInfo"]/ul/li')
    print(f"XPath result: Found {len(result_info_list)} elements")

    # Check for void/abandoned
    void_text = doc.xpath('//*[contains(text(), "void") or contains(text(), "Void") or contains(text(), "abandoned") or contains(text(), "Abandoned") or contains(text(), "VOID") or contains(text(), "ABANDONED")]')
    if void_text:
        print(f"VOID/ABANDONED indicators found: {len(void_text)}")
        for elem in void_text[:3]:
            print(f"  - {elem.text_content()[:100]}")

    # Check number of runners
    runner_rows = doc.xpath('//tr[contains(@class, "rp-horseTable__row")]')
    print(f"Runner rows found: {len(runner_rows)}")

    # Look for race class/name
    race_name = doc.xpath('//h2[@class="rp-raceTimeCourseName__title"]')
    if race_name:
        print(f"Race name: {race_name[0].text_content().strip()[:60]}")
