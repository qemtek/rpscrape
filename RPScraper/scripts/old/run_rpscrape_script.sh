#!/bin/bash
echo "setting project_dir to RPScraper/scripts"
cd RPscraper/scripts
echo "Running scraper. Date: $1, Country: $2"
python3 RPScraperrpscrape.py -d "$1" -r "$2"

