#!/bin/bash
echo "setting project_dir to ./scripts"
cd ./scripts
echo "Running scraper. Date: $1, Country: $2"
python3 rpscrape.py -d "$1" "$2"

