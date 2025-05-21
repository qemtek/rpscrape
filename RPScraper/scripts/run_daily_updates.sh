#!/bin/bash

cd RPScraper || exit
export PYTHONPATH=.

date=$(date +%Y/%m/%d -d "3 days ago")
countries=("gb" "ire" "fr")

echo "Running rpscrape for date: $date"
echo "Running rpscrape for countries: "
ls
echo "${countries[@]}"

for country in "${countries[@]}"
do
  echo "Running scraper. Date: $date, Country: $country"
  python3 scripts/rpscrape.py -d "$date" -r "$country" || echo "Completed"
done

# Use the new simple upload script that matches full_refresh.py processing
python scripts/simple_upload_to_s3.py
