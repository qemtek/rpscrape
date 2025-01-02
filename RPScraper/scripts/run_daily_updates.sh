#!/bin/bash

cd RPScraper || exit
export PYTHONPATH=.

date=$(date +%Y/%m/%d -d "yesterday")
countries=("gb" "ire")

echo "Running rpscrape for date: $date"
echo "Running rpscrape for countries: "
ls
echo "${countries[@]}"

for country in "${countries[@]}"
do
  echo "Running scraper. Date: $date, Country: $country"
  python3 scripts/rpscrape.py -d "$date" -r "$country" || echo "Completed"
done

python scripts/upload_data_to_s3.py false
