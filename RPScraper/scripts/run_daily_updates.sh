#!/bin/bash
date=$(date +%Y/%m/%d -d "yesterday")
countries=("gb" "ire" "usa" "aus")

echo "Running rpscrape for date: $date"
echo "Running rpscrape for countries: "
echo "${countries[@]}"

for country in "${countries[@]}"
do
  echo "Running scraper. Date: $date, Country: $country"
  python3 ./rpscrape.py -d "$date" -r "$country" || echo "Completed"
done

export PYTHONPATH=..
python upload_data_to_s3.py false
