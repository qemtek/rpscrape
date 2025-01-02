#!/bin/bash

# Set AWS credentials
export AWS_ACCESS_KEY_ID=*
export AWS_SECRET_ACCESS_KEY=*
export AWS_DEFAULT_REGION="eu-west-1"

# Set Python path
cd RPScraper || exit
export PYTHONPATH=.

# Get yesterday's date
date=$(date -v-1d +%Y/%m/%d)
countries=("gb" "ire")

echo "Running rpscrape for date: $date"
echo "Running rpscrape for countries: ${countries[@]}"

# Run scraper for each country
for country in "${countries[@]}"
do
  echo "Running scraper. Date: $date, Country: $country"
  python3 scripts/rpscrape.py -d "$date" -r "$country" || echo "Completed"
done

# Upload data to S3
python scripts/upload_data_to_s3.py false
