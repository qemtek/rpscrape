#!/bin/bash
if [ -z "$1" ]
  then
    echo "No date argument supplied, running for yesterdays date"
    date=$(date +%Y/%m/%d -d "yesterday")
  else
    echo "Date argument supplied, running for {$1}"
    date="$1"
fi

if [ -z "$2" ]
  then
    echo "No country argument supplied, running for gb/ire/usa/aus"
    countries=("gb" "ire" "usa" "aus")
  else
    echo "Country argument supplied, running for {$1}"
    countries=("$1")
fi

echo "Running rpscrape for date: $date"

cd RPScraper
./scripts/run_daily_updates.sh "${countries[@]}" "$date"