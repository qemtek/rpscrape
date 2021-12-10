#!/bin/bash
echo "setting project_dir to ./src/scripts"
cd ./src/scripts
echo "Running scraper. Date: $1, Country: $2"
python3 full_refresh2.py
python3 upload_data_to_s3.py

