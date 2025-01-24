#!/bin/bash

cd RPScraper || exit
export PYTHONPATH=.

python scripts/full_refresh.py
python scripts/upload_data_to_s3.py true

