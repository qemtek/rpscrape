#!/bin/bash
cd /RPScraper/src
export PYTHONPATH=.
python full_refresh.py
python upload_data_to_s3.py true

