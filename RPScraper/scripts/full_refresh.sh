#!/bin/bash
cd /RPScraper/src
python full_refresh.py
python upload_data_to_s3.py true

