# Downloads all files from rpscrape and stores them in the data folder

import datetime as dt
import subprocess
import awswrangler as wr
import pandas as pd

from settings import PROJECT_DIR, boto3_session


def run_rpscrape(country, date):
    try:
        subprocess.call(f'python scripts/rpscrape.py -d {date} -r {country}', shell=True)
    except EOFError:
        pass


date_today = dt.datetime.today().date()
start_date = pd.to_datetime('2020-11-23').date()  #date_today - dt.timedelta(days=round(364.25*15))
print(f"Start date: {start_date}")
end_date = date_today - dt.timedelta(days=1)
print(f"End date: {end_date}")

# Get the countries we want
countries = ["gb"]  # "aus", "usa", "fr" "ire"
# Find the number of days between the start and end dates
delta = end_date - start_date
dates = list()
for country in countries:
    for i in range(delta.days + 1):
        day = (start_date + dt.timedelta(days=i)).strftime(format='%Y/%m/%d')
        s3_file_name = f"{country}_{str(day).replace('/', '-')}.parquet"
        local_file_path = f"{PROJECT_DIR}/data/dates/{country}/{str(day).replace('/', '_')}.csv"
        if i % 100 == 0:
            print(local_file_path)
        try:
            run_rpscrape(country, day)
            file_name = local_file_path.split('/')[-1]
            wr.s3.upload(local_file=local_file_path, boto3_session=boto3_session,
                         path=f"s3://rpscrape/data/{country}/{file_name}")
        except Exception as e:
            print(f"Couldnt get data for {country} on {day}")
