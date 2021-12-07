# Downloads all files from rpscrape and stores them in the data folder

import datetime as dt
import subprocess


def run_rpscrape(country, date):
    try:
        print(f'python ../scripts/rpscrape.py -d {date} -r {country}')
        subprocess.call(f'python ../scripts/rpscrape.py -d {date} -r {country}', shell=True)
        print(f'Finished scraping {country}: {date}')
    except EOFError:
        print("Run rpscrape failed")
        pass


date_today = dt.datetime.today().date()
start_date = date_today - dt.timedelta(days=round(364.25*10))
print(f"Start date: {start_date}")
end_date = date_today - dt.timedelta(days=1)
print(f"End date: {end_date}")

# Get the countries we want
countries = ["gb", "aus"]  # "usa", "ire",
# Find the number of days between the start and end dates
delta = end_date - start_date
dates = list()
for country in countries:
    run_rpscrape(country, date=f"{str(start_date).replace('-', '/')}-{str(end_date).replace('-', '/')}")
