import awswrangler as wr
import boto3
import datetime as dt
import pandas as pd
import subprocess

from settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

DATABASE = 'finish-time-predict'

boto3_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='eu-west-1'
)

df_gb = wr.athena.read_sql_query("select distinct country, date from rpscrape where country = 'gb'",
                                 database=DATABASE, boto3_session=boto3_session)
df_ire = wr.athena.read_sql_query("select distinct country, date from rpscrape where country = 'ire'",
                                  database=DATABASE, boto3_session=boto3_session)
# df_usa = wr.athena.read_sql_query("select distinct country, date from rpscrape where country = 'usa'",
#                                   database=DATABASE, boto3_session=boto3_session)
# df_aus = wr.athena.read_sql_query("select distinct country, date from rpscrape where country = 'aus'",
#                                   database=DATABASE, boto3_session=boto3_session)

df = wr.athena.read_sql_query("select * from rpscrape", database=DATABASE, boto3_session=boto3_session)

d1 = pd.to_datetime('2014-04-05')
d2 = pd.to_datetime(dt.datetime.today().date() - dt.timedelta(days=1))

# this will give you a list containing all of the dates
dd = [d1 + dt.timedelta(days=x) for x in range((d2-d1).days + 1)]

missing_dates_gb = [d for d in dd if d not in list(df_gb['date'].unique())]
missing_dates_ire = [d for d in dd if d not in list(df_ire['date'].unique())]
# missing_dates_fr = [d for d in dd if d not in list(df_fr['date'].unique())]
# missing_dates_usa = [d for d in dd if d not in list(df_usa['date'].unique())]
# missing_dates_aus = [d for d in dd if d not in list(df_aus['date'].unique())]

def run_rpscrape(country, date):
    subprocess.call(f'python3 scripts/rpscrape.py -d {date} -r {country}', shell=True)
    print(f'Finished scraping {country} - {date}')


for date in missing_dates_gb:
    run_rpscrape('gb', str(date.date()).replace('-', '/'))

for date in missing_dates_ire:
    run_rpscrape('ire', str(date.date()).replace('-', '/'))

# for date in missing_dates_aus:
#     run_rpscrape('aus', str(date.date()).replace('-', '/'))
#
# for date in missing_dates_usa:
#     run_rpscrape('usa', str(date.date()).replace('-', '/'))

# for date in missing_dates_fr:
#     run_rpscrape('fr', str(date.date()).replace('-', '/'))
