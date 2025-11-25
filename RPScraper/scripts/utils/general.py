import datetime as dt
import pandas as pd
import awswrangler as wr
import os

from settings import PROJECT_DIR, S3_BUCKET


def convert_off_to_readable_format(x):
    """Convert the 'Off' column into a time object.

    Handles both old format (6:45) and new 24-hour format (18:45).
    """
    x = str(x).strip()

    # New format: already in 24-hour time (e.g., '18:45')
    if ':' in x and len(x.split(':')[0]) == 2:
        try:
            return str(dt.datetime.strptime(x, '%H:%M').time())
        except ValueError:
            pass

    # Old format: needs AM/PM conversion (e.g., '6:45')
    if x[0:2] not in ['10', '11', '12']:
        x = '0' + x + ' PM'
    else:
        if x[0:2] == '12':
            x = x + ' PM'
        else:
            x = x + ' AM'
    return str(dt.datetime.strptime(x, '%I:%M %p').time())


def clean_name(x, illegal_symbols="'$@#^(%*)._ ", append_with=None):
    x = str(x).lower().strip()
    while x[0].isdigit():
        x = x[1:]
    # Remove any symbols, including spaces
    for s in illegal_symbols:
        x = x.replace(s, "")
    if append_with is not None:
        x = f"{x}_{append_with}"
    return x


def clean_horse_name(x, illegal_symbols="'$@#^(%*) ", append_with=None):
    # Remove the country part
    try:
        if str(x) == 'nan':
            return 'none'
        else:
            x = ' '.join(str(x).split(' ')[:-1])
            x = clean_name(x, illegal_symbols=illegal_symbols, append_with=append_with)
    except Exception as e:
        print(f"clean_horse_name failed. x: {x}. Error: {e}")
    return x


def nullify_non_finishers(x):
    x['time'] = x['time'] if str(x['pos']).isdigit() else None
    return x


def clean_data(df_in, country):
    """Perform all 'data cleansing' steps on raw RPScraper data
    """
    df = df_in.copy()
    # Make all columns lower case
    df.columns = [col.lower() for col in df_in.columns]
    # Add country
    df['country'] = country
    # Drop duplicates
    df = df.drop_duplicates()
    # Convert the 'Off' column into a time object
    df['off'] = df['off'].apply(lambda x: convert_off_to_readable_format(x))
    df['time'] = df['secs']
    # Create a unique identifier for each race
    # Clean up horse name (remove the country indicator from the end and make lower case)
    df['horse_cleaned'] = df['horse'].apply(lambda x: clean_horse_name(x))
    # Clean up dam name (remove the country indicator from the end and make lower case)
    df['dam_cleaned'] = df['dam'].apply(lambda x: clean_horse_name(x))
    # Clean up sire name (remove the country indicator from the end and make lower case)
    df['sire_cleaned'] = df['sire'].apply(lambda x: clean_horse_name(x))
    # Add dam and sire names to horse name to make it unique
    df['horse_cleaned'] = df.apply(lambda x: f"{x['horse_cleaned']}_{x['dam_cleaned']}_{x['sire_cleaned']}", axis=1)
    return df


def upload_csv_to_s3(country, date):
    file_name = f"{str(date).replace('/', '_')}"
    try:
        df = pd.read_csv(f"{PROJECT_DIR}/data/{country}/{file_name}.csv")
        if len(df) > 0 and df is not None:
            # Apply some preprocessing steps
            df = clean_data(df, country)
            df['pos'] = df['pos'].astype(str)
            df['pattern'] = df['pattern'].astype(str)
            df['prize'] = df['prize'].astype(str)
            df['date'] = pd.to_datetime(df['event_dt'])
            df['year'] = df['date'].apply(lambda x: x.year)
            # Upload to S3
            new_file_name = f"{country}_{file_name.replace('_', '-')}"
            s3_path = f"s3://{S3_BUCKET}/data/{new_file_name}.parquet"
            wr.s3.to_parquet(df, s3_path)
            # Upload to parquet dataset
            # wr.s3.to_parquet(df, path='s3://RPScraper/datasets/', dataset=True, database='finish-time-predict',
            #                  table='rpscrape', dtype=SCHEMA_COLUMNS, mode='append', boto3_session=session)
            print(f"Finished uploading to S3 {country} - {date}")
            os.remove(f"{PROJECT_DIR}/data/{country}/{file_name}.csv")
            print(f"Finished clean up {country} - {date}")
    except:
        print("Upload failed, the file was likely empty")
        os.remove(f"{PROJECT_DIR}/data/{country}/{file_name}.csv")
