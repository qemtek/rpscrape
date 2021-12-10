import awswrangler as wr
import pandas as pd
import time
import os
import pyarrow
import numpy as np

from apscheduler.schedulers.background import BackgroundScheduler

from RPScraper.settings import PROJECT_DIR, S3_BUCKET, AWS_GLUE_DB, AWS_GLUE_TABLE,\
    SCHEMA_COLUMNS, boto3_session, COL_DTYPES, OUTPUT_COLS
from RPScraper.src.utils.general import clean_data


df_all_dir = f'{PROJECT_DIR}/tmp/df_all.csv'


def append_to_pdataset(local_path, folder, mode='a', header=False, index=False):
    try:
        if folder == 'data/dates':
            df = pd.read_csv(local_path,  warn_bad_lines=True, error_bad_lines=False, engine='python')
            cols = df.columns
            for key, value in COL_DTYPES.items():
                if key in cols:
                    if value in ['int', 'double']:
                        df[key] = pd.to_numeric(df[key], errors='coerce')
            if len(df) > 0:
                country = local_path.split('/')[-2]
                df = clean_data(df, country=country)
        elif folder == 's3_data':
            df = pd.read_parquet(local_path)
        if len(df) > 0:
            df['pos'] = df['pos'].astype(str)
            df['pattern'] = df['pattern'].astype(str)
            df['prize'] = df['prize'].astype(str)
            df['date'] = pd.to_datetime(df['date'])
            df['year'] = df['date'].apply(lambda x: x.year)
            df = df[list(SCHEMA_COLUMNS.keys())]
            mode = 'a' if os.path.exists(df_all_dir) else 'w'
            header = False if os.path.exists(df_all_dir) else True
            for col in OUTPUT_COLS:
                if col in df.columns:
                    pass
                else:
                    # Dont append the dataframe if there is a column mismatch
                    print(f"Skipping day with bad data: {df[['date', 'course']].unique()}")
                    return None
            df[OUTPUT_COLS].to_csv(df_all_dir, mode=mode, header=header, index=index)
            with open(df_all_dir, 'a') as f_out:
                f_out.write('\n')

    except pyarrow.lib.ArrowInvalid as e:
        print(f"Loading parquet file failed. \nFile path: {local_path}. \nError: {e}")


def upload_local_files_to_dataset(folder='data/dates', full_refresh=False):
    scheduler2 = BackgroundScheduler()
    # Get all files currently in S3
    folders = os.listdir(f"{PROJECT_DIR}/{folder}/")
    folders = [f for f in folders if 'DS_Store' not in f and '.keep' not in f
               and '.ipynb_checkpoints' not in f]
    print(f"Folders found: {folders}")
    first_row = True
    for country in folders:
        print(f"Loading data for country: {country}")
        files = os.listdir(f"{PROJECT_DIR}/{folder}/{country}/")
        files = [f for f in files if 'DS_Store' not in f and '.keep' not in f
                 and '.ipynb_checkpoints' not in f and '.csv' in f]
        print(f"Adding {len(files)} files")
        assert len(files) > 0, 'There are no files to upload'
        # Download / Upload the first file manually with overwrite
        filename = f"{PROJECT_DIR}/{folder}/{country}/{files[0]}"
        if first_row:
            append_to_pdataset(filename, mode='w', header=True, folder=folder)
            first_row = False
            start = 1
        else:
            start = 0
        files = files[start:]
        for file in files:
            filename = f"{PROJECT_DIR}/{folder}/{country}/{file}"
            print(filename)
            scheduler2.add_job(func=append_to_pdataset, kwargs={"local_path": filename, "folder": folder},
                               id=f"{country}_{file.split('/')[-1]}", replace_existing=True,
                               misfire_grace_time=999999999)
    scheduler2.start()
    time.sleep(1)
    print(f"Jobs left: {len(scheduler2._pending_jobs)}")
    time.sleep(1)
    while len(scheduler2._pending_jobs) > 0:
        print(f"Jobs left: {len(scheduler2._pending_jobs)}")
    scheduler2.shutdown()

    # Upload the dataframe to the /datasets/ directory in S3
    if os.path.exists(df_all_dir):
        df = pd.read_csv(df_all_dir, error_bad_lines=False, warn_bad_lines=True)
        print(f"Loaded {len(df)} rows")
        # Do some checks to remove bad rows
        df = df[~df['country'].isna()]
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['bad_date'] = df['date'].isnull()
        print(f"Found {sum(df['bad_date'])} bad date rows")
        df = df[~df['bad_date']]
        print(f"There are now {len(df)} rows")

        cols = df.columns
        for key, value in SCHEMA_COLUMNS.items():
            if key in cols:
                if value in ['int', 'double']:
                    df[key] = pd.to_numeric(df[key], errors='coerce')

        for key, value in SCHEMA_COLUMNS.items():
            if key in df.columns:
                if value == 'string':
                    df[key] = df[key].astype(str)
                    df[key] = df[key].fillna(pd.NA)
                elif value == 'int':
                    df.loc[~df[key].isna(), key] = df.loc[~df[key].isna(), key].astype(np.int32)
                    #df[key] = df[key].fillna(pd.NA)
                elif value == 'double':
                    df.loc[~df[key].isna(), key] = df.loc[~df[key].isna(), key].astype(np.float32)
                    #df[key] = df[key].fillna(pd.NA)

        print(f"Finally uploading {len(df)} rows")
        wr.s3.to_parquet(df[OUTPUT_COLS], path=f's3://{S3_BUCKET}/datasets/', dataset=True,
                         dtype=SCHEMA_COLUMNS, mode='overwrite' if full_refresh else 'append',
                         boto3_session=boto3_session, database=AWS_GLUE_DB, table=AWS_GLUE_TABLE,
                         partition_cols=['year'])
        print(f"Uploaded data to parquet dataset")


if __name__ == '__main__':
    df_all_dir = f"{PROJECT_DIR}/tmp/df_all.csv"
    upload_local_files_to_dataset(full_refresh=True)
