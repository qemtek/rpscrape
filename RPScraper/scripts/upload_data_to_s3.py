import sys
# print(f"Arg supplied to upload_data_to_s3.py: {sys.argv[1]}")

import awswrangler as wr
import pandas as pd
import os
import pyarrow
import numpy as np
from datetime import datetime

from settings import PROJECT_DIR, S3_BUCKET, AWS_GLUE_DB, AWS_RPSCRAPE_TABLE_NAME,\
    SCHEMA_COLUMNS, boto3_session, COL_DTYPES, OUTPUT_COLS
from utils.general import clean_data


df_all_dir = f'tmp/df_all.csv'


def append_to_pdataset(local_path, folder, mode='a', header=False, index=False):
    try:
        if folder == 'data/dates':
            df = pd.read_csv(local_path, engine='python')
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
            # Add created_at timestamp
            df['created_at'] = datetime.now().isoformat()
            for c in list(SCHEMA_COLUMNS.keys()):
                if c not in df.columns:
                    df[c] = None
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
    # Get all files currently in S3
    folders = os.listdir(f"{folder}/")
    folders = [f for f in folders if 'DS_Store' not in f and '.keep' not in f
               and '.ipynb_checkpoints' not in f]
    print(f"Folders found: {folders}")
    first_row = True
    for country in folders:
        print(f"Loading data for country: {country}")
        files = os.listdir(f"{folder}/{country}/")
        files = [f for f in files if 'DS_Store' not in f and '.keep' not in f
                 and '.ipynb_checkpoints' not in f and '.csv' in f]
        print(f"Adding {len(files)} files")
        assert len(files) > 0, 'There are no files to upload'
        # Download / Upload the first file manually with overwrite
        filename = f"{folder}/{country}/{files[0]}"
        if first_row:
            append_to_pdataset(filename, mode='w', header=True, folder=folder)
            first_row = False
            start = 1
        else:
            start = 0
        files = files[start:]
        for file in files:
            filename = f"{folder}/{country}/{file}"
            print(filename)
            append_to_pdataset(local_path=filename, folder=folder)

    # Upload the dataframe to the /datasets/ directory in S3
    if os.path.exists(df_all_dir):
        df = pd.read_csv(df_all_dir, engine='python')
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
                elif value == 'double':
                    df.loc[~df[key].isna(), key] = df.loc[~df[key].isna(), key].astype(np.float32)

        print(f"Finally uploading {len(df)} rows")
        wr.s3.to_parquet(df[OUTPUT_COLS], path=f's3://{S3_BUCKET}/datasets/', dataset=True,
                         dtype=SCHEMA_COLUMNS, mode='overwrite' if full_refresh else 'append',
                         boto3_session=boto3_session, database=AWS_GLUE_DB, table=AWS_RPSCRAPE_TABLE_NAME)
        print(f"Uploaded data to parquet dataset")
        wr.s3.to_csv(df[OUTPUT_COLS], f's3://rpscrape/data_agg/df_all.csv', boto3_session=boto3_session)
        print(f"Uploaded backup dataset to s3://{S3_BUCKET}/datasets/")


if __name__ == '__main__':
    # refresh = str(sys.argv[1])
    # refresh = refresh == 'true'
    df_all_dir = f"tmp/df_all.csv"
    refresh=False
    print(f"refresh = {refresh}")
    upload_local_files_to_dataset(full_refresh=refresh)
