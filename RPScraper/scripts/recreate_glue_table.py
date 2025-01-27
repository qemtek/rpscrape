#!/usr/bin/env python3
"""
Script to recreate the Glue table in the rpscrape database.
This will:
1. Create the rpscrape database if it doesn't exist
2. Drop the existing rpscrape table if it exists
3. Create a new rpscrape table with the correct schema and location
"""

import awswrangler as wr
import pandas as pd
from settings import (
    S3_GLUE_PATH,
    AWS_GLUE_DB,
    AWS_RPSCRAPE_TABLE_NAME,
    boto3_session,
    SCHEMA_COLUMNS
)

def get_pandas_dtype(glue_type: str) -> str:
    """Convert Glue type to pandas dtype"""
    type_map = {
        'int': 'Int64',  # Nullable integer
        'double': 'float64',
        'string': 'object',  # Use object for strings
        'timestamp': 'datetime64[ns]'
    }
    return type_map[glue_type]

def main():
    """Recreate the Glue table"""
    # Create empty DataFrame with correct schema
    pandas_dtypes = {col: get_pandas_dtype(dtype) for col, dtype in SCHEMA_COLUMNS.items()}
    df = pd.DataFrame(columns=pandas_dtypes.keys())
    for col, dtype in pandas_dtypes.items():
        df[col] = pd.Series(dtype=dtype)

    # Create database if it doesn't exist
    databases = wr.catalog.databases(boto3_session=boto3_session)
    if AWS_GLUE_DB not in databases:
        wr.catalog.create_database(
            name=AWS_GLUE_DB,
            description='Database for RPScrape data',
            boto3_session=boto3_session,
            exist_ok=True
        )
        print(f"Created database: {AWS_GLUE_DB}")
    else:
        print(f"Using existing database: {AWS_GLUE_DB}")

    # Drop table if it exists
    try:
        wr.catalog.delete_table_if_exists(
            database=AWS_GLUE_DB,
            table=AWS_RPSCRAPE_TABLE_NAME,
            boto3_session=boto3_session
        )
        print(f"Dropped existing table: {AWS_RPSCRAPE_TABLE_NAME}")
    except Exception as e:
        print(f"Error dropping table: {str(e)}")

    # Create new table
    try:
        wr.s3.to_parquet(
            df=df,
            path=S3_GLUE_PATH,
            dataset=True,
            mode='overwrite',
            database=AWS_GLUE_DB,
            table=AWS_RPSCRAPE_TABLE_NAME,
            boto3_session=boto3_session,
            partition_cols=['country', 'date'],
            compression='snappy',
            dtype=SCHEMA_COLUMNS  # Use Glue types directly
        )
        print(f"Created new table: {AWS_RPSCRAPE_TABLE_NAME}")
        print(f"Table location: {S3_GLUE_PATH}")
    except Exception as e:
        print(f"Error creating table: {str(e)}")

if __name__ == '__main__':
    main()
