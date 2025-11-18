#!/usr/bin/env python3
"""
Test that AWS Wrangler correctly updates Glue partitions
"""
import sys
sys.path.insert(0, '/Users/christophercollins/Documents/GitHub/rpscrape/RPScraper')

import pandas as pd
import awswrangler as wr
from settings import boto3_session

# Create a test partitioned table
test_db = 'finish-time-predict'
test_table = 'rpscrape_partition_test'
test_path = 's3://rpscrape/test_partitions/'

# Test data
df = pd.DataFrame({
    'race_id': [1, 2, 3],
    'horse_id': [100, 101, 102],
    'country': ['gb', 'gb', 'ire'],
    'date': pd.to_datetime(['2025-11-15', '2025-11-15', '2025-11-16']),
    'value': [10, 20, 30]
})

print("=" * 80)
print("PARTITION UPDATE TEST")
print("=" * 80)
print()

# Clean up old test table if exists
print("1. Cleaning up old test table...")
try:
    wr.catalog.delete_table_if_exists(
        database=test_db,
        table=test_table,
        boto3_session=boto3_session
    )
    # Delete S3 data
    wr.s3.delete_objects(test_path, boto3_session=boto3_session)
    print("   ✓ Cleaned up")
except Exception as e:
    print(f"   (No cleanup needed: {e})")
print()

# Write with partitions
print("2. Writing data WITH partitions (country, date)...")
wr.s3.to_parquet(
    df=df,
    path=test_path,
    dataset=True,
    mode='overwrite',
    database=test_db,
    table=test_table,
    partition_cols=['country', 'date'],  # ← Partitioning!
    boto3_session=boto3_session,
    dtype={
        'race_id': 'int',
        'horse_id': 'int',
        'country': 'string',
        'date': 'timestamp',
        'value': 'int'
    }
)
print("   ✓ Data written to S3")
print()

# Check if partitions were registered
print("3. Checking if Glue knows about partitions...")
partitions = wr.catalog.get_partitions(
    database=test_db,
    table=test_table,
    boto3_session=boto3_session
)
print(f"   Partitions found in Glue catalog: {len(partitions)}")
for partition_values in partitions.keys():
    print(f"     - {partition_values}")
print()

# Query the data to prove it works
print("4. Querying data (should return 3 rows)...")
query = f"SELECT * FROM {test_table}"
df_result = wr.athena.read_sql_query(
    query,
    database=test_db,
    boto3_session=boto3_session
)
print(f"   Rows returned: {len(df_result)}")
print(df_result.to_string(index=False))
print()

# Now ADD MORE DATA to NEW partition
print("5. Adding NEW partition (gb, 2025-11-17)...")
df_new = pd.DataFrame({
    'race_id': [4, 5],
    'horse_id': [103, 104],
    'country': ['gb', 'gb'],
    'date': pd.to_datetime(['2025-11-17', '2025-11-17']),
    'value': [40, 50]
})

wr.s3.to_parquet(
    df=df_new,
    path=test_path,
    dataset=True,
    mode='append',  # ← Appending to existing table
    database=test_db,
    table=test_table,
    partition_cols=['country', 'date'],
    boto3_session=boto3_session,
    dtype={
        'race_id': 'int',
        'horse_id': 'int',
        'country': 'string',
        'date': 'timestamp',
        'value': 'int'
    }
)
print("   ✓ New data written")
print()

# Check partitions again
print("6. Checking partitions AFTER append...")
partitions = wr.catalog.get_partitions(
    database=test_db,
    table=test_table,
    boto3_session=boto3_session
)
print(f"   Partitions found: {len(partitions)}")
for partition_values in partitions.keys():
    print(f"     - {partition_values}")
print()

# Query to prove new partition is visible
print("7. Querying ALL data (should return 5 rows now)...")
df_result = wr.athena.read_sql_query(
    query,
    database=test_db,
    boto3_session=boto3_session
)
print(f"   Rows returned: {len(df_result)}")
print(df_result.to_string(index=False))
print()

print("=" * 80)
print("TEST RESULTS")
print("=" * 80)
if len(df_result) == 5 and len(partitions) >= 3:
    print("✅ SUCCESS!")
    print("   - Partitions automatically registered in Glue catalog")
    print("   - New partitions appear after append")
    print("   - No MSCK REPAIR needed")
    print("   - Data immediately queryable")
else:
    print("❌ FAILED!")
    print(f"   Expected 5 rows, got {len(df_result)}")
    print(f"   Expected 3+ partitions, got {len(partitions)}")
print()

# Cleanup
print("Cleaning up test table...")
wr.catalog.delete_table_if_exists(
    database=test_db,
    table=test_table,
    boto3_session=boto3_session
)
wr.s3.delete_objects(test_path, boto3_session=boto3_session)
print("✓ Done")
