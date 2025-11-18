#!/usr/bin/env python3
"""
Check the actual Glue table structure to verify partitioning
"""

import sys
sys.path.insert(0, '/Users/christophercollins/Documents/GitHub/rpscrape/RPScraper')

import awswrangler as wr
from settings import (
    AWS_GLUE_DB,
    AWS_RPSCRAPE_TABLE_NAME,
    S3_GLUE_PATH,
    boto3_session
)

print("="*80)
print("CHECKING GLUE TABLE STRUCTURE")
print("="*80)

# Check if database exists
print(f"\n1. Checking database: {AWS_GLUE_DB}")
try:
    databases = wr.catalog.databases(boto3_session=boto3_session)
    if AWS_GLUE_DB in databases.values:
        print(f"   ✓ Database exists: {AWS_GLUE_DB}")
    else:
        print(f"   ✗ Database NOT found: {AWS_GLUE_DB}")
        print(f"   Available databases: {list(databases.values)}")
        sys.exit(1)
except Exception as e:
    print(f"   ✗ Error checking database: {e}")
    sys.exit(1)

# Check if table exists and get its details
print(f"\n2. Checking table: {AWS_RPSCRAPE_TABLE_NAME}")
try:
    tables = wr.catalog.tables(database=AWS_GLUE_DB, boto3_session=boto3_session)
    if AWS_RPSCRAPE_TABLE_NAME in tables['Table'].values:
        print(f"   ✓ Table exists: {AWS_RPSCRAPE_TABLE_NAME}")
    else:
        print(f"   ✗ Table NOT found: {AWS_RPSCRAPE_TABLE_NAME}")
        print(f"   Available tables: {list(tables['Table'].values)}")
        sys.exit(1)
except Exception as e:
    print(f"   ✗ Error checking table: {e}")
    sys.exit(1)

# Get table metadata
print(f"\n3. Getting table metadata...")
try:
    table_metadata = wr.catalog.get_table_parameters(
        database=AWS_GLUE_DB,
        table=AWS_RPSCRAPE_TABLE_NAME,
        boto3_session=boto3_session
    )
    print(f"   Table parameters: {table_metadata}")
except Exception as e:
    print(f"   Error getting table parameters: {e}")

# Get partition columns
print(f"\n4. Checking partition columns...")
try:
    # Get full table details using boto3
    glue_client = boto3_session.client('glue')
    response = glue_client.get_table(
        DatabaseName=AWS_GLUE_DB,
        Name=AWS_RPSCRAPE_TABLE_NAME
    )

    table = response['Table']
    partition_keys = table.get('PartitionKeys', [])

    if partition_keys:
        print(f"   ✓ Table IS partitioned!")
        print(f"   Partition columns:")
        for pk in partition_keys:
            print(f"     - {pk['Name']} ({pk['Type']})")
    else:
        print(f"   ✗ Table is NOT partitioned!")
        print(f"   No partition keys found")

    # Show table location
    storage_descriptor = table.get('StorageDescriptor', {})
    location = storage_descriptor.get('Location', 'N/A')
    print(f"\n   Table location: {location}")

    # Show columns
    columns = storage_descriptor.get('Columns', [])
    print(f"\n   Number of columns: {len(columns)}")

except Exception as e:
    print(f"   ✗ Error getting partition info: {e}")
    import traceback
    traceback.print_exc()

# Check partitions if table is partitioned
print(f"\n5. Checking existing partitions...")
try:
    partitions = wr.catalog.get_partitions(
        database=AWS_GLUE_DB,
        table=AWS_RPSCRAPE_TABLE_NAME,
        boto3_session=boto3_session
    )

    if partitions:
        print(f"   ✓ Found {len(partitions)} partitions")
        print(f"\n   First 10 partitions:")
        for i, partition_values in enumerate(list(partitions.keys())[:10]):
            print(f"     {i+1}. {partition_values}")
    else:
        print(f"   ✗ No partitions found")

except Exception as e:
    print(f"   Note: {e}")
    print(f"   (This is expected if table is not partitioned)")

# Check S3 structure
print(f"\n6. Checking S3 file structure at: {S3_GLUE_PATH}")
try:
    s3_files = wr.s3.list_objects(S3_GLUE_PATH, boto3_session=boto3_session)

    if s3_files:
        print(f"   ✓ Found {len(s3_files)} files/folders")
        print(f"\n   First 15 items (showing structure):")
        for i, path in enumerate(s3_files[:15]):
            # Remove S3 bucket prefix to show relative path
            relative_path = path.replace(S3_GLUE_PATH, '').lstrip('/')
            print(f"     {i+1}. {relative_path}")

        # Check if structure shows partitioning
        if any('country=' in f for f in s3_files[:20]):
            print(f"\n   ✓ S3 structure SHOWS partition folders (country=...)")
        elif any('date=' in f for f in s3_files[:20]):
            print(f"\n   ✓ S3 structure SHOWS partition folders (date=...)")
        else:
            print(f"\n   ✗ S3 structure does NOT show partition folders")
            print(f"      Files appear to be in flat structure")
    else:
        print(f"   ✗ No files found at {S3_GLUE_PATH}")

except Exception as e:
    print(f"   ✗ Error checking S3: {e}")

# Query table to check actual data
print(f"\n7. Querying table to check data...")
try:
    query = f"SELECT COUNT(*) as total_rows FROM {AWS_RPSCRAPE_TABLE_NAME} LIMIT 1"
    df = wr.athena.read_sql_query(
        query,
        database=AWS_GLUE_DB,
        boto3_session=boto3_session
    )
    print(f"   ✓ Total rows in table: {df['total_rows'].values[0]:,}")

    # Check for duplicates
    dup_query = f"""
    SELECT race_id, horse_id, COUNT(*) as cnt
    FROM {AWS_RPSCRAPE_TABLE_NAME}
    GROUP BY race_id, horse_id
    HAVING COUNT(*) > 1
    LIMIT 10
    """

    print(f"\n   Checking for duplicates on (race_id, horse_id)...")
    df_dups = wr.athena.read_sql_query(
        dup_query,
        database=AWS_GLUE_DB,
        boto3_session=boto3_session
    )

    if len(df_dups) > 0:
        print(f"   ⚠️  DUPLICATES FOUND! {len(df_dups)} duplicate (race_id, horse_id) pairs")
        print(f"\n   Sample duplicates:")
        print(df_dups.head(10))
    else:
        print(f"   ✓ No duplicates found")

except Exception as e:
    print(f"   ✗ Error querying table: {e}")

print("\n" + "="*80)
print("CHECK COMPLETE")
print("="*80)
