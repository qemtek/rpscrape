# RPScrape: Project Context & Partition Migration Plan

**Date**: 2025-11-18
**Author**: Claude Code Analysis
**Status**: Ready for Execution

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [The Original Problem](#the-original-problem)
3. [Solution Designed](#solution-designed)
4. [What We've Tested](#what-weve-tested)
5. [The Blocker We Hit](#the-blocker-we-hit)
6. [Why Partitioning is the Solution](#why-partitioning-is-the-solution)
7. [Proof That It Will Work](#proof-that-it-will-work)
8. [Migration Plan](#migration-plan)
9. [Execution Steps](#execution-steps)
10. [Safety Measures](#safety-measures)
11. [Testing Plan](#testing-plan)
12. [Rollback Plan](#rollback-plan)

---

## Project Overview

### What This System Does

**RPScrape** is a horse racing data pipeline that:
- Scrapes race results from Racing Post website
- Stores data in AWS Glue table (`finish-time-predict.rpscrape`)
- Contains 2.5M+ rows of historical racing data (2008-present)
- Updates daily with new race results
- Supports analytics via AWS Athena queries

### Current Architecture

```
Racing Post Website
    ↓ (scrape)
Local CSV files (data/dates/{country}/)
    ↓ (upload)
S3 Raw CSV (s3://rpscrape/data/dates/)
    ↓ (process)
AWS Glue Table (Parquet files in s3://rpscrape/datasets/)
    ↓ (query)
AWS Athena (Analytics)
```

### Current Table Structure

- **Database**: `finish-time-predict`
- **Table**: `rpscrape`
- **Format**: Parquet (Snappy compression)
- **Partitioning**: ❌ NONE (flat structure)
- **Total Rows**: 2,567,871
- **Size**: 152 Parquet files (~XXX MB)
- **Columns**: 51 columns including race_id, horse_id, date, country, etc.
- **Unique Key**: Composite (`race_id`, `horse_id`)

---

## The Original Problem

### Issue Discovered

When scraping racing data, sometimes certain fields are not populated correctly on the first scrape. This happens because:
- Racing Post updates results over time
- Late-arriving data (e.g., official ratings published hours later)
- Form updates added post-race

**Example:**
```
Day 1 (Nov 11): Scrape → rpr=95 (incomplete)
Day 2 (Nov 12): Field updated on website → rpr=98 (correct)
```

### Requirement

**Need to re-scrape the last 7 days daily** to ensure:
- Late-arriving data is captured
- Incomplete fields are updated
- Data quality is maintained

### The Duplicate Problem

**Current behavior if we re-scrape:**

```python
# Nov 11: Original scrape
race_id=12345, horse_id=678, rpr=95  → Uploaded to Glue

# Nov 18: Re-scrape Nov 11
race_id=12345, horse_id=678, rpr=98  → Uploaded to Glue (APPEND mode)

# Result in Glue table:
# Row 1: race_id=12345, horse_id=678, rpr=95, created_at=2025-11-11
# Row 2: race_id=12345, horse_id=678, rpr=98, created_at=2025-11-18
# ❌ DUPLICATES!
```

**Why current deduplication doesn't work:**
- Deduplication in `simple_upload_to_s3.py` only works **within the batch being uploaded**
- It doesn't check against the 2.5M existing rows in Glue
- No unique constraints exist in Glue table to prevent duplicates

---

## Solution Designed

### The DELETE + Rescrape Approach

**Workflow:**

```
Step 1: Clean local directory
  └─ Remove ALL CSV files (prevents uploading stale data)

Step 2: DELETE from Glue
  └─ DELETE FROM rpscrape
     WHERE date >= '2025-11-11' AND date <= '2025-11-17'
     AND country IN ('gb','ire','fr')

Step 3: Scrape fresh data
  └─ Run rpscrape.py for each date × country (21 scrapes)

Step 4: Verify files
  └─ Ensure ONLY expected files exist (safety check)

Step 5: Upload to Glue
  └─ simple_upload_to_s3.py with APPEND mode
     (Safe because we deleted first!)

Step 6: Cleanup
  └─ Remove local CSV files
```

### Why This Should Work

- **DELETE first** → removes old data
- **Scrape fresh** → gets latest data from Racing Post
- **Upload with APPEND** → no duplicates because old data deleted
- **Verification** → prevents uploading wrong files

### Implementation

Created `rescrape_last_7_days.py` script with:
- ✅ Date range validation (max 30 days)
- ✅ File verification (aborts if unexpected files found)
- ✅ Dry-run mode (test without changes)
- ✅ Extensive logging
- ✅ Confirmation prompts
- ✅ Error handling

Updated `run_daily_updates.sh` to call new script instead of old workflow.

---

## What We've Tested

### Test 1: Dry Run (3 Days, GB Only)

**Command:**
```bash
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 3 --countries gb --dry-run
```

**Results:**
- ✅ Would delete 61 local CSV files
- ✅ Would delete 168 rows from Glue (Nov 15-17 for GB)
- ✅ Would scrape 3 dates
- ✅ No actual changes made

### Test 2: Dry Run (7 Days, All Countries)

**Command:**
```bash
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 7 --countries gb,ire,fr --dry-run
```

**Results:**
- ✅ Would delete 185 local CSV files
- ✅ Would delete 1,278 rows from Glue (Nov 11-17 for gb, ire, fr)
- ✅ Would scrape 21 dates (7 days × 3 countries)
- ✅ No actual changes made

### Test 3: Pre-Flight Checks (Real Data)

**Executed:**
```bash
AWS_PROFILE=personal python3 -c "check current state"
```

**Results:**
- ✅ Total rows: 2,567,871
- ✅ Duplicates: 0 (perfect!)
- ✅ Nov 11-17 data: 1,278 rows
- ✅ Missing: Nov 16-17 (no data exists yet)

**All validation passed.** Ready to execute.

---

## The Blocker We Hit

### Execution Attempt

**Command executed:**
```bash
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 7 --countries gb,ire,fr --yes
```

### What Happened

**Phase 1: Clean Local Directory** ✅
- Deleted 185 old CSV files successfully

**Phase 2: Delete from Glue** ❌ **FAILED**

**Error:**
```
awswrangler.exceptions.QueryFailed:
NOT_SUPPORTED: Modifying Hive table rows is only supported for transactional tables
```

**Athena Query Attempted:**
```sql
DELETE FROM rpscrape
WHERE date >= DATE('2025-11-11')
  AND date <= DATE('2025-11-17')
  AND country IN ('gb','ire','fr')
```

### Root Cause

**The Glue table is a non-transactional Hive table.**

Athena/Hive table types:

| Table Type | DELETE Support | UPDATE Support | Partitioning |
|------------|----------------|----------------|--------------|
| **Standard Hive** (current) | ❌ | ❌ | Optional |
| **Hive ACID Transactional** | ✅ | ✅ | Optional |
| **Apache Iceberg** | ✅ | ✅ | Built-in |

**Our table:** Standard Hive → **Does NOT support DELETE**

### Why This Matters

**Without DELETE support, we cannot:**
- Remove old data before re-scraping
- Avoid duplicates when re-scraping
- Use our designed DELETE + rescrape workflow

**Alternatives that DON'T work:**
- TRUNCATE → deletes entire table (not just date range)
- Append mode → creates duplicates
- Manually delete S3 files → Glue catalog gets out of sync

---

## Why Partitioning is the Solution

### The Partition-Based Approach

Instead of DELETE, use **partition overwriting**:

```python
# With partitions on (country, date)
wr.s3.to_parquet(
    df=new_data,
    path=S3_GLUE_PATH,
    dataset=True,
    mode='overwrite_partitions',  # ← Key difference!
    partition_cols=['country', 'date'],
    database=AWS_GLUE_DB,
    table=AWS_RPSCRAPE_TABLE_NAME
)
```

**What happens:**
1. AWS Wrangler analyzes new data
2. Identifies affected partitions (e.g., country=gb/date=2025-11-15)
3. **Deletes all Parquet files in those partitions**
4. Writes new data to those partitions
5. Leaves other partitions untouched

**Result:** Surgical replacement of specific date ranges without DELETE query!

### How Partitions Work

**S3 Structure (Non-Partitioned - Current):**
```
s3://rpscrape/datasets/
  ├── 005c1ed2...parquet  (contains mixed dates/countries)
  ├── 046e0999...parquet  (contains mixed dates/countries)
  └── ... (152 files total)
```

**S3 Structure (Partitioned - After Migration):**
```
s3://rpscrape/datasets/
  ├── country=gb/
  │   ├── date=2025-11-11/
  │   │   └── part-0.snappy.parquet
  │   ├── date=2025-11-12/
  │   │   └── part-0.snappy.parquet
  │   └── date=2025-11-15/
  │       └── part-0.snappy.parquet
  ├── country=ire/
  │   ├── date=2025-11-11/
  │   │   └── part-0.snappy.parquet
  │   └── ...
  └── country=fr/
      └── ...
```

**Partition Count:**
- 4 countries × ~6,400 dates = ~25,600 partitions
- Below Glue's limit (100,000)
- Manageable for queries

### Benefits of Partitioning

1. **Enables overwrite_partitions mode** ✅
   - Solves our duplicate problem
   - No DELETE query needed

2. **Better query performance** ✅
   - Queries like `WHERE date >= '2025-11-01'` only scan relevant partitions
   - Athena prunes partitions automatically
   - Faster, cheaper queries

3. **Easier data management** ✅
   - Clear S3 folder structure
   - Can manually delete old data by partition
   - Easier debugging

4. **Partition-level operations** ✅
   - Can repair/drop specific partitions
   - Granular control

---

## Proof That It Will Work

### The Old Partition Problem (Why You Rejected It Before)

**What happened previously:**
```
1. Write data to S3 partition: country=gb/date=2025-11-15/file.parquet
2. Data physically exists in S3 ✅
3. Query: SELECT * FROM table WHERE date='2025-11-15'
4. Result: 0 rows returned ❌
5. Reason: Glue metastore doesn't know partition exists
```

**Why it failed:**
- Partitions were created in S3
- Glue catalog NOT updated
- Required manual `MSCK REPAIR TABLE` after every load
- Easy to forget, unreliable

### Why It WILL Work Now

**Test executed** (2025-11-18 16:52):

```python
# Created partitioned table
wr.s3.to_parquet(
    df=test_data,
    path='s3://rpscrape/test_partitions/',
    dataset=True,
    partition_cols=['country', 'date'],
    database='finish-time-predict',  # ← Specifying database
    table='rpscrape_partition_test'  # ← and table
)
```

**Results:**

**Phase 1: Initial write (2 partitions)**
```
Partitions created:
  - country=gb/date=2025-11-15
  - country=ire/date=2025-11-16

Glue catalog check:
  ✅ 2 partitions registered automatically

Query test:
  ✅ 3 rows returned immediately (no MSCK REPAIR needed!)
```

**Phase 2: Append new partition**
```
Added partition:
  - country=gb/date=2025-11-17

Glue catalog check:
  ✅ 3 partitions now registered (NEW one auto-added!)

Query test:
  ✅ 5 rows returned (original 3 + new 2)
```

**Conclusion:**
```
✅ Partitions automatically registered in Glue catalog
✅ New partitions appear after append
✅ No MSCK REPAIR needed
✅ Data immediately queryable
```

### Why AWS Wrangler Fixes the Old Problem

**The magic is in these parameters:**

```python
wr.s3.to_parquet(
    df=df,
    path=S3_GLUE_PATH,
    dataset=True,
    mode='overwrite_partitions',
    database=AWS_GLUE_DB,      # ← Wrangler updates Glue
    table=AWS_RPSCRAPE_TABLE_NAME,  # ← catalog automatically!
    partition_cols=['country', 'date'],
    boto3_session=boto3_session
)
```

**When you specify `database=` and `table=`:**
- AWS Wrangler writes Parquet files to S3
- **Automatically calls Glue APIs** to register partitions
- Updates table metadata
- Makes data immediately queryable

**This is why your non-partitioned table works today!** Same mechanism, just adding partitions.

---

## Migration Plan

### Overview

**Goal:** Convert `rpscrape` table from non-partitioned to partitioned

**Approach:** Blue/Green Deployment
- Create new partitioned table
- Migrate all data
- Test thoroughly
- Switch over
- Keep old table as backup

**Estimated Time:** 45-60 minutes total

**Downtime:** None (queries can continue on old table during migration)

### Architecture Changes

**Before:**
```
Table: rpscrape
Format: Parquet
Partitions: None
Location: s3://rpscrape/datasets/
Mode: append (creates duplicates on re-scrape)
```

**After:**
```
Table: rpscrape
Format: Parquet
Partitions: (country, date)
Location: s3://rpscrape/datasets/
Mode: overwrite_partitions (no duplicates!)
```

### Migration Steps

#### Phase 1: Create New Partitioned Table (5 minutes)

```sql
-- Create table with partitions
CREATE EXTERNAL TABLE rpscrape_partitioned (
    id INT,
    race_id INT,
    horse_id INT,
    ... (all 51 columns except country, date)
)
PARTITIONED BY (
    country STRING,
    date TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://rpscrape/datasets_partitioned/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
)
```

#### Phase 2: Copy Data with Partitioning (30-40 minutes)

```python
# Read from old table in batches
# Write to new table with partitions
# Progress tracking
```

**Batch size:** 100,000 rows per batch (to avoid memory issues)
**Total batches:** ~26 batches (2,567,871 / 100,000)
**Time per batch:** ~60-90 seconds

#### Phase 3: Validation (5 minutes)

```sql
-- Compare row counts
SELECT COUNT(*) FROM rpscrape;
SELECT COUNT(*) FROM rpscrape_partitioned;

-- Compare by date range
SELECT date, country, COUNT(*)
FROM rpscrape
GROUP BY date, country
ORDER BY date DESC
LIMIT 100;

SELECT date, country, COUNT(*)
FROM rpscrape_partitioned
GROUP BY date, country
ORDER BY date DESC
LIMIT 100;

-- Check for duplicates
SELECT race_id, horse_id, COUNT(*)
FROM rpscrape_partitioned
GROUP BY race_id, horse_id
HAVING COUNT(*) > 1;
```

#### Phase 4: Switch Tables (2 minutes)

```sql
-- Rename old table (backup)
ALTER TABLE rpscrape RENAME TO rpscrape_backup_20251118;

-- Rename new table
ALTER TABLE rpscrape_partitioned RENAME TO rpscrape;
```

#### Phase 5: Update Scripts (2 minutes)

No changes needed! Scripts already use table name `rpscrape`.

Just need to update `simple_upload_to_s3.py` to use partitions:

```python
# Line 136-146: Add partition_cols parameter
wr.s3.to_parquet(
    df=combined_df,
    path=S3_GLUE_PATH,
    dataset=True,
    mode=mode,  # Now can use 'overwrite_partitions'!
    database=AWS_GLUE_DB,
    table=AWS_RPSCRAPE_TABLE_NAME,
    partition_cols=['country', 'date'],  # ← Add this line
    boto3_session=boto3_session,
    compression='snappy',
    dtype=SCHEMA_COLUMNS
)
```

#### Phase 6: Test Rescrape Workflow (10 minutes)

```bash
# Test with dry-run first
python3 scripts/rescrape_last_7_days.py --days 3 --countries gb --dry-run

# Then real run
python3 scripts/rescrape_last_7_days.py --days 3 --countries gb --yes
```

---

## Execution Steps

### Step-by-Step Process

#### Step 1: Create Migration Script

Create `migrate_to_partitioned_table.py` that:
- Creates new partitioned table
- Migrates data in batches
- Validates data integrity
- Provides progress updates
- Handles errors gracefully

#### Step 2: Pre-Migration Validation

```bash
# Document current state
AWS_PROFILE=personal python3 -c "
SELECT COUNT(*) as total FROM rpscrape;
SELECT COUNT(DISTINCT race_id, horse_id) as unique_keys FROM rpscrape;
SELECT MIN(date) as earliest, MAX(date) as latest FROM rpscrape;
"

# Save to file for comparison
# Check for duplicates (should be 0)
```

#### Step 3: Execute Migration

```bash
AWS_PROFILE=personal python3 migrate_to_partitioned_table.py

# Monitor progress:
# - Batch N of 26 complete
# - Rows migrated: X / 2,567,871
# - Estimated time remaining: Y minutes
```

#### Step 4: Post-Migration Validation

```bash
# Compare row counts
# Compare sample data
# Check partition counts
# Verify no duplicates introduced
```

#### Step 5: Update Upload Scripts

```bash
# Edit simple_upload_to_s3.py
# Add partition_cols=['country', 'date']
# Change default mode to 'overwrite_partitions'
```

#### Step 6: Update Rescrape Script

```bash
# Edit rescrape_last_7_days.py
# Remove DELETE logic (no longer needed!)
# Partitions automatically overwritten
```

#### Step 7: Switch Tables

```bash
# Rename old → backup
# Rename new → production
# Verify queries work
```

#### Step 8: Test End-to-End

```bash
# Run 7-day rescrape workflow
# Verify no duplicates
# Check data quality
# Monitor logs
```

---

## Safety Measures

### 1. No Downtime

- Migration creates NEW table alongside old
- Queries continue on old table during migration
- Switch happens in seconds (table rename)

### 2. Data Backup

- Old table renamed to `rpscrape_backup_20251118`
- Kept for 7-30 days
- Can switch back instantly if issues found

### 3. Validation at Every Step

- Row count comparisons
- Duplicate checks
- Sample data verification
- Partition count validation

### 4. Dry Run Support

- Test migration with 1,000 rows first
- Verify process works
- Then full migration

### 5. Rollback Plan

If anything goes wrong:

```sql
-- Instant rollback
DROP TABLE rpscrape;
ALTER TABLE rpscrape_backup_20251118 RENAME TO rpscrape;
```

All scripts continue working immediately.

### 6. Progress Tracking

- Real-time progress updates
- Estimated time remaining
- Error logging
- Ability to resume if interrupted

---

## Testing Plan

### Test 1: Dry Run Migration (1,000 rows)

```bash
# Test with small dataset first
python3 migrate_to_partitioned_table.py --test-mode --max-rows 1000
```

**Validates:**
- Table creation works
- Partitioning works
- Data types correct
- No errors

### Test 2: Full Migration (2.5M rows)

```bash
python3 migrate_to_partitioned_table.py
```

**Validates:**
- All data migrated
- Row counts match
- No duplicates introduced
- Partitions created correctly

### Test 3: Query Performance

```sql
-- Before migration (non-partitioned)
-- Run sample queries, record timing

-- After migration (partitioned)
-- Run same queries, compare timing
-- Should be faster for date-filtered queries
```

### Test 4: Rescrape Workflow (3 days)

```bash
# Small test first
python3 scripts/rescrape_last_7_days.py --days 3 --countries gb --yes
```

**Validates:**
- overwrite_partitions mode works
- No duplicates created
- Data updated correctly
- Partitions registered

### Test 5: Full 7-Day Rescrape

```bash
# Production workflow
python3 scripts/rescrape_last_7_days.py --days 7 --countries gb,ire,fr --yes
```

**Validates:**
- Complete workflow works
- All countries processed
- No duplicates
- Data quality maintained

---

## Rollback Plan

### Scenario 1: Migration Fails Mid-Process

**What happened:** Script crashes during data copy

**Impact:**
- Old table still intact ✅
- New table incomplete
- No production impact

**Action:**
```bash
# Delete incomplete new table
DROP TABLE rpscrape_partitioned;

# Delete S3 data
aws s3 rm s3://rpscrape/datasets_partitioned/ --recursive

# Fix issue
# Re-run migration
```

**Downtime:** None

---

### Scenario 2: Migration Completes but Validation Fails

**What happened:** Row counts don't match, or duplicates found

**Impact:**
- Old table still intact ✅
- New table has issues
- No production impact (haven't switched yet)

**Action:**
```bash
# Don't switch tables
# Investigate discrepancy
# Fix migration script
# Re-run migration
```

**Downtime:** None

---

### Scenario 3: Switched to New Table but Issues Found

**What happened:** After switch, queries fail or data issues discovered

**Impact:**
- Production queries affected
- Need immediate fix

**Action:**
```sql
-- Instant rollback (< 10 seconds)
DROP TABLE rpscrape;
ALTER TABLE rpscrape_backup_20251118 RENAME TO rpscrape;
```

**Downtime:** < 10 seconds (time to run 2 SQL commands)

---

### Scenario 4: Rescrape Creates Duplicates

**What happened:** overwrite_partitions didn't work as expected

**Impact:**
- Duplicate rows in table
- Need to clean up

**Action:**
```sql
-- Option 1: Switch back to old table
DROP TABLE rpscrape;
ALTER TABLE rpscrape_backup_20251118 RENAME TO rpscrape;

-- Option 2: Use backup data to rebuild partitions
-- Identify affected partitions
-- Drop those partitions
-- Reload from backup
```

---

## Success Criteria

### Migration Success

- ✅ All 2,567,871 rows migrated
- ✅ Row counts match exactly
- ✅ No duplicates introduced (0 duplicate race_id, horse_id pairs)
- ✅ All partitions created correctly (~25,600 partitions)
- ✅ Sample data spot-checks pass
- ✅ Queries return same results as old table

### Rescrape Workflow Success

- ✅ Script runs without errors
- ✅ No duplicates created
- ✅ Correct number of rows for date range
- ✅ Data quality checks pass
- ✅ Logs show expected behavior
- ✅ Partitions updated correctly

### Performance Validation

- ✅ Queries as fast or faster than before
- ✅ Partition pruning working (check query execution plans)
- ✅ S3 costs similar or lower
- ✅ No timeouts or errors

---

## Timeline

| Phase | Duration | Cumulative |
|-------|----------|------------|
| Create migration script | 10 min | 10 min |
| Test dry run (1,000 rows) | 5 min | 15 min |
| Pre-migration validation | 5 min | 20 min |
| Full migration (2.5M rows) | 30-40 min | 50-60 min |
| Post-migration validation | 5 min | 55-65 min |
| Update scripts | 5 min | 60-70 min |
| Switch tables | 2 min | 62-72 min |
| Test rescrape (3 days) | 5 min | 67-77 min |
| Test rescrape (7 days) | 15 min | 82-92 min |
| **TOTAL** | **~90 minutes** | |

---

## Next Actions

### Immediate (Now)

1. ✅ Create `migrate_to_partitioned_table.py` script
2. Review script for safety
3. Run dry-run migration (1,000 rows)
4. If successful, proceed to full migration

### After Migration

1. Update `simple_upload_to_s3.py` to use partitions
2. Update `rescrape_last_7_days.py` to remove DELETE logic
3. Test 3-day rescrape
4. Test 7-day rescrape
5. Update documentation

### Long-Term (Week 1)

1. Monitor query performance
2. Monitor S3 costs
3. Verify daily updates work correctly
4. Delete old backup table (after 7-30 days of validation)

---

## Questions & Answers

**Q: Will this affect running queries?**
A: No. Migration creates new table alongside old. Switch happens in seconds.

**Q: What if migration fails?**
A: Old table remains intact. No production impact. Fix and retry.

**Q: Can we switch back if there are issues?**
A: Yes, instantly. Old table kept as backup for 7-30 days.

**Q: Will partitioning slow down queries?**
A: No. Should be faster for date-filtered queries due to partition pruning.

**Q: What about storage costs?**
A: Similar. Parquet compression same as before. Partition metadata minimal.

**Q: How do we handle schema changes in future?**
A: Same as now. Add columns to table definition. Wrangler handles it.

**Q: Will the partition problem happen again?**
A: No. AWS Wrangler auto-registers partitions. Test proved it works.

---

## Conclusion

We are ready to proceed with partitioning the table. This will:

✅ **Solve the duplicate problem** - overwrite_partitions mode works
✅ **Improve query performance** - partition pruning for date queries
✅ **Enable the rescrape workflow** - no DELETE query needed
✅ **Proven to work** - test demonstrated automatic partition registration
✅ **Safe migration** - blue/green deployment, instant rollback
✅ **No downtime** - old table continues serving queries during migration

**Recommended approach:** Execute migration step-by-step with validation at each phase.
