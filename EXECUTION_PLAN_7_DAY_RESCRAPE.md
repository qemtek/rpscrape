# Execution Plan: 7-Day Rescrape (PRODUCTION RUN)

**Date**: 2025-11-18
**Operator**: Christopher Collins
**Operation**: Real 7-day data regeneration (Nov 11-17, 2025)
**Impact**: WILL MODIFY DATA - Deletes and re-creates 1,278+ rows

---

## Pre-Execution State (Current)

### Glue Table Data (Nov 11-17)
```
      date country  row_count  races
2025-11-11      fr        100     11
2025-11-11      gb        142     19
2025-11-12      gb        123     14
2025-11-12     ire         90      7
2025-11-13      fr         11      1
2025-11-13      gb         92     13
2025-11-13     ire         98      8
2025-11-14      fr        105      8
2025-11-14      gb        172     20
2025-11-14     ire         76      7
2025-11-15      fr        101      9
2025-11-15      gb        168     21
```

**Current Total**: 1,278 rows
**Missing Data**: Nov 16-17 (no data exists yet)
**Total Glue Rows**: 2,567,871

### Local State
- 185 old CSV files in `data/dates/` (March-April 2025)
- These will be deleted before scraping (correct behavior)

---

## What Will Happen

### Phase 1: Cleanup (Local)
- **Delete 185 CSV files** from local directories
  - 61 files from gb/
  - 62 files from ire/
  - 62 files from fr/
- **Time**: < 1 second
- **Risk**: None (these are stale files)

### Phase 2: Delete from Glue (AWS)
- **Query**: `DELETE FROM rpscrape WHERE date >= '2025-11-11' AND date <= '2025-11-17' AND country IN ('gb','ire','fr')`
- **Rows deleted**: 1,278 rows
- **Time**: ~10-30 seconds (Athena query)
- **Risk**: Data loss if script fails before re-scraping
- **Mitigation**: Data will be re-scraped immediately

### Phase 3: Scrape Fresh Data
- **Scrapes**: 21 total (7 days × 3 countries)
  - Nov 11-17 for GB (7 scrapes)
  - Nov 11-17 for IRE (7 scrapes)
  - Nov 11-17 for FR (7 scrapes)
- **Output**: 21 CSV files in local `data/dates/{country}/`
- **Time**: ~10-15 minutes (21 × ~30 seconds each)
- **Risk**: Racing Post website issues, network failures
- **Mitigation**: Script continues on failure, logs which dates failed

### Phase 4: Verify Files
- **Check**: Only expected 21 files exist locally
- **Fail if**: Any unexpected files found
- **Time**: < 1 second
- **Risk**: None (safety check)

### Phase 5: Upload to Glue
- **Process**: Upload 21 CSVs to S3 → Process to Glue
- **Mode**: append (safe because we deleted first)
- **Expected rows**: ~1,200-1,500 (similar to current 1,278)
  - Note: May differ slightly if race results changed
  - Nov 16-17 will be NEW data
- **Time**: ~2-5 minutes
- **Risk**: Duplicate rows if verification failed
- **Mitigation**: Verification step prevents this

### Phase 6: Cleanup
- **Delete**: 21 local CSV files
- **Time**: < 1 second
- **Risk**: None

---

## Expected Outcome

### After Execution
- **Nov 11-15**: Refreshed data (~1,200-1,300 rows, similar to current)
- **Nov 16-17**: NEW data (~200-400 rows, depending on races)
- **Total new row count**: ~1,400-1,700 rows (for this date range)
- **Net change in Glue**: +122 to +422 rows (depending on Nov 16-17 data)
- **No duplicates**: Guaranteed by DELETE before re-scrape

---

## Execution Steps

### STEP 1: Pre-Flight Checks (5 minutes)

Run these commands to document current state:

```bash
cd /Users/christophercollins/Documents/GitHub/rpscrape/RPScraper

# 1. Save current state
AWS_PROFILE=personal python3 -c "
import sys
sys.path.insert(0, '/Users/christophercollins/Documents/GitHub/rpscrape/RPScraper')
import awswrangler as wr
from settings import AWS_GLUE_DB, boto3_session

# Total rows
query_total = 'SELECT COUNT(*) as total FROM rpscrape'
df_total = wr.athena.read_sql_query(query_total, database=AWS_GLUE_DB, boto3_session=boto3_session)
print(f'Total rows before: {df_total[\"total\"].values[0]:,}')

# Check for duplicates (should be 0)
query_dups = '''
SELECT COUNT(*) as dup_count
FROM (
    SELECT race_id, horse_id, COUNT(*) as cnt
    FROM rpscrape
    GROUP BY race_id, horse_id
    HAVING COUNT(*) > 1
)
'''
df_dups = wr.athena.read_sql_query(query_dups, database=AWS_GLUE_DB, boto3_session=boto3_session)
print(f'Duplicate (race_id, horse_id) pairs before: {df_dups[\"dup_count\"].values[0]}')
print('(Should be 0)')

# Data for Nov 11-17
query_range = '''
SELECT COUNT(*) as range_count
FROM rpscrape
WHERE date >= DATE(\"2025-11-11\") AND date <= DATE(\"2025-11-17\")
'''
df_range = wr.athena.read_sql_query(query_range, database=AWS_GLUE_DB, boto3_session=boto3_session)
print(f'Rows in Nov 11-17 range before: {df_range[\"range_count\"].values[0]:,}')
" > /tmp/pre_rescrape_state.txt

cat /tmp/pre_rescrape_state.txt
```

**Expected output:**
```
Total rows before: 2,567,871
Duplicate (race_id, horse_id) pairs before: 0
Rows in Nov 11-17 range before: 1,278
```

**✓ CHECKPOINT**: Confirm duplicates = 0 before proceeding

---

### STEP 2: Execute Rescrape (15-20 minutes)

```bash
cd /Users/christophercollins/Documents/GitHub/rpscrape/RPScraper

# Run the real 7-day rescrape
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 7 \
  --countries gb,ire,fr \
  --yes \
  2>&1 | tee /tmp/rescrape_execution.log
```

**What to monitor:**
- ✓ "Cleaned N files from local directory"
- ✓ "Deleted 1,278 rows from Glue table"
- ✓ "Scraping complete: 21 successful, 0 failed"
- ✓ "Verification passed: Found exactly 21 expected files"
- ✓ "Upload completed successfully"
- ✓ "✓ OPERATION COMPLETE"

**If you see errors:**
- Check the log file in `RPScraper/logs/rescrape_*.log`
- Common issues:
  - Racing Post timeout: Some dates may fail, re-run for those dates
  - Athena timeout: Retry the script
  - Verification failed: Unexpected files exist, investigate before re-running

**Exit code:**
- 0 = Success
- 1 = Failure (check logs)

---

### STEP 3: Post-Execution Validation (5 minutes)

```bash
# 1. Check new state
AWS_PROFILE=personal python3 -c "
import sys
sys.path.insert(0, '/Users/christophercollins/Documents/GitHub/rpscrape/RPScraper')
import awswrangler as wr
from settings import AWS_GLUE_DB, boto3_session

# Total rows
query_total = 'SELECT COUNT(*) as total FROM rpscrape'
df_total = wr.athena.read_sql_query(query_total, database=AWS_GLUE_DB, boto3_session=boto3_session)
print(f'Total rows after: {df_total[\"total\"].values[0]:,}')

# Check for duplicates (MUST be 0!)
query_dups = '''
SELECT race_id, horse_id, COUNT(*) as cnt
FROM rpscrape
GROUP BY race_id, horse_id
HAVING COUNT(*) > 1
LIMIT 10
'''
df_dups = wr.athena.read_sql_query(query_dups, database=AWS_GLUE_DB, boto3_session=boto3_session)
if len(df_dups) == 0:
    print('✓ NO DUPLICATES FOUND!')
else:
    print(f'✗ ERROR: Found {len(df_dups)} duplicate pairs!')
    print(df_dups)

# Data for Nov 11-17 (should be more than before due to Nov 16-17)
query_range = '''
SELECT
    date,
    country,
    COUNT(*) as row_count,
    COUNT(DISTINCT race_id) as races
FROM rpscrape
WHERE date >= DATE(\"2025-11-11\") AND date <= DATE(\"2025-11-17\")
GROUP BY date, country
ORDER BY date, country
'''
df_range = wr.athena.read_sql_query(query_range, database=AWS_GLUE_DB, boto3_session=boto3_session)
print(f'\nNov 11-17 data after rescrape:')
print(df_range.to_string(index=False))
print(f'\nTotal for range: {df_range[\"row_count\"].sum():,}')
" > /tmp/post_rescrape_state.txt

cat /tmp/post_rescrape_state.txt
```

**Expected output:**
```
Total rows after: 2,568,000-2,568,300 (increase of ~129-429 rows)
✓ NO DUPLICATES FOUND!

Nov 11-17 data after rescrape:
      date country  row_count  races
2025-11-11      fr       ~100     11
2025-11-11      gb       ~140     19
2025-11-12      gb       ~120     14
2025-11-12     ire        ~90      7
2025-11-13      fr        ~11      1
2025-11-13      gb        ~90     13
2025-11-13     ire        ~98      8
2025-11-14      fr       ~105      8
2025-11-14      gb       ~170     20
2025-11-14     ire        ~76      7
2025-11-15      fr       ~101      9
2025-11-15      gb       ~168     21
2025-11-16      fr        NEW     NEW
2025-11-16      gb        NEW     NEW
2025-11-16     ire        NEW     NEW
2025-11-17      fr        NEW     NEW
2025-11-17      gb        NEW     NEW
2025-11-17     ire        NEW     NEW

Total for range: ~1,400-1,700
```

**✓ CHECKPOINT**: Verify:
1. NO duplicates found
2. Nov 16-17 data now exists
3. Total rows increased (not decreased)

---

### STEP 4: Check Execution Logs

```bash
# View the most recent log file
tail -100 /Users/christophercollins/Documents/GitHub/rpscrape/RPScraper/logs/rescrape_*.log | tail -50

# Check for errors
grep -i error /Users/christophercollins/Documents/GitHub/rpscrape/RPScraper/logs/rescrape_*.log
```

---

## Success Criteria

- ✅ Exit code = 0
- ✅ "✓ OPERATION COMPLETE" in output
- ✅ No duplicates in validation query
- ✅ Nov 16-17 data now exists
- ✅ Total row count increased (not decreased)
- ✅ No unexpected errors in logs

---

## Rollback Plan (If Needed)

**If something goes wrong:**

The data can be restored by re-running the regenerate-data task for Nov 11-17.

**Option 1: Re-run this script**
```bash
# If some dates failed, just re-run
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 7 \
  --countries gb,ire,fr \
  --yes
```

**Option 2: Use full_refresh.py for specific dates**
```bash
# Set environment and run full_refresh
START_DATE=2025-11-11 END_DATE=2025-11-17 COUNTRIES=gb,ire,fr FORCE=true \
  python3 scripts/full_refresh.py
```

**Option 3: Query old data from S3 CSVs**
The original CSV files are preserved in S3 at `s3://rpscrape/data/dates/` and can be reprocessed.

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Duplicates created | LOW | High | File verification prevents this |
| Some scrapes fail | MEDIUM | Low | Script continues, logs failures |
| Athena timeout | LOW | Low | Retry script |
| Racing Post down | LOW | Medium | Retry later |
| Data loss | VERY LOW | High | Data re-scraped immediately |

---

## Timeline

| Step | Duration | Cumulative |
|------|----------|------------|
| Pre-flight checks | 5 min | 5 min |
| Execute rescrape | 15-20 min | 20-25 min |
| Post validation | 5 min | 25-30 min |
| Review logs | 2 min | 27-32 min |
| **TOTAL** | **~30 minutes** | |

---

## Approval

**Ready to execute?**

- [ ] Pre-flight checks completed
- [ ] Current state documented
- [ ] Rollback plan understood
- [ ] Execution command ready

**Execute when ready:**
```bash
cd /Users/christophercollins/Documents/GitHub/rpscrape/RPScraper
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 7 \
  --countries gb,ire,fr \
  --yes
```
