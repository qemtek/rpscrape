# Rescrape Workflow - Last 7 Days Regeneration

## Overview

This document explains the new workflow for safely regenerating the last N days of racing data to handle cases where certain fields weren't updated correctly during initial scraping.

## Problem Statement

**Issue**: Sometimes when we scrape data, certain fields are not populated correctly. We need to re-scrape recent data to fix this.

**Previous Behavior**:
- Running scraper + upload with existing workflow would create **duplicates** in the Glue table
- The table is NOT partitioned, so `overwrite_partitions` mode doesn't work
- Deduplication only works within a batch, not against existing 2.5M rows in Glue

**Solution**: DELETE old data from Glue, then re-scrape and upload fresh data

---

## New Workflow Architecture

### Key Principle: **DELETE + RESCRAPE**

```
┌─────────────────────────────────────────────────────────────┐
│  STEP 1: Clean Local Directory                              │
│  └─ Remove ALL CSV files from data/dates/                   │
│     (Prevents accidentally uploading old/stale data)         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 2: Delete from Glue                                   │
│  └─ DELETE FROM rpscrape                                    │
│     WHERE date >= start_date AND date <= end_date           │
│     AND country IN ('gb', 'ire', 'fr')                      │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 3: Scrape Fresh Data                                  │
│  └─ Run rpscrape.py for each date × country                 │
│     Output: Fresh CSV files in data/dates/{country}/        │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 4: Verify Local Files                                 │
│  └─ Ensure ONLY expected files exist                        │
│     (No unexpected old files that could cause duplicates)    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 5: Upload to S3 and Glue                              │
│  └─ simple_upload_to_s3.py processes local CSV files        │
│     - Uploads to S3: s3://rpscrape/data/dates/{country}/    │
│     - Processes to Glue with 'append' mode (safe now!)      │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 6: Cleanup                                            │
│  └─ Remove local CSV files                                  │
│     (Leaves clean state for next run)                       │
└─────────────────────────────────────────────────────────────┘
```

---

## Files Changed/Created

### New File: `RPScraper/scripts/rescrape_last_7_days.py`
- **Purpose**: Core script that orchestrates the DELETE + rescrape workflow
- **Features**:
  - Configurable date range (default: 7 days)
  - Configurable countries
  - Dry-run mode for safe testing
  - Extensive validation and error checking
  - File verification before upload
  - Detailed logging

### Modified: `RPScraper/scripts/run_daily_updates.sh`
- **Before**: Scraped 3 days ago + uploaded (created duplicates!)
- **After**: Calls `rescrape_last_7_days.py` to DELETE + regenerate last 7 days
- **Environment Variables**:
  - `DAYS_TO_REGENERATE` (default: 7)
  - `COUNTRIES` (default: "gb,ire,fr")

### Not Changed:
- `simple_upload_to_s3.py` - Still uses `append` mode, but safe now because we delete first
- `process_s3_to_glue.py` - Not used in daily workflow
- `full_refresh.py` - Not used in daily workflow

---

## Usage

### Local Testing (Dry Run)

**Test what would happen without making changes:**

```bash
cd /Users/christophercollins/Documents/GitHub/rpscrape/RPScraper

# Test regenerating last 7 days
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 7 \
  --countries gb,ire,fr \
  --dry-run

# Test regenerating last 3 days
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 3 \
  --countries gb,ire \
  --dry-run
```

**Output will show:**
- Which local files would be deleted
- How many rows would be deleted from Glue
- Which dates/countries would be scraped
- What would be uploaded

### Local Execution (Real Run)

**Actually regenerate data:**

```bash
cd /Users/christophercollins/Documents/GitHub/rpscrape/RPScraper

# Regenerate last 7 days (with confirmation prompt)
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 7 \
  --countries gb,ire,fr

# Skip confirmation (for scripting)
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 7 \
  --countries gb,ire,fr \
  --yes
```

**You will be prompted to confirm** unless you use `--yes`:
```
⚠️  WARNING: DESTRUCTIVE OPERATION
This will DELETE and re-scrape data for:
  Dates: 2025-11-09 to 2025-11-15 (7 days)
  Countries: ['gb', 'ire', 'fr']

This operation:
  1. Deletes existing data from Glue table
  2. Re-scrapes data from Racing Post
  3. Uploads new data to S3 and Glue

Type 'yes' to continue:
```

### Docker/ECS Execution

The `run_daily_updates.sh` script now calls the new workflow automatically:

```bash
# In Docker container:
./RPScraper/scripts/run_daily_updates.sh

# With custom configuration:
DAYS_TO_REGENERATE=14 COUNTRIES="gb,ire" ./RPScraper/scripts/run_daily_updates.sh
```

**Task Definition** (`.aws/run-rpscrape.json`):
- No changes needed - uses existing Dockerfile
- Automatically runs new workflow
- Can override env vars if needed

---

## Safety Features

### 1. Date Range Validation
- Maximum 30 days per run (prevents accidentally regenerating years of data)
- No future dates allowed
- No dates before 2008 allowed

### 2. Local File Verification
- Before upload, verifies that ONLY expected files exist locally
- If unexpected files found, **aborts** to prevent duplicates
- Example error:
  ```
  ✗ Unexpected file: gb/2025_03_16.csv
    This file should not exist! Local directory should only have files from 2025-11-09 to 2025-11-15
  ```

### 3. Dry Run Mode
- Preview every action before execution
- Shows exactly what would be deleted/scraped/uploaded
- No changes made to Glue or S3

### 4. Extensive Logging
- All operations logged with timestamps
- Log files saved to `RPScraper/logs/rescrape_YYYYMMDD_HHMMSS.log`
- Includes query IDs for Athena operations (for debugging)

### 5. Atomic Operations
- Cleans local directory FIRST (prevents stale files)
- Deletes from Glue BEFORE scraping (no window for duplicates)
- Verifies files BEFORE upload

---

## Testing Plan

### Phase 1: Dry Run Test (SAFE - No Changes)

```bash
# 1. Test with last 3 days (small dataset)
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 3 \
  --countries gb \
  --dry-run

# Expected output:
# - Shows it would delete ~300-500 rows
# - Shows it would scrape 3 dates for GB
# - Shows it would upload 3 CSV files
```

### Phase 2: Small Real Test

```bash
# 2. Actually regenerate last 3 days for GB only
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 3 \
  --countries gb \
  --yes

# Monitor output for:
# ✓ Cleaned N files from local directory
# ✓ Deleted N rows from Glue table
# ✓ Scraping complete: 3 successful, 0 failed
# ✓ Verification passed: Found exactly 3 expected files
# ✓ Upload completed successfully
```

**Validation:**
```sql
-- Check for duplicates in regenerated range
SELECT race_id, horse_id, COUNT(*) as cnt
FROM rpscrape
WHERE date >= DATE('2025-11-13')
  AND date <= DATE('2025-11-15')
  AND country = 'gb'
GROUP BY race_id, horse_id
HAVING COUNT(*) > 1;

-- Should return 0 rows
```

### Phase 3: Full 7-Day Test

```bash
# 3. Regenerate last 7 days for all countries
AWS_PROFILE=personal python3 scripts/rescrape_last_7_days.py \
  --days 7 \
  --countries gb,ire,fr \
  --yes
```

### Phase 4: Docker Test

```bash
# 4. Test in Docker locally
docker build -t rpscrape:test -f Dockerfile .
docker run --rm \
  -e AWS_PROFILE=personal \
  -v ~/.aws:/root/.aws:ro \
  rpscrape:test
```

---

## Monitoring & Debugging

### Check Logs

```bash
# View most recent log
tail -f RPScraper/logs/rescrape_*.log

# Search for errors
grep -i error RPScraper/logs/rescrape_*.log
```

### Check Glue Table

```sql
-- Total rows
SELECT COUNT(*) FROM rpscrape;

-- Rows by date (should increase by ~300-500 per day)
SELECT
    date,
    country,
    COUNT(*) as rows
FROM rpscrape
WHERE date >= DATE('2025-11-01')
GROUP BY date, country
ORDER BY date DESC, country;

-- Check for duplicates globally
SELECT race_id, horse_id, COUNT(*) as cnt
FROM rpscrape
GROUP BY race_id, horse_id
HAVING COUNT(*) > 1
LIMIT 100;
```

### Check S3 CSV Files

```bash
# Count CSV files in S3
AWS_PROFILE=personal python3 -c "
import awswrangler as wr
import boto3
session = boto3.Session(profile_name='personal', region_name='eu-west-1')
files = wr.s3.list_objects('s3://rpscrape/data/dates/', boto3_session=session)
csv_files = [f for f in files if f.endswith('.csv')]
print(f'Total CSV files: {len(csv_files)}')
"
```

---

## Troubleshooting

### Error: "Unexpected file found"

**Cause**: Old CSV files exist in local directory

**Solution**:
```bash
# Manually clean local directory
rm -rf /Users/christophercollins/Documents/GitHub/rpscrape/RPScraper/data/dates/*/

# Then re-run
```

### Error: "File verification failed"

**Cause**: Scraping didn't complete for all dates

**Solution**:
- Check scraping logs for which dates failed
- Re-run with smaller date range
- Check Racing Post website availability

### Error: "Athena query timeout"

**Cause**: Large DELETE query taking too long

**Solution**:
- Reduce `--days` parameter
- Try during off-peak hours
- Check Athena query history in AWS Console

### Duplicates Still Appearing

**Root Cause Analysis**:
1. Check if `simple_upload_to_s3.py` was run independently (outside workflow)
2. Check if old local files were uploaded
3. Check CloudWatch logs for ECS task failures

**Fix**:
```sql
-- Delete duplicates manually
DELETE FROM rpscrape
WHERE created_at IN (
    SELECT MAX(created_at)
    FROM rpscrape
    GROUP BY race_id, horse_id
    HAVING COUNT(*) > 1
);
```

---

## Migration from Old Workflow

### Before (Old Way - Creates Duplicates):

```bash
# DON'T USE THIS ANYMORE
bash run_daily_updates.sh
# This would:
# 1. Scrape 3 days ago
# 2. Upload with append (DUPLICATES!)
```

### After (New Way - Safe):

```bash
# Use this instead
bash run_daily_updates.sh
# This now:
# 1. Cleans local directory
# 2. Deletes last 7 days from Glue
# 3. Re-scrapes last 7 days
# 4. Uploads (safe because we deleted first)
```

**No changes needed to**:
- Docker container (uses same Dockerfile)
- ECS task definition (`.aws/run-rpscrape.json`)
- Scheduling (if using EventBridge/CloudWatch)

---

## FAQ

**Q: Why regenerate 7 days instead of just yesterday?**

A: Because we discovered that sometimes fields aren't updated for several days. Regenerating 7 days ensures we catch all late-arriving data.

**Q: Won't this be slow/expensive?**

A: Re-scraping 7 days × 3 countries = ~21 scrapes per day. Each scrape takes ~30 seconds. Total runtime: ~10-15 minutes. Athena queries are cheap (<$0.01 per run).

**Q: What if Racing Post is down during regeneration?**

A: The script continues with partial data and logs which dates failed. You can re-run just for failed dates later.

**Q: Can I regenerate more than 30 days?**

A: Yes, but you need to modify the validation limit in the script. Use `full_refresh.py` for very large regenerations.

**Q: Does this affect historical data?**

A: No! It only touches the last N days. All data before that remains untouched.

**Q: What about the `upload-data` task?**

A: Not used in daily workflow. Only used for bulk processing of unprocessed S3 files (e.g., after full_refresh.py).

---

## Summary

✅ **Problem Solved**: No more duplicates when re-scraping

✅ **Safe**: Multiple validation layers prevent accidents

✅ **Automated**: Works in Docker/ECS without changes

✅ **Testable**: Dry-run mode for safe testing

✅ **Monitored**: Comprehensive logging and error handling

✅ **Flexible**: Configurable date range and countries
