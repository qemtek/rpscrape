# Daily Rescrape Workflow Documentation

## Overview
This workflow runs daily to refresh the last 7 days of horse racing data, ensuring any initially incomplete data is updated using partition-based overwrites.

## Architecture

### Key Components
1. **Partitioned Glue Table**: Data partitioned by (country, date) enables surgical overwrites
2. **Retry Logic**: 3 attempts with exponential backoff (2s → 4s → 8s) for HTTP 406 blocks
3. **Failure Tracking**: All failed races logged to `*_failures.log` files
4. **Monitoring Tools**: Statistical analysis to detect missing data

## Daily Rescrape Workflow

### 1. Automated Daily Run (Recommended)

**Schedule**: Run daily after midnight to capture previous day + refresh last 7 days

```bash
# Via Fargate (recommended for production)
AWS_PROFILE=personal aws ecs run-task \
  --cluster horse-racing-trader \
  --task-definition rpscrape-regenerate-data \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[YOUR_SUBNET],securityGroups=[YOUR_SG],assignPublicIp=ENABLED}" \
  --overrides '{
    "containerOverrides": [{
      "name": "rpscrape-regenerate-data",
      "environment": [
        {"name": "START_DATE", "value": "2025-11-11"},
        {"name": "END_DATE", "value": "2025-11-17"},
        {"name": "COUNTRIES", "value": "gb,ire,fr"},
        {"name": "FORCE", "value": "true"}
      ]
    }]
  }'
```

**What happens:**
1. Scrapes last 7 days for all countries (gb, ire, fr)
2. Creates local CSV files: `data/dates/{country}/YYYY_MM_DD.csv`
3. Retry logic attempts each race 3 times if it fails
4. Failed races logged to `data/dates/{country}/YYYY_MM_DD_failures.log`
5. Uploads to S3: `s3://rpscrape/data/dates/{country}/YYYY_MM_DD.csv`
6. Uploads to Glue with `mode=overwrite_partitions`
   - Only replaces specific (country, date) partitions
   - Existing data for other dates untouched
   - No duplicates

### 2. Manual Upload (if Fargate fails)

If the scrape succeeded but upload failed:

```bash
PYTHONPATH=/Users/christophercollins/Documents/GitHub/rpscrape/RPScraper \
  MODE=overwrite_partitions \
  AWS_PROFILE=personal \
  python3 scripts/simple_upload_to_s3.py
```

This processes all CSV files in `data/dates/` and uploads to Glue.

## Monitoring & Failure Recovery

### Daily Health Check

```bash
# Check last 7 days for anomalies
PYTHONPATH=/Users/christophercollins/Documents/GitHub/rpscrape/RPScraper \
  AWS_PROFILE=personal \
  python3 scripts/check_data_completeness.py \
    --start-date 2025-11-11 \
    --end-date 2025-11-17 \
    --country gb
```

**Output example:**
```
Race Count Statistics:
  Mean: 22.5
  Median: 20
  Std Dev: 11.3

SUSPICIOUS DATES (< 5 races):
  2025-11-17: 1 races, 7 runners
```

### Check for Failure Logs

```bash
# Find any failure logs from last scrape
find data/dates -name "*_failures.log" -mtime -1
```

### Retry Failed Races

```bash
# Retry all recent failures
PYTHONPATH=/Users/christophercollins/Documents/GitHub/rpscrape/RPScraper \
  python3 scripts/retry_failed_races.py --all

# Or retry specific date
python3 scripts/retry_failed_races.py \
  --log data/dates/gb/2025_11_17_failures.log
```

**What happens:**
1. Reads failure log to extract URLs
2. Attempts to rescrape with same retry logic
3. Creates `YYYY_MM_DD_retry.csv` with recovered data
4. If successful, archives failure log to `*_failures_resolved.log`

### Upload Retry Data

After successful retry:

```bash
# Upload retry CSV files
PYTHONPATH=/Users/christophercollins/Documents/GitHub/rpscrape/RPScraper \
  MODE=overwrite_partitions \
  AWS_PROFILE=personal \
  python3 scripts/simple_upload_to_s3.py
```

## Monthly Data Quality Audit

Run comprehensive check once per month:

```bash
# Check entire month
PYTHONPATH=/Users/christophercollins/Documents/GitHub/rpscrape/RPScraper \
  AWS_PROFILE=personal \
  python3 scripts/check_data_completeness.py \
    --start-date 2025-11-01 \
    --end-date 2025-11-30 \
    --country gb \
    --check-local
```

## Troubleshooting

### Issue: High Failure Rate (HTTP 406)

**Symptom**: Many `*_failures.log` files with "HTTP 406" errors

**Cause**: Racing Post rate limiting/bot detection

**Solution**:
1. Wait 30-60 minutes before retrying
2. Retry during off-peak hours (late night/early morning)
3. Retry in smaller batches (1-2 days at a time)

### Issue: Partition Not Updating

**Symptom**: Old data still showing in Glue queries

**Check**:
```sql
SELECT date, country, COUNT(*) as row_count,
       COUNT(DISTINCT created_at) as update_count,
       MAX(created_at) as last_update
FROM rpscrape
WHERE date >= DATE('2025-11-15')
GROUP BY date, country
ORDER BY date DESC
```

**Expected**: `update_count = 1` (all rows have same created_at)

**If `update_count > 1`**: Partition overwrite didn't work, you have duplicates

**Fix**:
```bash
# Re-upload with correct mode
MODE=overwrite_partitions python3 scripts/simple_upload_to_s3.py
```

### Issue: Missing Entire Dates

**Symptom**: Dates with no data in Glue

**Investigation**:
```bash
# Check if CSV exists locally
ls -lh data/dates/gb/2025_11_17.csv

# Check if file was uploaded to S3
AWS_PROFILE=personal aws s3 ls s3://rpscrape/data/dates/gb/2025_11_17.csv

# Check if partition exists in Glue
AWS_PROFILE=personal aws athena start-query-execution \
  --query-string "SHOW PARTITIONS rpscrape" \
  --result-configuration "OutputLocation=s3://aws-athena-query-results-249959970268-eu-west-1/"
```

## Best Practices

1. **Always use `FORCE=true`** when re-scraping to overwrite existing files
2. **Check failure logs daily** - don't let them accumulate
3. **Run completeness check weekly** to catch systematic issues
4. **Retry failures within 24-48 hours** while data is still available
5. **Monitor `created_at` timestamps** to verify partition overwrites worked
6. **Keep local CSV files** for at least 7 days as backup

## File Locations

- **Local CSV**: `data/dates/{country}/YYYY_MM_DD.csv`
- **Failure logs**: `data/dates/{country}/YYYY_MM_DD_failures.log`
- **Retry CSV**: `data/dates/{country}/YYYY_MM_DD_retry.csv`
- **S3 CSV**: `s3://rpscrape/data/dates/{country}/YYYY_MM_DD.csv`
- **Glue Parquet**: `s3://rpscrape/datasets/rpscrape/country={country}/date=YYYY-MM-DD/`

## Summary

The workflow is now **resilient to HTTP 406 failures** with:
- ✓ Automatic retry (3 attempts with backoff)
- ✓ Explicit failure logging
- ✓ Manual retry capability
- ✓ Statistical monitoring
- ✓ Partition-based overwrites (no duplicates)

**Daily routine**: Run scrape → Check logs → Retry failures → Verify completeness
