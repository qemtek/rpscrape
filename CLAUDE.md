# Claude Code Project Settings

## AWS ECS Task Configuration

### Network Configuration
When launching ECS Fargate tasks in this project, use the following network configuration:

- **Subnet ID**: `subnet-7ff27625`
- **Security Group ID**: `sg-9c337de8`
- **Assign Public IP**: `ENABLED`

### Example: Launch Regenerate Data Task

```bash
AWS_PROFILE=personal aws ecs run-task \
  --cluster horse-racing-trader \
  --task-definition rpscrape-regenerate-data \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-7ff27625],securityGroups=[sg-9c337de8],assignPublicIp=ENABLED}" \
  --overrides '{
    "containerOverrides": [{
      "name": "rpscrape-regenerate-data",
      "environment": [
        {"name": "START_DATE", "value": "2025-10-20"},
        {"name": "END_DATE", "value": "2025-10-27"},
        {"name": "COUNTRIES", "value": "gb,ire,fr"},
        {"name": "FORCE", "value": "true"}
      ]
    }]
  }'
```

### Example: Launch Daily Updates Task

```bash
AWS_PROFILE=personal aws ecs run-task \
  --cluster horse-racing-trader \
  --task-definition rpscrape-daily-updates \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-7ff27625],securityGroups=[sg-9c337de8],assignPublicIp=ENABLED}"
```

## Project Structure

### Key Directories
- `RPScraper/` - Main application code
- `RPScraper/scripts/` - Python scripts for scraping and data management
- `RPScraper/data/dates/{country}/` - Local CSV storage organized by country
- `.aws/` - AWS task definitions and deployment configurations (excluded from version control)

### Key Files
- `RPScraper/scripts/rescrape_last_7_days.py` - Daily rescrape workflow (called by daily updates task)
- `RPScraper/scripts/full_refresh.py` - Main scraping script
- `RPScraper/scripts/simple_upload_to_s3.py` - Uploads local CSV files to S3 and Glue
- `RPScraper/scripts/process_s3_to_glue.py` - Batch processes S3 CSV files to Glue
- `RPScraper/scripts/retry_failed_races.py` - Retries failed races from failure logs
- `RPScraper/scripts/check_data_completeness.py` - Statistical data quality analysis
- `DAILY_RESCRAPE_WORKFLOW.md` - Comprehensive workflow documentation

## AWS Resources

### S3 Buckets
- **Bucket**: `s3://rpscrape/`
- **Raw CSV files**: `s3://rpscrape/data/dates/{country}/YYYY_MM_DD.csv`
- **Glue Parquet**: `s3://rpscrape/datasets/rpscrape/country={country}/date=YYYY-MM-DD/`

### Glue Database & Table
- **Database**: `finish-time-predict`
- **Table**: `rpscrape`
- **Partitions**: `country` (string), `date` (date)
- **Row count**: ~2.5M rows
- **Partition count**: ~18,500 partitions

### ECS Cluster & Tasks
- **Cluster**: `horse-racing-trader`
- **Task Definitions**:
  - `rpscrape-regenerate-data` - For historical data regeneration (manual)
  - `rpscrape-daily-updates` - For daily rescrape of last 7 days (automated)
  - `rpscrape-upload-data` - For uploading processed data to Glue

## Important Patterns

### Environment Variables for Scripts

**full_refresh.py** reads configuration from environment variables:
- `START_DATE` - Format: `YYYY-MM-DD`
- `END_DATE` - Format: `YYYY-MM-DD`
- `COUNTRIES` - Comma-separated: `gb,ire,fr`
- `FORCE` - Boolean: `true` or `false`

**simple_upload_to_s3.py** reads:
- `MODE` - Options: `append`, `overwrite`, `overwrite_partitions` (default: `overwrite_partitions`)
- `PYTHONPATH` - Should be set to project directory

### Partition Overwrite Mode

Always use `MODE=overwrite_partitions` when re-uploading data to avoid duplicates. This mode:
- Only replaces specific (country, date) partitions that exist in the new data
- Leaves all other partitions untouched
- Works atomically per partition
- No risk of data loss for other dates

### Failure Handling

The scraper includes retry logic:
- 3 attempts per race with exponential backoff (2s → 4s → 8s)
- Failed races logged to `*_failures.log` files
- Use `retry_failed_races.py` to manually retry failures
- Check data quality with `check_data_completeness.py`

## Common Commands

### Check Task Status
```bash
AWS_PROFILE=personal aws ecs describe-tasks \
  --cluster horse-racing-trader \
  --tasks <TASK_ARN>
```

### View Task Logs
```bash
AWS_PROFILE=personal aws logs describe-log-streams \
  --log-group-name /ecs/<TASK_NAME> \
  --order-by LastEventTime \
  --descending \
  --max-items 1
```

### Query Glue Data
```python
import awswrangler as wr
from settings import boto3_session

query = """
SELECT date, country, COUNT(*) as race_count
FROM rpscrape
WHERE date >= DATE('2025-11-01')
GROUP BY date, country
ORDER BY date DESC
"""

df = wr.athena.read_sql_query(query, database='finish-time-predict', boto3_session=boto3_session)
print(df)
```

## Data Quality Monitoring

### RPR Coverage Check
```python
query = """
SELECT date,
       COUNT(*) as total_races,
       COUNT(rpr) as races_with_rpr,
       ROUND(100.0 * COUNT(rpr) / COUNT(*), 1) as rpr_coverage_pct
FROM rpscrape
WHERE date >= DATE('2025-10-01') AND country = 'gb'
GROUP BY date
ORDER BY date DESC
"""
```

### Expected RPR Coverage
- Recent data (last 7 days): May be 0-60% (RPR published with delay)
- Older data (>7 days): Should be 85-90%
- If < 80% after 2 weeks: Indicates scraping issues

## Troubleshooting

### Issue: HTTP 406 Errors
**Symptom**: Many `*_failures.log` files with "HTTP 406" errors
**Cause**: Racing Post rate limiting/bot detection
**Solution**: Wait 30-60 minutes, retry during off-peak hours

### Issue: Hardcoded Docker Paths
**Location**: `full_refresh.py:16` has `cd /app/RPScraper`
**Impact**: Script won't run locally (only works in Docker container)
**Workaround**: Use Fargate tasks for regeneration instead of running locally

### Issue: Missing STEP 2 Logs
**Symptom**: CloudWatch logs show scraping but not upload step
**Likely Cause**: Logs displayed in reverse chronological order
**Verification**: Check `created_at` timestamps in Glue data to confirm upload occurred
