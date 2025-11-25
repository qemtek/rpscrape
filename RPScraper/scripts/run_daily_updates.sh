#!/bin/bash

cd /app/RPScraper || exit
export PYTHONPATH=/app/RPScraper:/app/RPScraper/scripts

# Configuration
DAYS_TO_REGENERATE=${DAYS_TO_REGENERATE:-7}  # Default to 7 days, can be overridden by env var
COUNTRIES=${COUNTRIES:-"gb,ire,fr"}          # Default countries, can be overridden by env var

echo "=========================================="
echo "DAILY UPDATE: Regenerate Last ${DAYS_TO_REGENERATE} Days"
echo "=========================================="
echo "Countries: ${COUNTRIES}"
echo "Date range: Last ${DAYS_TO_REGENERATE} days"
echo ""

# Run the safe rescrape workflow
# This will:
# 1. Clean local directory
# 2. Delete existing data from Glue for last N days
# 3. Re-scrape those dates
# 4. Upload to S3 and Glue
# 5. Clean up local files
python3 scripts/rescrape_last_7_days.py \
  --days "${DAYS_TO_REGENERATE}" \
  --countries "${COUNTRIES}" \
  --yes

exit_code=$?

if [ $exit_code -eq 0 ]; then
  echo ""
  echo "✓ Daily update completed successfully"
  exit 0
else
  echo ""
  echo "✗ Daily update failed with exit code ${exit_code}"
  exit $exit_code
fi
