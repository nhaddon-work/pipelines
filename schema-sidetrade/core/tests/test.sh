#!/bin/bash

TABLE="Actions"
BUCKET="s3://ebs-prod-sidetrade-sftp-data-informatica/"

echo "Listing ${TABLE} files modified in the past 2 days..."

# Get current time in epoch seconds
now_epoch=$(date +%s)
# Two days = 172800 seconds
two_days_ago=$((now_epoch - 172800))

aws s3 ls "${BUCKET}" --recursive | while read -r date time size file; do
  # Convert "YYYY-MM-DD HH:MM:%S" to epoch (BSD and GNU both support -j -f fallback)
  file_epoch=$(date -j -f "%Y-%m-%d %H:%M:%S" "$date $time" +%s 2>/dev/null || date -d "$date $time" +%s)

  # Compare timestamps and match table name
  if [[ $file_epoch -ge $two_days_ago && "$file" == *${TABLE}* ]]; then
      echo "Processing $file (modified recently)"
  fi
done
