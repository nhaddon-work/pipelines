#!/bin/bash

## List of dates to process
dates=("20251031" "20251101" "20251102" "20251103" "20251104" "20251105" "20251106" "20251107" "20251108" "20251109" "20251110")

# Get S3 file list (requires AWS CLI)
aws s3 ls s3://ebs-prod-sidetrade-sftp-data-informatica/ --recursive | awk '{print $4}' | while read file; do
    for date in "${dates[@]}"; do
        if [[ "$file" == *Actions* && "$file" == *$date* ]]; then
#  if [[ "$file" == *Actions* ]]; then
          echo "Processing $file"
          echo "Running main.py next"
          python3 core/main.py "$file"
          cd core/dbt_project
          echo "Running dbt base models next"
          dbt run --select models/staging/Actions.sql
          dbt run --select models/staging/Actions_Current_State.sql
          echo "Running dbt snapshot next"
          dbt snapshot --select actions_snapshot
          dbt run-operation replica_view --args '{"schema":"sidetrade", "x":"Actions"}'
          dbt run-operation replica_view --args '{"schema":"sidetrade", "x":"Actions_Current_State"}'
          dbt run-operation replica_view --args '{"schema":"sidetrade", "x":"actions_snapshot"}'
          cd ../..
        fi
    done
done