#!/bin/bash

## List of dates to process
dates=("20251031" "20251101" "20251102" "20251103" "20251104" "20251105" "20251106" "20251107" "20251108" "20251109" "20251110")

# Get S3 file list (requires AWS CLI)
aws s3 ls s3://ebs-prod-sidetrade-sftp-data-informatica/ --recursive | awk '{print $4}' | while read file; do
    for date in "${dates[@]}"; do
        if [[ "$file" == *Transaction* && "$file" == *$date* ]]; then
#  if [[ "$file" == *Transaction* ]]; then
          echo "Processing $file"
          echo "Running main.py next"
          python3 core/main.py "$file"
          cd core/dbt_project
          echo "Running dbt base models next"
          dbt run --select models/staging/Transaction.sql
          dbt run --select models/staging/Transaction_Current_State.sql
          echo "Running dbt snapshot next"
          dbt snapshot --select transaction_snapshot
          dbt run-operation replica_view --args '{"schema":"sidetrade", "x":"Transaction"}'
          dbt run-operation replica_view --args '{"schema":"sidetrade", "x":"Transaction_Current_State"}'
          dbt run-operation replica_view --args '{"schema":"sidetrade", "x":"transaction_snapshot"}'
          cd ../..
      fi
    done
done