import pandas as pd
import awswrangler as wr
from datetime import datetime, timedelta, timezone
import boto3

# df = pd.read_csv(
#     "303610-Transaction-20251013.csv",
#     sep=";",
#     engine="python",
#     quoting=3,        # ignore quotes completely
#     quotechar=None,   # treat " as plain text
#     encoding="utf-8-sig"
# )
# print(df.columns)
#
# df.to_csv("cleaned_transaction.csv", index=False)
#
# deduped_df = deduplicate_df(df, subset_cols=['Extraction Date', 'Item Technical Number'])
#
# deduped_df.to_csv("cleaned_transaction_deduped.csv", index=False)

# Actions

# df = wr.s3.read_csv(path='s3://ebs-prod-sidetrade-sftp-data-informatica/303610-Actions-20250829.csv', sep=';', header=0, dtype=object)
#
# initial_count = len(df)
# duplicates = df[df.duplicated(subset=['Extraction Date', 'Unique Action Code'], keep=False)]
#
# if not duplicates.empty:
#     logging.info(f"Initial_count : {initial_count}")
#
#     logging.warning(f"Found {len(duplicates)} duplicate rows based on 'Extraction Date', 'Unique Action Code'")
#
#     logging.warning("Sample duplicate rows:")
#     # You can change head(5) to head(10) or remove it to print all
#     logging.warning("\n" + str(duplicates))
#
# # Drop duplicates, keeping the first occurrence
# clean_df = df.drop_duplicates(subset=['Extraction Date', 'Unique Action Code'], keep='first')
# logging.info(f"Removed {initial_count - len(clean_df)} duplicates. Final row count: {len(clean_df)}")
#
# print(df)
# df.to_csv("cleaned_actions_20250829.csv", index=False)
# print(df.columns)

# KPI
#
# df = wr.s3.read_csv(path='s3://ebs-prod-sidetrade-sftp-data-informatica/303610-KPI-20251014.csv', sep=';', header=0, dtype=object)
# #
# # print(df)
# print(df.columns)
#
# initial_count = len(df)
# duplicates = df[df.duplicated(subset=['Extraction Date', 'Technical Company Number'], keep=False)]
#
# if not duplicates.empty:
#     logging.info(f"Initial_count : {initial_count}")
#
#     logging.warning(f"Found {len(duplicates)} duplicate rows based on 'Extraction Date', 'Technical Company Number'")
#
#     logging.warning("Sample duplicate rows:")
#     # You can change head(5) to head(10) or remove it to print all
#     logging.warning("\n" + str(duplicates))
#
# # Drop duplicates, keeping the first occurrence
# clean_df = df.drop_duplicates(subset=['Extraction Date', 'Technical Company Number'], keep='first')
# logging.info(f"Removed {initial_count - len(clean_df)} duplicates. Final row count: {len(clean_df)}")
#
# print(df)
# df.to_csv("cleaned_KPI-20251014.csv", index=False)

s3_bucket = 'ebs-prod-sidetrade-sftp-data-informatica'

# aws connection
aws_client = boto3.client('s3')

# S3 response
response = aws_client.list_objects(Bucket=s3_bucket)

# Calculate the datetime for 2 days ago
current_time_utc = datetime.now(timezone.utc)
min_date = current_time_utc - timedelta(days=0.5)
print(min_date)


def main():
    # Retrieve the list of files modified in the date range defined in utils
    file_objs = [obj for obj in response.get('Contents', []) if obj['LastModified'] > min_date]
    # Get all the files into clean dataframes and insert them into the staging table
    for file_obj in file_objs:
        file_name = file_obj['Key']
        file_last_modified = file_obj['LastModified']
        print(file_name)
        print(file_last_modified)

main()