import awswrangler as wr
# Load self-defined packages
from utils import file_exists, load_df_to_snowflake, insert_into_snowflake_log, deduplicate_df
import utils
import sys

s3_bucket = 'ebs-prod-sidetrade-sftp-data-informatica'
# S3 response
response = utils.aws_client.list_objects(Bucket=s3_bucket)

print(sys.argv[1])

# function that takes in csv file given a s3 link and returns a clean dataframe without report headers in csv
def get_clean_df(s3_url, file_name):
    if 'Transaction' in file_name:
        df = wr.s3.read_csv(
            path=s3_url,
            sep=";",
            header=0,
            dtype=str,  # equivalent to dtype=object
            engine="python",  # use Python engine (handles messy quoting)
            quoting=3,  # csv.QUOTE_NONE
            quotechar=None,  # treat " as plain text
            encoding="utf-8-sig"  # handle UTF-8 BOMs safely
        )
    else:
        df = wr.s3.read_csv(path=s3_url, sep=';', header=0, dtype=object)
    return df


def main():
    # Retrieve the list of files modified in the date range defined in utils
    file_objs = [obj for obj in response.get('Contents', []) if obj['LastModified'] > utils.min_date]
    # Get all the files into clean dataframes and insert them into the staging table
    for file_obj in file_objs:
        file_name = file_obj['Key']
        file_last_modified = file_obj['LastModified']
        if file_exists(file_name):
            print(f"File '{file_name}' has already been loaded into UTIL_DB.LOGS.S3_SIDETRADE_FILE_LOG.")
            continue
        try:
            # using sys.argv[1] in file_name to determine the dates we want to process locally & in Airflow
            if ('Transaction' in file_name and sys.argv[1] in file_name):
                table_name = 'Transaction'
                unique_composite_key = ['Extraction Date', 'Item Technical Number']
            elif ('Actions' in file_name and sys.argv[1] in file_name):
                table_name = 'Actions'
                unique_composite_key = ['Extraction Date', 'Unique Action Code']
            elif ('KPI' in file_name and sys.argv[1] in file_name):
                table_name = 'KPI'
                unique_composite_key = ['Extraction Date', 'Technical Company Number']
            else:
                # Ignores any other files such as test files
                continue
            # Load clean dataframe into snowflake staging table
            clean_df = get_clean_df('s3://' + s3_bucket + '/' + file_name, file_name)
            # Picking the first record when upstream has duplicates and we can't determine which one is correct
            # Only as a temporary solution until SideTrade fixes the duplicates issue upstream; func will skip if no duplicates found
            deduped_df = deduplicate_df(clean_df, subset_cols=unique_composite_key)
            load_df_to_snowflake(deduped_df, table_name)
            insert_into_snowflake_log(s3_bucket, file_last_modified, file_name)
        except Exception as e:
            print(f"Error processing file {file_name}: {e}")
            continue  # skip to next file

if __name__ == "__main__":
    main()
