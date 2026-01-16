import pyarrow as pa
from datetime import datetime, timedelta, timezone
import logging
import boto3
import bi_snowflake_connector
import uuid
import os
import pandas as pd

# Define global variables

# aws connection
aws_client = boto3.client('s3')

# snowflake connection
snow_con = bi_snowflake_connector.connect()
cursor = snow_con.cursor()

# Airflow env variables
os_env = os.environ.get('OS_ENV')
database = os.environ.get('SNOWFLAKE_DATABASE')
schema = os.environ.get('SNOWFLAKE_SCHEMA')

# Calculate the datetime for 12 hrs ago to get the most recent files
current_time_utc = datetime.now(timezone.utc)
min_date = current_time_utc - timedelta(days=20)
batch_id = str(uuid.uuid4())


# function that takes in a dataframe and make the dataframe's columns all lowercase and replace space with underscore
def get_sql_column_names(df_columns):
    # Remove special characters that can't be in sql name
    sql_columns = [col.lower().replace(" ", "_").replace('#_', 'number_of_')
                   .replace("(", "").replace(")", "").replace("/", "").replace('-', '_').replace('.', '_')
                   for col in df_columns]
    return sql_columns


# function that takes in a dataframe and adds a column for last updated date_time in UTC
def add_update_time_column(df):
    df['last_updated_datetime'] = datetime.now(timezone.utc)
    return df


def deduplicate_df(df: pd.DataFrame, subset_cols: list[str], log_path: str = None) -> pd.DataFrame:
    """
    Removes duplicate rows from a DataFrame and logs duplicates if found.

    Args:
        df: Input pandas DataFrame.
        subset_cols: List of column names to check for duplicates.
        log_path: Optional path to save duplicate records (e.g., 's3://bucket/duplicates.csv').

    Returns:
        Cleaned DataFrame without duplicates.
    """
    initial_count = len(df)
    duplicates = df[df.duplicated(subset=subset_cols, keep=False)]

    if not duplicates.empty:
        logging.info(f"Initial_count : {initial_count}")

        logging.warning(f"Found {len(duplicates)} duplicate rows based on {subset_cols}")

        logging.warning("Sample duplicate rows:")
        # You can change head(5) to head(10) or remove it to print all
        logging.warning("\n" + str(duplicates))

    # Drop duplicates, keeping the first occurrence
    clean_df = df.drop_duplicates(subset=subset_cols, keep='first')
    logging.info(f"Removed {initial_count - len(clean_df)} duplicates. Final row count: {len(clean_df)}")

    return clean_df


# Check if the same file has been loaded in the logging table already; if so, skip the file
def file_exists(file_name):
    cursor.execute(
        "SELECT COUNT(*) FROM UTIL_DB.LOGS.S3_SIDETRADE_FILE_LOG WHERE FILE_NAME = %s AND ENV = %s",
        (file_name, os_env),
    )
    result = cursor.fetchone()
    return result[0] > 0


# Insert a log record given folder, file last modified time and file name
def insert_into_snowflake_log(folder_name, file_last_modified, file_name):
    cursor.execute(
        """INSERT INTO UTIL_DB.LOGS.S3_SIDETRADE_FILE_LOG (FILE_LAST_MODIFIED_TIMESTAMP, CURRENT_TIME_UTC, ENV, BATCH_ID,
                                                   FOLDER_NAME, FILE_NAME) 
                    VALUES (%s, %s, %s, %s, %s, %s)""",
        # Change the ENV to local if running locally
        (file_last_modified, current_time_utc, os_env, batch_id, folder_name, file_name),
    )
    logging.info(f"File {file_name} logged in Snowflake at UTIL_DB.LOGS.S3_SIDETRADE_FILE_LOG.")


# Prep the given dataframe in the right format, convert it to parquet table and upload to snowflake
def load_df_to_snowflake(clean_df, table_name):
    clean_df.columns = get_sql_column_names(clean_df.columns)
    clean_df = add_update_time_column(clean_df)
    # Convert dataframe data types to string as Parquet has its own data type assumptions and it might cause problems
    clean_df = clean_df.astype("string")
    # Convert DataFrame into Parquet table to get inserted into Snowflake
    parquet_table = pa.Table.from_pandas(clean_df, preserve_index=False)
    target_table = database + '.' + schema + '.' + table_name
    # Append the file into staging table as is, deduplicate the transactions later in DBT
    # Change if_exists to "replace" if the table is new and hasn't been created in Snowflake, run it once

    cursor.upload_parquet(parquet_table, target_table, if_exists="append",
                          include_meta_fields=False)
