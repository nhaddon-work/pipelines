import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import pytz
import pandas as pd
from io import BytesIO

# Load self-defined packages
from utils import file_exists, load_df_to_snowflake, insert_into_snowflake_log, get_ssh_client
import utils

# Getting local environment variables
load_dotenv()
username = os.getenv('BLOOMBERG_USER')
password = os.getenv('BLOOMBERG_PASSWORD')
host = os.getenv('BLOOMBERG_HOST')
port = int(os.getenv('BLOOMBERG_PORT'))

# Create a BytesIO object
buffer = BytesIO()

# Extract file in stream and convert into data frame
def get_clean_df(file_name, buffer, sftp):
    sftp.getfo('/reports/' + file_name, buffer)
    buffer.seek(0)
    df = pd.read_csv(buffer, on_bad_lines='skip')
    return df


def main():
    # Update this later to utils if other reports use the same ones
    # Not the best client, disconnects and fails at times, maybe the port is bad?
    client = get_ssh_client(host, port, username, password)
    sftp = client.open_sftp()
    files = sftp.listdir_attr('/reports')

    # Filter files modified in the last 7 days
    for file_attr in files:
        file_name = file_attr.filename
        # Convert int into datetime.datetime in UTC for the comparison
        # Confirm the timezone here in code review later, use UTC for now
        file_mtime = datetime.utcfromtimestamp(file_attr.st_mtime).replace(tzinfo=pytz.utc)
        # Convert int into datetime.datetime in UTC for the comparison
        if file_mtime >= utils.min_date and 'readership' in file_name:
            if file_exists(file_name):
                logging.info(f"File '{file_name}' has already been loaded into UTIL_DB.LOGS.S3_MIERS_FILE_LOG.")
            else:
                # Load clean dataframe into snowflake staging table
                clean_df = get_clean_df(file_name, buffer, sftp)
                load_df_to_snowflake(clean_df, 'BLOOMBERG')
                insert_into_snowflake_log('/reports', file_mtime, file_name)

    # Close connection when done
    client.close()
    sftp.close()


if __name__ == "__main__":
    main()

