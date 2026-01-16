import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import pytz
import pandas as pd
import zipfile
from io import BytesIO

# Load self-defined packages
from utils import file_exists, load_df_to_snowflake, insert_into_snowflake_log, get_ssh_client
import utils

# Getting local environment variables
load_dotenv()
username = os.getenv('CAPIQ_USER')
password = os.getenv('CAPIQ_PASSWORD')
host = os.getenv('CAPIQ_HOST')
port = int(os.getenv('CAPIQ_PORT'))

# Create a BytesIO object
buffer = BytesIO()

# Clean up df's title column
def get_full_title(row):
    # If the 11th's column doesn't have a value, merge the values to get the full title
    if isinstance(row.iloc[10], str):
        return row.iloc[8].strip() + ":" + row.iloc[9]
    else:
        return row.iloc[8]


def move_category_forward(row):
    if isinstance(row.iloc[10], str):
        return row.iloc[10]
    else:
        return row.iloc[9]


# Extract file in stream and convert into data frame
# CAPIQ files give 10 or 11 columns depending on the title, if that contains |, the title will be split into 2 halves
# The last column can't use | to divide the columns out, they are all part of the title
# Tile shouldn't just have Morningstar in them
def get_clean_df(file_name, sftp):
    sftp.getfo('/Inbox/' + file_name, buffer)

    buffer.seek(0)
    with zipfile.ZipFile(buffer, 'r') as zip_ref:
        # List the files inside the zip
        zip_file_names = zip_ref.namelist()
        zip_file_name = zip_file_names[0]
        with zip_ref.open(zip_file_name) as file_stream:
            # File doesn't have headers, dataframe's columns are numbers in this case
            df = pd.read_csv(file_stream, delimiter='|',  on_bad_lines='skip', header=None, encoding='unicode_escape')
            # This means an extra column was generated because the title contains |
            if df.shape[1] > 10:
                # Make sure the title column has the correct values
                df.iloc[:, 8] = df.apply(get_full_title, axis=1)
                # Make sure the category column has the correct values
                df.iloc[:, 9] = df.apply(move_category_forward, axis=1)
                # Drop the 11th column since it's an extra if it's created because of the | delimiter
                df = df.drop(df.columns[10], axis=1)

            df.columns = ['RAW_DOC_ID', 'ID1', 'ID2', 'DIVISIONNAME', 'NAME', 'USEREMAIL', 'ID3',
                          'VIEWED_DATETIME', 'TITLE', 'CATEGORY']
    return df


def main():
    client = get_ssh_client(host, port, username, password)
    sftp = client.open_sftp()
    files = sftp.listdir_attr('/Inbox')

    for file_attr in files:
        file_name = file_attr.filename
        # Convert int into datetime.datetime in UTC for the comparison
        # Confirm the timezone here in code review later, use UTC for now
        file_mtime = datetime.utcfromtimestamp(file_attr.st_mtime).replace(tzinfo=pytz.utc)
        # Convert int into datetime.datetime in UTC for the comparison
        if file_mtime >= utils.min_date:
            if file_exists(file_name):
                logging.info(f"File '{file_name}' has already been loaded into UTIL_DB.LOGS.S3_MIERS_FILE_LOG.")
            else:
                # Load clean dataframe into snowflake staging table
                clean_df = get_clean_df(file_name, sftp)
                load_df_to_snowflake(clean_df, 'CAPIQ')
                insert_into_snowflake_log('/Inbox/', file_mtime, file_name)

    # Close connection when done
    client.close()
    sftp.close()


if __name__ == "__main__":
    main()

