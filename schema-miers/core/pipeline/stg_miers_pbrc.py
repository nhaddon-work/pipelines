import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import pytz
import pandas as pd
from ftplib import FTP
from io import BytesIO
import calendar

# Load self-defined packages
from utils import file_exists, load_df_to_snowflake, insert_into_snowflake_log
import utils

# Getting local environment variables
load_dotenv()
username = os.getenv('PBRC_USER')
password = os.getenv('PBRC_PASSWORD')
host = os.getenv('PBRC_HOST')

# Create a BytesIO object
buffer = BytesIO()
# Get a list of all month names
all_months = calendar.month_name[1:]


def get_clean_df(file_name, ftp):
    ftp.cwd("/EquityResearchData")
    ftp.retrbinary('RETR ' + file_name, buffer.write)
    buffer.seek(0)
    df = pd.read_csv(buffer, on_bad_lines='skip')
    return df


def bad_file(file_name):
    if file_name in ('.', '..', 'HistoricalEquityResearch_stats.csv'):
        return True
    elif file_name.endswith('.dat'):
        return True
    else:
        for month in all_months:
            if month.lower() in file_name.lower():
                return True
        return False


def main():
    # Connect to FTP server
    ftp = FTP(host)
    ftp.login(user=username, passwd=password)
    ftp.cwd("/EquityResearchData")

    # Getting the list of files to be extracted
    files = ftp.nlst()
    for file in files:
        modification_time_str = ftp.sendcmd('MDTM ' + file)
        # Needs to confirm the timezone here later during code review
        modification_time = datetime.strptime(modification_time_str[4:], '%Y%m%d%H%M%S')
        # Set the timezone here to UTC for the comparison with current time
        # Skip file . and file double dots since they are not real files
        if modification_time.replace(tzinfo=pytz.utc) >= utils.min_date and not bad_file(file):
            if file_exists(file):
                logging.info(f"File '{file}' has already been loaded into UTIL_DB.LOGS.S3_MIERS_FILE_LOG.")
            else:
                # Load clean dataframe into snowflake staging table
                clean_df = get_clean_df(file, ftp)
                if clean_df is not None:
                    load_df_to_snowflake(clean_df, 'PBRC')
                    insert_into_snowflake_log('/EquityResearchData', modification_time, file)

    # Close connection when done
    ftp.close()


if __name__ == "__main__":
    main()

