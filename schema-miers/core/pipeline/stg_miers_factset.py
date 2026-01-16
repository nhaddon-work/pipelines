import awswrangler as wr
import logging

# Load self-defined packages
from utils import file_exists, load_df_to_snowflake, insert_into_snowflake_log
import utils

# S3 client to connect to AWS
bucket_prefix = 'sftp-miers-3PAgg-FactSet/'

# FactSet response
response = utils.aws_client.list_objects(Bucket='sftp.pitchbookbi.com',
                                         Prefix=bucket_prefix)

# function that takes in a xlsx file given a s3 link and returns a clean dataframe without report headers in xlsx
def get_clean_df(s3_url):
    df = wr.s3.read_excel(path=s3_url)
    # Get the row number where the actual value of the files come in after the file headers
    first_row_number = df[df['Morningstar Equity Research'] == 'Date/time read'].index[0]
    half_clean_df = df.loc[first_row_number:]
    half_clean_df.loc[first_row_number]
    columns = list(half_clean_df.loc[first_row_number])
    clean_columns = [x for x in columns if isinstance(x, str)]
    clean_df = half_clean_df[1:]
    clean_df.columns = clean_columns
    # Reset the firm column names to remove the duplicated field names for location data
    # This will break if the file format/fields change in any way upstream
    clean_df.columns.values[10] = 'firm_address'
    clean_df.columns.values[11] = 'firm_city'
    clean_df.columns.values[12] = 'firm_state'
    clean_df.columns.values[13] = 'firm_country'
    clean_df.reset_index(drop=True, inplace=True)
    return clean_df


def main():
    # Retrieve the list of files modified in the date range defined in utils
    file_objs = [obj for obj in response.get('Contents', []) if obj['LastModified'] > utils.min_date]
    s3_bucket = 's3://sftp.pitchbookbi.com/'

    # Get all the files into clean dataframes and insert them into the staging table
    for file_obj in file_objs:
        file_name = file_obj['Key']
        file_last_modified = file_obj['LastModified']

        if file_exists(file_name):
            logging.info(f"File '{file_name}' has already been loaded into UTIL_DB.LOGS.S3_MIERS_FILE_LOG.")
        else:
            # Load clean dataframe into snowflake staging table
            clean_df = get_clean_df(s3_bucket + file_name)
            load_df_to_snowflake(clean_df, 'FACTSET')
            insert_into_snowflake_log(s3_bucket + bucket_prefix, file_last_modified, file_name)


if __name__ == "__main__":
    main()
