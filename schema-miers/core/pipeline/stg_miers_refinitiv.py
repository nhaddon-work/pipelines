import awswrangler as wr
import logging

# Load self-defined packages
from utils import file_exists, load_df_to_snowflake, insert_into_snowflake_log
import utils

bucket_prefix = 'sftp-miers-3PAgg-Refinitiv/data'
response = utils.aws_client.list_objects(Bucket='sftp.pitchbookbi.com',
                               Prefix=bucket_prefix)


# function that takes in a xlsx file given a s3 link and returns a clean dataframe without report headers in xlsx
def get_clean_df(s3_url):
    df = wr.s3.read_excel(path=s3_url)
    # Get the row number where the actual value of the files come in after the file headers
    if df[df['Unnamed: 0'] == 'Refinitiv Doc ID'].empty:
        if df[df['Unnamed: 0'] == 'LSEG Doc ID'].empty:
            return None
        else:
            first_row_number = df[df['Unnamed: 0'] == 'LSEG Doc ID'].index[0]
    else:
        first_row_number = df[df['Unnamed: 0'] == 'Refinitiv Doc ID'].index[0]
    half_clean_df = df.loc[first_row_number:]
    # Get all the column values in the file
    columns = list(half_clean_df.loc[first_row_number])
    clean_columns = [x for x in columns if isinstance(x, str)]
    clean_df = half_clean_df[1:]
    # Drop all the empty columns brought in by the dataframe conversion
    # The assumption here is file format never changes upstream, otherwise, this might break
    clean_df.drop(columns=['Unnamed: 1', 'Unnamed: 4', 'Unnamed: 5',
                           'Unnamed: 7', 'Unnamed: 11'], inplace=True)
    clean_df.columns = clean_columns
    clean_df.reset_index(drop=True, inplace=True)
    return clean_df


def main():
    # Factory Design:
    # Get a List of Files that Are in Range -> Check if Data Exists -> Extract Data -> Clean Data -> Load Data
    file_objs = [obj for obj in response.get('Contents', []) if obj['LastModified'] > utils.min_date]
    s3_bucket = 's3://sftp.pitchbookbi.com/'

    for file_obj in file_objs:
        file_name = file_obj['Key']
        file_last_modified = file_obj['LastModified']

        if file_exists(file_name):
            logging.info(f"File '{file_name}' has already been loaded into UTIL_DB.LOGS.S3_MIERS_FILE_LOG.")
        else:
            # Load clean dataframe into snowflake staging table
            clean_df = get_clean_df(s3_bucket + file_name)
            load_df_to_snowflake(clean_df, 'REFINITIV')
            insert_into_snowflake_log(s3_bucket + bucket_prefix, file_last_modified, file_name)


if __name__ == "__main__":
    main()

