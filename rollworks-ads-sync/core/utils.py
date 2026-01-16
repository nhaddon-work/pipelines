import pyarrow as pa
from datetime import datetime, timedelta, timezone
import bi_snowflake_connector
import uuid
import pandas as pd
from pandas import json_normalize
import os
import requests

# Airflow env variables
os_env = os.environ.get('OS_ENV')
database = os.environ.get('SNOWFLAKE_DATABASE')
schema = os.environ.get('SNOWFLAKE_SCHEMA')

# snowflake connection
snow_con = bi_snowflake_connector.connect(method='env')
cursor = snow_con.cursor()

batch_id = str(uuid.uuid4())
current_time_utc = datetime.now(timezone.utc)

# function that takes in a start and end date pair and returns the campaign object from RollWorks API
def get_campaign_json(start_date, end_date, api_key, api_secret):
    reporting_url = f'https://services.adroll.com/reporting/api/v1/query?apikey={api_key}'
    # For whatever reason, even paused campaigns still have running ads here so we keep them all and unfiltered in the call
    graphQL = {
        "query": f"""
            {{
              advertisable {{
                byEID(advertisable: "2HN5SB32U5B7RKLIF5GUQE") {{
                  campaigns {{
                    eid
                    name
                    playbookName
                    createdDate
                    startDate
                    endDate
                    status
                    updatedDate
                    channel
                    currency
                    adgroups {{
                      ads(isActive: true) {{
                        eid
                        name
                        destinationURL
                        createdDate
                        updatedDate
                        status
                        campaignType
                        metrics(start: "{start_date}", end: "{end_date}") {{
                          byDate {{
                            impressions
                            clicks
                            cost
                            conversions
                          }}
                        }}
                      }}
                    }}
                  }}
                }}
              }}
            }}
        """
    }
    response = requests.post(
        url=reporting_url,
        json=graphQL,
        headers={
            'Authorization': f'Token {api_secret}'
        }
    )
    # content_type = response.headers.get('Content-Type')
    if response.status_code == 200:
        print('API Call Succeeded!')
        data_dict = response.json()
        # Uncomment if you want to check the raw format of the returned JSON response
        # with open("output.txt", "w") as f:
        #     json.dump(data_dict, f, indent=4)
        return data_dict
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code} - {response.text}")
        return None

# function that flatten a very nested JSON dict and returns a dataframe
def flatten_campaign_dict_to_df(data_dict):
    # Extract campaigns data based on the schema structure
    campaigns_dict = data_dict['data']['advertisable']['byEID']['campaigns']
    # Flatten adgroups column
    ad_groups_df = pd.json_normalize(
        campaigns_dict, "adgroups", ["eid", "name", "createdDate", "startDate", "endDate", "status", "updatedDate",
                                     "channel", "currency"]
    )

    # Rename campaign related columns if exists
    ad_groups_df = ad_groups_df.rename(columns={'eid': 'campaign_id', 'name': 'campaign_name',
                                                'createdDate': 'campaign_created_date',
                                                'startDate': 'campaign_start_date', 'endDate': 'campaign_end_date',
                                                'status': 'campaign_status', 'updatedDate': 'campaign_updated_date',
                                                'channel': 'campaign_channel', 'currency': 'campaign_currency'})
    # ad_groups_df.to_csv('ad_groups.csv', index=False)
    ad_groups_df_exploded = ad_groups_df.explode("ads", ignore_index=True)

    # Extract the ads column as a df for further flattening
    ads_df = pd.json_normalize(ad_groups_df_exploded['ads'])
    ads_df = ads_df.rename(columns={'eid': 'ad_id', 'name': 'ad_name', 'destinationURL': 'ad_destination_URL',
                                    'createdDate': 'ad_created_date', 'updatedDate': 'ad_updated_date',
                                    'status': 'ad_status', 'campaignType': 'campaign_type'})
    # ads_df.to_csv('ads_df.csv', index=False)

    # Explode the 'metrics.byDate' column to transform each element of a list-like to a row
    df = ads_df.explode('metrics.byDate', ignore_index=True)
    # Normalize the nested dictionaries within 'metrics.byDate' into a flat table in a dataframe
    metrics_df = json_normalize(df['metrics.byDate'])
    # metrics_df.to_csv('metrics_df.csv', index=False)

    # Added back the flattened metric data within ads to the original ads DataFrame as columns
    result_df = pd.concat([df.drop(columns=['metrics.byDate']), metrics_df], axis=1)
    # Added back the ads df to the original campaign/ad_groups level df as columns
    final_df = pd.concat([ad_groups_df_exploded.drop(columns=['ads']), result_df], axis=1)
    # final_df.to_csv('final_df.csv', index=False)
    print('API Data Extraction into DataFrame Succeeded!')
    return final_df


# function that takes in a dataframe and adds a column for last updated date_time in UTC
def add_update_time_column(df):
    df['last_updated_datetime'] = datetime.now(timezone.utc)
    return df


# Check if the same file has been loaded in the logging table already; if so, skip the file
def file_exists(snapshot_date):
    cursor.execute(
        "SELECT COUNT(*) FROM UTIL_DB.LOGS.SYNC_ROLLWORKS WHERE SNAPSHOT_DATE = %s AND ENV = %s",
        (snapshot_date, os_env),
    )
    result = cursor.fetchone()
    return result[0] > 0


# Insert a log record given snapshot_date
def insert_into_snowflake_log(snapshot_date):
    cursor.execute(
        """INSERT INTO UTIL_DB.LOGS.SYNC_ROLLWORKS (CURRENT_TIMESTAMP_UTC, ENV, BATCH_ID, SNAPSHOT_DATE) 
                    VALUES (%s, %s, %s, %s)""",
        # Change the ENV to local if running locally
        (current_time_utc, os_env, batch_id, snapshot_date),
    )
    print(f"Snapshot {snapshot_date} logged in Snowflake at UTIL_DB.LOGS.SYNC_ROLLWORKS.")


# Prep the given dataframe in the right format, convert it to parquet table and upload to snowflake
def load_df_to_snowflake(clean_df, target_table):
    clean_df = add_update_time_column(clean_df)
    # Convert dataframe data types to string as Parquet has its own data type assumptions and it might cause problems
    clean_df = clean_df.astype("string")
    # Convert DataFrame into Parquet table to get inserted into Snowflake
    parquet_table = pa.Table.from_pandas(clean_df)
    target_table = database + '.' + schema + '.' + target_table
    # Append the file into staging table as is, deduplicate the transactions later in DBT
    # Change this to replace if the table is new and hasn't been created, load it once
    cursor.upload_parquet(parquet_table, target_table, if_exists="append",
                          include_meta_fields=False)
    print('Successfully appended the pandas dataframe into snowflake from parquet!')