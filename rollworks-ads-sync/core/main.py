from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone
# Load self-defined packages
from utils import (file_exists, load_df_to_snowflake, insert_into_snowflake_log, get_campaign_json,
                   flatten_campaign_dict_to_df)

api_key = os.environ.get('NEXTROLL_API_KEY')
api_secret = os.environ.get('NEXTROLL_API_SECRET')

# Calculate the datetime for 2 days ago since these API data won't be finalized until 48 hrs later
current_time_utc = datetime.now(timezone.utc)
print(f"Current Timestamp in UTC: {current_time_utc} \n")
start_date = (current_time_utc - timedelta(days=2)).date()
print(f"API start_date (48hrs after current timestamp): {start_date} \n")
# GraphQL query has to take in different start and end dates vs the platform can take the same dates
# The good news is a different start and end date here only grabs the start_date's data, so it's technically just
# a snapshot date of these data points, which is what we want here
end_date = start_date + timedelta(days=1)
print(f"API end_date (time delta should be 1 for daily metric pull): {end_date}\n")

# Playbooks are campaign templates within the RollWorks Platform; used more than ad groups here
# https://apidocs.nextroll.com/graphql-reporting-api/schema.html#Campaign
# Using GraphQL here as the recommended API by NextRoll
# https://apidocs.nextroll.com/graphql-reporting-api/reporting-api-migration.html

# GraphQL query to get all the advertisable in RollWorks
# advertisable = {
#     "query": """
#         {
#           organization {
#             current {
#               advertisables {
#                 eid
#                 name
#               }
#             }
#           }
#         }
#         """
# }

data_dict = get_campaign_json(start_date, end_date, api_key, api_secret)
final_df = flatten_campaign_dict_to_df(data_dict)
# Manually adding a snapshot date here to the table based on the API pull
final_df['snapshot_date'] = start_date

if file_exists(start_date):
    print(f"Snapshot '{start_date}' has already been loaded into UTIL_DB.LOGS.SYNC_ROLLWORKS.")
else:
    # Load clean dataframe into snowflake staging table
    load_df_to_snowflake(final_df, 'AD_SPEND')
    insert_into_snowflake_log(start_date)