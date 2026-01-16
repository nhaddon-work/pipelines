from dotenv import load_dotenv
import os
from datetime import date, timedelta
# Load self-defined packages
from utils import (file_exists, load_df_to_snowflake, insert_into_snowflake_log, get_campaign_json,
                   flatten_campaign_dict_to_df)

load_dotenv()
api_key = os.getenv('NEXTROLL_API_KEY')
api_secret = os.getenv('NEXTROLL_API_SECRET')

# One time backfilling script; only run this manually if you need to backfill for some days all at once

# Define the start and end date range for this backfill run
start_date = date(2025, 3, 6)  # Change to your desired start date
end_date = date(2025, 3, 8)   # Change to your desired end date

# Loop through the range of dates, inclusive for both start and end, insert their snapshot data all at once
for i in range((end_date - start_date).days + 1):
    curr = start_date + timedelta(days=i)
    curr_end = curr + timedelta(days=1)  # This is so the API can have a delta of 1 even though it's a snapshot
    data_dict = get_campaign_json(curr, curr_end, api_key, api_secret)
    final_df = flatten_campaign_dict_to_df(data_dict)
    # Manually adding a snapshot date here to the table based on the API pull
    final_df['snapshot_date'] = curr

    if file_exists(curr):
        print(f"Snapshot '{curr}' has already been loaded into UTIL_DB.LOGS.SYNC_ROLLWORKS.")
    else:
        # Load clean dataframe into snowflake staging table
        load_df_to_snowflake(final_df, 'AD_SPEND')
        insert_into_snowflake_log(curr)