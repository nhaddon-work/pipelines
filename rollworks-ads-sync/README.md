# RollWorks Ads Sync

### Frequently Asked Questions for the API
https://apidocs.nextroll.com/faq.html

Note: "We provide approximate real-time reporting data throughout the day. The numbers for a day are typically available within twelve hours after the day finishes. However, that data is not considered finalized until 48 hours after the start of the current UTC day. All of our dates and times are in UTC."

Since the data won't be finalized until 2 days after, the pipeline will only grab the daily snapshot 2 days later of the current date to onboard only the final data points

### GraphQL Reporting API Overview
https://apidocs.nextroll.com/graphql-reporting-api/overview.html#what-is-graphql

### Project Requirements
We will need tables for the Campaign, Adgroup, and Ad object with name, dates, status, clicks, impressions, clickthrough, and revenue fields 
https://pitchbook.atlassian.net/browse/FNBIA-1571

### Project Challenge
1. Understand the API object schemas and the hierarchy between each objects
2. Tackle the multiple nested layers within the JSON ad objects returned and load them into ready-to-go formats in snowflake