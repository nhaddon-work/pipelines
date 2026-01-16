SELECT ID,
       TRY_TO_TIMESTAMP(EVENT_DATE) AS EVENT_DATE,
       ACCOUNT_NAME,
       USER_NAME,
       EMAIL,
       DOC_ID,
       TRY_TO_TIMESTAMP(DOCUMENT_POSTED_DATE) AS DOCUMENT_POSTED_DATE,
       HEADLINE,
       ANALYST,
       PB_ID,
       ACCOUNT_TYPE,
       ACCOUNT_STATUS,
       LAST_UPDATED_DATETIME
FROM {{ env_var('SNOWFLAKE_DATABASE') }}.MIERS.PBRC
-- Don't add ; here to close it out ever, it gives error instead
WHERE ACCOUNT_TYPE = 'PM | Buy Side'