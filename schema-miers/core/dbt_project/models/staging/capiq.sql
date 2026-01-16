SELECT RAW_DOC_ID,
       ID1,
       ID2,
       DIVISIONNAME,
       NAME,
       USEREMAIL,
       ID3,
       TRY_TO_TIMESTAMP(VIEWED_DATETIME) AS VIEWED_DATETIMEM,
       TITLE,
       CATEGORY,
       LAST_UPDATED_DATETIME
FROM {{ env_var('SNOWFLAKE_DATABASE') }}.MIERS.CAPIQ
-- Don't add ; here to close it out ever, it gives error instead
WHERE RAW_DOC_ID NOT LIKE 'RT%'