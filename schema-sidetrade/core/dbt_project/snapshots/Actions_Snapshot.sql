{% snapshot actions_snapshot %}

{{
    config(
      target_schema=env_var('SNOWFLAKE_SCHEMA'),
      unique_key='UNIQUE_ACTION_CODE',
      strategy='check',
      check_cols=['ROW_HASH'],
      updated_at='EXTRACTION_DATE'
    )
}}

SELECT
    -- ROW_HASH for faster change data capture
    MD5(
        CONCAT_WS('|',
            -- COALESCE(EXTRACTION_DATE::STRING, ''),
            COALESCE(TECHNICAL_COMPANY_CODE::STRING, ''),
            -- COALESCE(UNIQUE_ACTION_CODE::STRING, ''),
            COALESCE(COLLECTOR_IN_CHARGE::STRING, ''),
            COALESCE(ACTION_CODE::STRING, ''),
            COALESCE(ACTION_LABEL::STRING, ''),
            COALESCE(ORIGIN::STRING, ''),
            COALESCE(CREATION_DATE::STRING, ''),
            COALESCE(EXPECTED_DATE::STRING, ''),
            COALESCE(EFFECTIVE_DATE::STRING, ''),
            COALESCE(VIEW_DATE::STRING, ''),
            COALESCE(ACTION_TYPE::STRING, ''),
            COALESCE(MEDIA_TYPE::STRING, ''),
            COALESCE(DETAILS::STRING, ''),
            COALESCE(AUTO::STRING, ''),
            COALESCE(STATE::STRING, ''),
            COALESCE(ACTION_BY::STRING, ''),
            COALESCE(CANCELLATION_REASON::STRING, ''),
            COALESCE(ACTION_AMOUNT::STRING, '')
            -- COALESCE(LAST_UPDATED_DATETIME::STRING, '')
        )) AS ROW_HASH,
    EXTRACTION_DATE,
    TECHNICAL_COMPANY_CODE,
    UNIQUE_ACTION_CODE,
    COLLECTOR_IN_CHARGE,
    ACTION_CODE,
    ACTION_LABEL,
    ORIGIN,
    CREATION_DATE,
    EXPECTED_DATE,
    EFFECTIVE_DATE,
    VIEW_DATE,
    ACTION_TYPE,
    MEDIA_TYPE,
    DETAILS,
    AUTO,
    STATE,
    ACTION_BY,
    CANCELLATION_REASON,
    ACTION_AMOUNT,
    LAST_UPDATED_DATETIME
FROM {{ ref('Actions_Current_State') }}

{% endsnapshot %}