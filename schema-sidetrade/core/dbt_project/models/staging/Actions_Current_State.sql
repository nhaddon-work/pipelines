with base as (
  select *,
         row_number() over (
           partition by UNIQUE_ACTION_CODE
           order by EXTRACTION_DATE desc
         ) as rn
  from {{ ref('Actions') }}
)
select * EXCLUDE rn,
from base
where rn = 1
