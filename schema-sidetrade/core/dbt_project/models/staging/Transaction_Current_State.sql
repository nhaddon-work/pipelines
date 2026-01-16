with base as (
  select *,
         row_number() over (
           partition by ITEM_TECHNICAL_NUMBER
           order by EXTRACTION_DATE desc
         ) as rn
  from {{ ref('Transaction') }}
)
select * EXCLUDE rn,
from base
where rn = 1
AND TECHNICAL_PAYMENT_CENTER_CODE LIKE '%-PB'
