with savings as (

  SELECT * FROM {{ref('stg_savings')}}
)
,
exchange_rate as (

    select * from {{ source('bigquery', 'exchange_rate_dim') }}

)

select s.date,
s.date_to,
person,
bank,
product,
amount,
CASE WHEN currency = 'CAD' THEN amount ELSE (1/cad_gbp_rate) * amount END AS amount_cad,
from savings s
LEFT JOIN
exchange_rate er on DATE_TRUNC(s.date, MONTH) = er.date
