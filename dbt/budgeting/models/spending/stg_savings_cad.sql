with savings as (

    select * from {{ ref('stg_savings') }}
)
,
exchange_rate as (

    select * from {{ source('bigquery', 'exchange_rate_dim') }}

)

select
    savings.date,
    savings.date_to,
    person,
    bank,
    product,
    amount,
    case
        when currency = 'CAD' then amount else (1 / cad_gbp_rate) * amount
    end as amount_cad
from savings
left join
    exchange_rate on DATE_TRUNC(savings.date, month) = exchange_rate.date
