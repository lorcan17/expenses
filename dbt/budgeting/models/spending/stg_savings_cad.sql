with savings as (

    select * from {{ ref('stg_savings') }}
)
,
rates as (

    select * from {{ source('bigquery', 'exchange_rate_dim') }}

)

select
    savings.date_dt as date,
    savings.date_to,
    savings.person,
    savings.source,
    savings.product,
    savings.amount,
    case
        when savings.currency = 'CAD' then savings.amount else (1 / rates.cad_gbp_rate) * savings.amount
    end as amount_cad
from savings
left join
    rates on DATE_TRUNC(savings.date_dt, month) = rates.date
