with savings as (
    select * from {{ ref('fct_savings') }}
)

select
    month_date,
    month_date_to,
    person,
    max(amount) as total_savings_amount
from
    (select
        date_trunc(date, month) as month_date,
        date_trunc(date_to, month) as month_date_to,
        person,
        sum(amount_cad) as amount
        from savings
        group by
            1, 2, 3
    )
group by
    1, 2, 3
