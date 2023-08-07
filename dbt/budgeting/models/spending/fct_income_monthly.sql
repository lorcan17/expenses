with date as (
    select distinct
        person,
        year_date,
        quarter_date,
        month_date,
        year_num,
        year_month_id,
        month_index,
        month_date_ly
    from {{ ref('dim_date') }}
    cross join (
        select 'Lorcan' as person
        union all
        select 'Grace' as person
    )
),

income as (
    select * from {{ source('bigquery', 't_income_fact') }}
),

income_monthly as (
    select
        date.person,
        date.month_date,
        date.year_num,
        date.year_month_id,
        date.month_index,
        date.month_date_ly,
        SUM(amount) as amount
    from date
    left join
        income on
            DATE_TRUNC(
                income.date_dt, month
            ) = date.month_date and income.person = date.person
    group by 1, 2, 3, 4, 5, 6
),

base as (
    select
        person,
        month_date,
        year_num,
        year_month_id,
        month_index,
        month_date_ly,
        amount as amount,
        SUM(amount) over (
            partition by person, year_num
            order by month_date
        ) as amount_ytd,
        SUM(amount) over (
            partition by person
            order by month_index
            range between 2 preceding and 0 preceding
        ) as amount_3mnth,
        SUM(amount) over (
            partition by person
            order by month_index
            range between 1 preceding and 1 preceding
        ) as amount_prev_mnth
    from income_monthly
),

base_stage as (
    select
        base.person,
        date.month_date,
        amount as amount_ty,
        amount_ytd as amount_ytd_ty,
        amount_3mnth as amount_3mnth_ty,
        amount_prev_mnth as amount_prev_mnth_ty,
        NULL as amount_ly,
        NULL as amount_ytd_ly,
        NULL as amount_3mnth_ly,
        NULL as amount_prev_mnth_ly
    from base
    inner join
        date on base.person = date.person and base.month_date = date.month_date
    union all
    select
        base.person,
        date.month_date,
        NULL as amount_ty,
        NULL as amount_ytd_ty,
        NULL as amount_3mnth_ty,
        NULL as amount_prev_mnth_ty,
        amount as amount_ly,
        amount_ytd as amount_ytd_ly,
        amount_3mnth as amount_3mnth_ly,
        amount_prev_mnth as amount_prev_mnth_ly
    from base
    inner join
        date on
            base.person = date.person and base.month_date = date.month_date_ly
),

base_stage2 as (
    select
        person,
        month_date,
        SUM(amount_ty) as amount_ty,
        SUM(amount_ytd_ty) as amount_ytd_ty,
        SUM(amount_3mnth_ty) / 3 as amount_3mnth_ty,
        SUM(amount_prev_mnth_ty) as amount_prev_mnth_ty,
        SUM(amount_ly) as amount_ly,
        SUM(amount_ytd_ly) as amount_ytd_ly,
        SUM(amount_3mnth_ly) / 3 as amount_3mnth_ly,
        SUM(amount_prev_mnth_ly) as amount_prev_mnth_ly
    from base_stage
    group by 1, 2
)

select
    person,
    month_date,
    --ty
    amount_ty as income_amount_ty,
    amount_ly as income_amount_ly,
    --(amount_ty - amount_ly)/ amount_ly as income_amount_yoy,
    amount_ytd_ty as income_amount_ytd_ty,
    amount_ytd_ly as income_amount_ytd_ly,
    --(amount_ytd_ty - amount_ytd_ly)/ amount_ytd_ly as income_amount_ytd_yoy_pct,
    amount_3mnth_ty as income_amount_3mnth_ty,
    amount_3mnth_ly as income_amount_3mnth_ly,
    --(amount_3mnth_ty - amount_3mnth_ly)/ amount_3mnth_ly as income_amount_3mnth_yoy_pct,
    amount_prev_mnth_ty as income_amount_prev_mnth_ty,
    --month on month
    --(amount_ty - amount_prev_mnth_ty)/ amount_prev_mnth_ty as income_amount_mom_ty_pct,
    amount_prev_mnth_ly as income_amount_prev_mnth_ly
--(amount_ly - amount_prev_mnth_ly)/ amount_prev_mnth_ly as income_amount_mom_ly_pct
from base_stage2
