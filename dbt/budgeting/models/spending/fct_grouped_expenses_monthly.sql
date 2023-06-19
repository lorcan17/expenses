with date as (
    select distinct
        person,
        cat_group,
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
    cross join (
        select cat_group
        from
            UNNEST(['dining_out', 'groceries', 'other', 'holiday']) as cat_group
    )
),

expense as (
    select
        date,
        person,
        amount,
        case
            when cat_name = 'Holiday' then 'holiday'
            when subcat_name_old = 'Dining out' then 'dining_out'
            when subcat_name_old = 'Groceries' then 'groceries' else 'other'
        end as cat_group
    from {{ ref('fct_expenses') }}
),

expense_monthly as (
    select
        date.person,
        date.cat_group,
        date.month_date,
        date.year_num,
        date.year_month_id,
        date.month_index,
        date.month_date_ly,
        SUM(expense.amount) as amount
    from date
    left join
        expense
        on
            DATE_TRUNC(
                expense.date, month
            ) = date.month_date and expense.person = date.person
            and date.cat_group = expense.cat_group
    group by 1, 2, 3, 4, 5, 6, 7
)
,

base as (
    select
        person,
        cat_group,
        month_date,
        year_num,
        year_month_id,
        month_index,
        month_date_ly,
        amount as amount,
        SUM(amount) over (
            partition by person, cat_group, year_num
            order by month_date
        ) as amount_ytd,
        SUM(amount) over (
            partition by person, cat_group
            order by month_index
            range between 2 preceding and 0 preceding
        ) as amount_3mnth,
        SUM(amount) over (
            partition by person, cat_group
            order by month_index
            range between 1 preceding and 1 preceding
        ) as amount_prev_mnth
    from expense_monthly
)
,
base_stage as (
    select
        base.person,
        base.cat_group,
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
        date
        on
            base.person = date.person
            and base.month_date = date.month_date
            and base.cat_group = date.cat_group
    union all
    select
        base.person,
        base.cat_group,
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
        and base.cat_group = date.cat_group
),


base_stage2 as (
    select
        person,
        cat_group,
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
    group by 1, 2, 3
),

base_stage3 as (
    select *
    from (
        select *
        from base_stage2
    )
    pivot (
        SUM(amount_ty) as amount_ty,
        SUM(amount_ly) as amount_ly,
        SUM(amount_ytd_ty) as amount_ytd_ty,
        SUM(amount_ytd_ly) as amount_ytd_ly,
        SUM(amount_3mnth_ty) as amount_3mnth_ty,
        SUM(amount_3mnth_ly) as amount_3mnth_ly
        for cat_group in ('dining_out', 'groceries', 'other', 'holiday')
    )
)

select
    person,
    month_date,
    SUM(amount_ty_dining_out) as amount_ty_dining_out,
    SUM(amount_ly_dining_out) as amount_ly_dining_out,
    SUM(amount_ytd_ty_dining_out) as amount_ytd_ty_dining_out,
    SUM(amount_ytd_ly_dining_out) as amount_ytd_ly_dining_out,
    SUM(amount_3mnth_ty_dining_out) as amount_3mnth_ty_dining_out,
    SUM(amount_3mnth_ly_dining_out) as amount_3mnth_ly_dining_out,
    SUM(amount_ty_groceries) as amount_ty_groceries,
    SUM(amount_ly_groceries) as amount_ly_groceries,
    SUM(amount_ytd_ty_groceries) as amount_ytd_ty_groceries,
    SUM(amount_ytd_ly_groceries) as amount_ytd_ly_groceries,
    SUM(amount_3mnth_ty_groceries) as amount_3mnth_ty_groceries,
    SUM(amount_3mnth_ly_groceries) as amount_3mnth_ly_groceries,
    SUM(amount_ty_other) as amount_ty_other,
    SUM(amount_ly_other) as amount_ly_other,
    SUM(amount_ytd_ty_other) as amount_ytd_ty_other,
    SUM(amount_ytd_ly_other) as amount_ytd_ly_other,
    SUM(amount_3mnth_ty_other) as amount_3mnth_ty_other,
    SUM(amount_3mnth_ly_other) as amount_3mnth_ly_other,
    SUM(amount_ty_holiday) as amount_ty_holiday,
    SUM(amount_ly_holiday) as amount_ly_holiday,
    SUM(amount_ytd_ty_holiday) as amount_ytd_ty_holiday,
    SUM(amount_ytd_ly_holiday) as amount_ytd_ly_holiday,
    SUM(amount_3mnth_ty_holiday) as amount_3mnth_ty_holiday,
    SUM(amount_3mnth_ly_holiday) as amount_3mnth_ly_holiday
from base_stage3
group by 1, 2
