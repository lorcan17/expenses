with income as (

    select *
    from {{ ref('fct_income_monthly') }}

),

expenses as (

    select *
    from {{ ref('fct_expenses_monthly') }}


),

savings as (

    select *
    from {{ ref('fct_savings_monthly') }}

),

grouped_expenses as (
    select *
    from {{ ref('fct_grouped_expenses_monthly') }}
),

base as (

    select distinct
        year_date,
        quarter_date,
        month_date,
        year_month_id,
        year_month_report,
        person
    from {{ ref('dim_date') }}
    cross join (
        select 'Lorcan' as person
        union all
        select 'Grace' as person
    )

),

stage1 as (

    select
        base.month_date,
        base.year_month_report,
        base.person,
        --e.cat_name,
        --e.subcat_name,
        total_savings_amount,
        income_amount_ty,
        income_amount_ly,
        income_amount_ytd_ty,
        income_amount_ytd_ly,
        income_amount_3mnth_ty,
        income_amount_3mnth_ly,
        income_amount_prev_mnth_ty,
        income_amount_prev_mnth_ly,
        expense_amount_ty,
        expense_amount_ly,
        expense_amount_ytd_ty,
        expense_amount_ytd_ly,
        expense_amount_3mnth_ty,
        expense_amount_3mnth_ly,
        expense_amount_prev_mnth_ty,
        expense_amount_prev_mnth_ly,
        income_amount_ty - expense_amount_ty as net_savings_ty,
        income_amount_ly - expense_amount_ly as net_savings_ly,
        income_amount_ytd_ty - expense_amount_ytd_ty as net_savings_ytd_ty,
        income_amount_ytd_ly - expense_amount_ytd_ly as net_savings_ytd_ly,
        income_amount_3mnth_ty
        - expense_amount_3mnth_ty as net_savings_3mnth_ty,
        income_amount_3mnth_ly
        - expense_amount_3mnth_ly as net_savings_3mnth_ly,
        income_amount_prev_mnth_ty
        - expense_amount_prev_mnth_ty as net_savings_prev_mnth_ty,
        income_amount_prev_mnth_ly
        - expense_amount_prev_mnth_ly as net_savings_prev_mnth_ly,
        amount_ty_dining_out,
        amount_ly_dining_out,
        amount_ytd_ty_dining_out,
        amount_ytd_ly_dining_out,
        amount_3mnth_ty_dining_out,
        amount_3mnth_ly_dining_out,
        amount_ty_groceries,
        amount_ly_groceries,
        amount_ytd_ty_groceries,
        amount_ytd_ly_groceries,
        amount_3mnth_ty_groceries,
        amount_3mnth_ly_groceries,
        amount_ty_other,
        amount_ly_other,
        amount_ytd_ty_other,
        amount_ytd_ly_other,
        amount_3mnth_ty_other,
        amount_3mnth_ly_other,
        amount_ty_holiday,
        amount_ly_holiday,
        amount_ytd_ty_holiday,
        amount_ytd_ly_holiday,
        amount_3mnth_ty_holiday,
        amount_3mnth_ly_holiday,
        SUM(amount_ty_holiday)
            over (
                partition by base.person
                order by extract(year from base.month_date)
                range between 1 preceding and 1 preceding
            )
            as amount_ly_all_holiday

    from base
    left join
        income
        on
            base.month_date = income.month_date and base.person = income.person
    left join
        expenses
        on
            base.month_date = expenses.month_date
            and base.person = expenses.person
    left join
        savings
        on
            base.month_date >= savings.month_date
            and base.month_date < savings.month_date_to
            and base.person = savings.person
    left join
        grouped_expenses
        on
            base.month_date = grouped_expenses.month_date
            and base.person = grouped_expenses.person
    order by
        1, 2
),

stage2 as (
    select
        *,
        SUM(net_savings_ty) over (
            partition by person
            order by month_date
        ) as net_savings_rolling
    from stage1
)

select
    *,
    COALESCE(
        total_savings_amount, net_savings_rolling
    ) as total_savings_amount_est
from stage2
