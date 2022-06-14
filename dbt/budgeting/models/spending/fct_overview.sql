with income as (

    SELECT  *
    FROM {{ref('fct_income_monthly')}}

),
expenses as (

  SELECT  *
  FROM {{ref('fct_expenses_monthly')}}


),savings as (

  SELECT  *
  FROM {{ref('fct_savings_monthly')}}

),
base as (

  select  distinct year_date,
          quarter_date,
          month_date,
          year_month_id,
          person
  from {{ref('dim_date')}}
  cross join (
          select 'Lorcan' as person
          union all
          select 'Grace' as person
        )

)

select
  b.month_date,
  b.person,
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
  income_amount_ytd_ly - expense_amount_ytd_ly as net_savings_ytd_ly ,
  income_amount_3mnth_ty - expense_amount_3mnth_ty as net_savings_3mnth_ty,
  income_amount_3mnth_ly - expense_amount_3mnth_ly as net_savings_3mnth_ly,
  income_amount_prev_mnth_ty - expense_amount_prev_mnth_ty as net_savings_prev_mnth_ty,
  income_amount_prev_mnth_ly - expense_amount_prev_mnth_ly as net_savings_prev_mnth_ly
  FROM base b
  LEFT JOIN income i on b.month_date = i.month_date and b.person = i.person
  LEFT JOIN expenses e on b.month_date = e.month_date and b.person = e.person
  LEFT JOIN savings s on b.month_date >= s.month_date and b.month_date < s.month_date_to and b.person = s.person
  ORDER BY
  1,2
