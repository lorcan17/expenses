with income as (

    SELECT  DATE_TRUNC(date, MONTH) as month_date,
            person,
            SUM(amount) as income_amount
    FROM {{ source('bigquery', 'gsheet_income') }}
    GROUP BY
    1,2

),
expenses as (

  SELECT  DATE_TRUNC(date, MONTH) as month_date,
          first_name as person,
          SUM(owed_share) as expense_amount
  FROM {{ref('fct_expenses')}}
  GROUP BY
  1,2

),
base as (

  select distinct year_date, quarter_date, month_date, person  from {{ref('dim_date')}}
  cross join (select 'Lorcan' as person union all select 'Grace' as person)

)

select
  b.month_date,
  b.person,
  e.expense_amount,
  i.income_amount,
  i.income_amount - e.expense_amount as net_savings_amount
  FROM base b
  LEFT JOIN income i on b.month_date = i.month_date and b.person = i.person
  LEFT JOIN expenses e on b.month_date = e.month_date and b.person = e.person
  ORDER BY
  b.month_date
