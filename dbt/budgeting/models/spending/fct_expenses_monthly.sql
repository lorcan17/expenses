with date as (
  select  distinct person,
          year_date,
          quarter_date,
          month_date,
          year_num,
          year_month_id,
          month_index,
          month_date_ly
   from {{ref('dim_date')}}
   cross join (
           select 'Lorcan' as person
           union all
           select 'Grace' as person
         )
),
expense as (
  SELECT * FROM {{ref('fct_expenses')}}
  where big_purchase_flag = 0
),
expense_monthly as (
    select
          d.person,
          d.month_date,
          d.year_num,
          d.year_month_id,
          d.month_index,
          d.month_date_ly,
          SUM(amount) as amount
          from date d
          left join expense i on DATE_TRUNC(i.date, MONTH) =  d.month_date AND i.person = d.person
          group by 1,2,3,4,5,6
),
base as (
  SELECT  person,
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
            ORDER BY month_index
            RANGE BETWEEN 2 PRECEDING AND 0 PRECEDING
          ) as amount_3mnth,
          SUM(amount) over (
            partition by person
            ORDER BY month_index
            RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING
          ) as amount_prev_mnth
from expense_monthly
),
base_stage as (
select  a.person,
        d.month_date,
        amount as amount_ty,
        amount_ytd as amount_ytd_ty,
        amount_3mnth as amount_3mnth_ty,
        amount_prev_mnth as amount_prev_mnth_ty,
        NULL as amount_ly,
        NULL as amount_ytd_ly,
        NULL as amount_3mnth_ly,
        NULL as amount_prev_mnth_ly
        from base a
        join date d on a.person = d.person and a.month_date = d.month_date
UNION all
select  a.person,
        d.month_date,
        NULL as amount_ty,
        NULL as amount_ytd_ty,
        NULL as amount_3mnth_ty,
        NULL as amount_prev_mnth_ty,
        amount as amount_ly,
        amount_ytd as amount_ytd_ly,
        amount_3mnth as amount_3mnth_ly,
        amount_prev_mnth as amount_prev_mnth_ly
        from base a
        join date d on a.person = d.person and a.month_date = d.month_date_ly
),
base_stage2 as (
      select  person,
              month_date,
              sum(amount_ty) as amount_ty,
              sum(amount_ytd_ty) as amount_ytd_ty,
              sum(amount_3mnth_ty)/3 as amount_3mnth_ty,
              sum(amount_prev_mnth_ty) as amount_prev_mnth_ty,
              sum(amount_ly) as amount_ly,
              sum(amount_ytd_ly) as amount_ytd_ly,
              sum(amount_3mnth_ly)/3 as amount_3mnth_ly,
              sum(amount_prev_mnth_ly) as amount_prev_mnth_ly
              from base_stage
              group by 1,2
      )

      select  person,
              month_date,
              --ty
              amount_ty as expense_amount_ty,
              amount_ly as expense_amount_ly,
              --(amount_ty - amount_ly)/ amount_ly as expense_amount_yoy,
              amount_ytd_ty as expense_amount_ytd_ty,
              amount_ytd_ly as expense_amount_ytd_ly,
              --(amount_ytd_ty - amount_ytd_ly)/ amount_ytd_ly as expense_amount_ytd_yoy_pct,
              amount_3mnth_ty as expense_amount_3mnth_ty,
              amount_3mnth_ly as expense_amount_3mnth_ly,
              --(amount_3mnth_ty - amount_3mnth_ly)/ amount_3mnth_ly as expense_amount_3mnth_yoy_pct,
              amount_prev_mnth_ty as expense_amount_prev_mnth_ty,
              --month on month
              --(amount_ty - amount_prev_mnth_ty)/ amount_prev_mnth_ty as expense_amount_mom_ty_pct,
              amount_prev_mnth_ly as expense_amount_prev_mnth_ly
              --(amount_ly - amount_prev_mnth_ly)/ amount_prev_mnth_ly as expense_amount_mom_ly_pct
       from base_stage2
