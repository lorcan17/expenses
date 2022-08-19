with expenses as (

  SELECT * FROM {{ref('stg_expenses')}}
)
,
fct as (
select
  date,
  exp_id,
  cat_name as cat_name_old,
  subcat_name as subcat_name_old,
  cat_name_new as cat_name,
  subcat_name_new as subcat_name,
  exp_desc,
  creation_method,
  exp_cost ,
  exp_currency,
  first_name,
  first_name as person,
  last_name,
  net_balance,
  paid_share,
  owed_share,
  owed_share_cad as amount,
  case when first_name = 'Lorcan' then paid_share else 0 end as lorcan_paid,
  case when first_name = 'Grace' then paid_share  else 0 end as grace_paid,
  case when first_name = 'Lorcan' then owed_share else 0 end as lorcan_owed,
  case when first_name = 'Grace' then owed_share else 0 end as grace_owed
  from expenses ex),

potential_duplicates as (
  select
  date,
  exp_cost,
  count(*) as pd_count
  from (
    select distinct exp_id, date, exp_cost from fct
  )
  group by 1,2

), fct_2 as (
  select fct.*,
  CASE
    WHEN subcat_name in ('Holiday', 'Big Purchase', 'Taxes', 'Immigration Costs')
    THEN 0
    ELSE 1 END AS daily_spending_flag,
  CASE
    WHEN subcat_name in ('Big Purchase', 'Taxes')
    THEN 1
    ELSE 0 END AS big_purchase_flag,
  CASE WHEN pd.pd_count > 1 then 1 else 0 end as potential_dup_flag
  FROM fct
  left join potential_duplicates pd on fct.date = pd.date and fct.exp_cost = pd.exp_cost
 )
 select * from fct_2
