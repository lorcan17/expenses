with expenses as (

    select * from {{ source('bigquery', 'splitwise_expenses') }}

),
category as (
  select * from {{ source('bigquery', 'dim_splitwise_category') }}
)

,fct as (
select
  ex.date,
  --deleted_date,
  ex.exp_id,
  --cat.cat_name,
  --cat.subcat_name,
  cat.cat_name,
  CASE
    WHEN  LOWER(ex.exp_desc) LIKE '%.pub%' OR
          LOWER(ex.exp_desc) LIKE '%pub.%' THEN "Pub"
    WHEN  LOWER(ex.exp_desc) LIKE '%.big%' OR
          LOWER(ex.exp_desc) LIKE '%big.%'
          --LOWER(ex.subcat_name) = 'taxes'
          THEN "Big Purchase"
    WHEN  LOWER(ex.exp_desc) LIKE '%.hol%' OR
          LOWER(ex.exp_desc) LIKE '%hol.%' THEN "Holiday"
    WHEN  LOWER(ex.exp_desc) LIKE '%.imm%' OR
          LOWER(ex.exp_desc) LIKE '%imm.%' THEN "Immigration Costs"
    ELSE ex.subcat_name
    END as subcat_name,
  ex.exp_desc,
  ifnull(creation_method,'python')  as  creation_method,
  ex.exp_cost ,
  ex.exp_currency,
  --user_id,
  ex.first_name,
  ex.first_name as person,
  ex.last_name,
  ex.net_balance,
  ex.paid_share,
  ex.owed_share,
  ex.owed_share as amount,
  case when first_name = 'Lorcan' then paid_share else 0 end as lorcan_paid,
  case when first_name = 'Grace' then paid_share  else 0 end as grace_paid,
  case when first_name = 'Lorcan' then owed_share else 0 end as lorcan_owed,
  case when first_name = 'Grace' then owed_share else 0 end as grace_owed
  from expenses ex
  left join category cat on ex.subcat_id = cat.subcat_id
  where 1=1
  AND deleted_date IS NULL
  AND ifnull(creation_method,'python') NOT IN ('debt_consolidation','payment')
),

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
