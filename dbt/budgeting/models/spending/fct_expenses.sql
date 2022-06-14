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
  ex.owed_share as amount
  -- Number of users in Expense
  from expenses ex
  left join category cat on ex.subcat_id = cat.subcat_id
  where 1=1
  AND deleted_date IS NULL
  AND ifnull(creation_method,'python') NOT IN ('debt_consolidation','payment')
)
  select *,
  CASE
    WHEN subcat_name in ('Holiday', 'Big Purchase', 'Taxes', 'Immigration Costs')
    THEN 0
    ELSE 1 END AS daily_spending_flag,
  CASE
    WHEN subcat_name in ('Big Purchase', 'Taxes')
    THEN 1
    ELSE 0 END AS big_purchase_flag
  FROM fct
