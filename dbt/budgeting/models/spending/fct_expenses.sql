with expenses as (

    select * from {{ source('splitwise_expenses', 'splitwise_expenses') }}

),
category as (
  select * from {{ source('splitwise_expenses', 'dim_splitwise_category') }}
)

select
  ex.date,
  --deleted_date,
  ex.exp_id,
  --cat.cat_name,
  --cat.subcat_name,
  CASE
    WHEN  LOWER(ex.exp_desc) LIKE '%.hol%' OR
          LOWER(ex.exp_desc) LIKE '%hol.%' THEN "Holiday"
    ELSE cat.cat_name
    END AS cat_name,
  CASE
    WHEN  LOWER(ex.exp_desc) LIKE '%.pub%' OR
          LOWER(ex.exp_desc) LIKE '%pub.%' THEN "Pub"
    WHEN  LOWER(ex.exp_desc) LIKE '%.big%' OR
          LOWER(ex.exp_desc) LIKE '%big.%' THEN "Big Purchase"
    WHEN  LOWER(ex.exp_desc) LIKE '%.hol%' OR
          LOWER(ex.exp_desc) LIKE '%hol.%' THEN "Holiday"
    WHEN  LOWER(ex.exp_desc) LIKE '%.imm%' OR
          LOWER(ex.exp_desc) LIKE '%imm.%' THEN "Immigration Costs"
    ELSE ex.subcat_name
    END as subcat_name,
  ex.exp_desc,
  ex.creation_method,
  ex.exp_cost ,
  ex.exp_currency,
  --user_id,
  ex.first_name,
  ex.last_name,
  ex.net_balance,
  ex.paid_share,
  ex.owed_share,
  -- Number of users in Expense
  from expenses ex
  left join category cat on ex.subcat_id = cat.subcat_id
  where 1=1
  AND deleted_date IS NULL
  AND ifnull(creation_method,'python') NOT IN ('debt_consolidation','payment')
