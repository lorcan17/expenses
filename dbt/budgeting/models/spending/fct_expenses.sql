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
  cat.cat_name,
  cat.subcat_name,
  ex.exp_desc,
  CASE
    WHEN  LOWER(ex.exp_desc) LIKE '%.pub%' OR
          LOWER(ex.exp_desc) LIKE '%pub.%' THEN "Pub"
    WHEN  LOWER(ex.exp_desc) LIKE '%.misc%' OR
          LOWER(ex.exp_desc) LIKE '%misc.%' THEN "Miscellaneous Ess."
    WHEN  LOWER(ex.exp_desc) LIKE '%.nisc%' OR
          LOWER(ex.exp_desc) LIKE '%nisc.%' THEN "Miscellaneous Non-Ess."
    WHEN  LOWER(ex.exp_desc) LIKE '%lorcan%' OR
          LOWER(ex.exp_desc) LIKE '%grace%' THEN "Personal Spending"
          END as parent_category,


  ex.creation_method,
  ex.exp_cost ,
  ex.exp_currency,
  --user_id,
  ex.first_name,
  ex.last_name,
  ex.net_balance,
  ex.paid_share,
  ex.owed_share
  from expenses ex
  left join category cat on ex.subcat_id = cat.subcat_id
  where 1=1
  AND deleted_date IS NULL
  AND creation_method NOT IN ('debt_consolidation','payment')
