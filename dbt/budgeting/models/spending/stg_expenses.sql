with expenses as (

    select * from {{ source('bigquery', 'splitwise_expenses') }}

),
category as (
  select * from {{ source('bigquery', 'dim_splitwise_category') }}
),
exchange_rate as (

    select * from {{ source('bigquery', 'exchange_rate_dim') }}

),
final as (
select
  ex.*,
  cat.cat_id,
  cat.cat_name,
  CASE
    WHEN  LOWER(ex.exp_desc) LIKE '%.hol%' OR
          LOWER(ex.exp_desc) LIKE '%hol.%' THEN "Holiday"
    WHEN  LOWER(ex.exp_desc) LIKE '%.big%' OR
          LOWER(ex.exp_desc) LIKE '%big.%' THEN "Big Purchase"
    ELSE cat.cat_name
    END AS cat_name_new,
  CASE
    WHEN  LOWER(ex.exp_desc) LIKE '%.pub%' OR
          LOWER(ex.exp_desc) LIKE '%pub.%' THEN "Pub"
    WHEN  LOWER(ex.exp_desc) LIKE '%.imm%' OR
          LOWER(ex.exp_desc) LIKE '%imm.%' THEN "Immigration Costs"
    ELSE ex.subcat_name
    END as subcat_name_new,
  CASE WHEN exp_currency = 'CAD' THEN exp_cost ELSE (1/cad_gbp_rate) * exp_cost END AS exp_cost_cad,
  CASE WHEN exp_currency = 'CAD' THEN paid_share ELSE (1/cad_gbp_rate) * paid_share END AS paid_share_cad,
  CASE WHEN exp_currency = 'CAD' THEN owed_share ELSE (1/cad_gbp_rate) * owed_share END AS owed_share_cad
  FROM expenses ex
  LEFT JOIN
  exchange_rate er on DATE_TRUNC(ex.date, MONTH) = er.date
  LEFT JOIN category cat on ex.subcat_id = cat.subcat_id
  where 1=1
  AND deleted_date IS NULL
  AND ifnull(creation_method,'python') NOT IN ('debt_consolidation','payment')
)
select
  date,
  deleted_date,
  created_date
  updated_date,
  exp_id,
  cat_id,
  cat_name,
  cat_name_new,
  subcat_id,
  subcat_name,
  subcat_name_new,
  exp_desc,
  ifnull(creation_method,'python')  as  creation_method,
  user_id,
  first_name,
  last_name,
  exp_currency,
  exp_cost,
  paid_share,
  owed_share,
  exp_cost_cad,
  paid_share_cad,
  owed_share_cad
  from final
