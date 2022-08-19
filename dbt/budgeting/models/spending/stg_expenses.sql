with expenses as (

    select * from {{ source('bigquery', 'splitwise_expenses') }}

),
exchange_rate as (

    select * from {{ source('bigquery', 'exchange_rate_dim') }}

)

select
  ex.date,
  delete_date,
  created_date
  updated_date,
  exp_id,
  subcat_id,
  subcat_name,
  exp_desc,
  creation_method,
  exp_currency,
  user_id,
  first_name,
  last_name,
  CASE WHEN exp_currency = 'CAD' THEN exp_cost ELSE (1/cad_gbp_rate) * exp_cost END AS exp_cost,
  CASE WHEN exp_currency = 'CAD' THEN paid_share ELSE (1/cad_gbp_rate) * paid_share END AS paid_share,
  CASE WHEN exp_currency = 'CAD' THEN owed_share ELSE (1/cad_gbp_rate) * owed_share END AS owed_share
  FROM expenses ex
  LEFT JOIN
  exchange_rate er on DATE_TRUNC(ex,date, MONTH) = er.date
