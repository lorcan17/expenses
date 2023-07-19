CREATE TABLE `python-splitwise.budgeting.t_balances_stage`
(
  date DATETIME,
  source STRING,
  product STRING,
  category STRING,
  person STRING,
  amount FLOAT64,
  currency STRING,
  process STRING,
  id INT64,
  insert_dt DATE DEFAULT CURRENT_DATE(),
  update_df DATE DEFAULT CURRENT_DATE(),
  CONSTRAINT unique_id UNIQUE(id)

)
OPTIONS(
    description = "This is a staging table to keep the balances before merging into t_balances_fact",
    expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 100 YEAR),  -- Set a large expiration time
  
);