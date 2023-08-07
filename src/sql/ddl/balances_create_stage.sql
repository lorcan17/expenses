CREATE TABLE `python-splitwise.budgeting.t_balances_stage`
(
    id STRING,
    date_dt DATETIME,
    source STRING,
    product STRING,
    category STRING,
    person STRING,
    amount FLOAT64,
    currency STRING,
    process STRING,
    etl_insert_dt DATE DEFAULT CURRENT_DATE(),
    etl_update_dt DATE DEFAULT CURRENT_DATE()


)
OPTIONS (
    description
    = "This is a staging table to keep the balances before merging into t_balances_fact",
    expiration_timestamp = "2999-12-31" -- Set a large expiration time

);
