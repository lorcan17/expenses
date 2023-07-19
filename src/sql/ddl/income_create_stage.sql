CREATE TABLE `python-splitwise.budgeting.t_income_stage`
(
    id STRING,
    date_dt DATE,
    source STRING,
    category STRING,
    person STRING,
    amount FLOAT64,
    currency STRING,
    process STRING,
    insert_dt DATE DEFAULT CURRENT_DATE(),
    update_dt DATE DEFAULT CURRENT_DATE()

)
OPTIONS (
    description
    = "This is a staging table to keep income before merging into t_income_fact",
    expiration_timestamp = "2999-12-31" -- Set a large expiration time

);
