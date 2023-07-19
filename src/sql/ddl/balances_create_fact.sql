CREATE TABLE `python-splitwise.budgeting.t_balances_fact`
(
    id STRING,
    date_dt DATE,
    source STRING,
    product STRING,
    category STRING,
    person STRING,
    amount FLOAT64,
    currency STRING,
    process STRING,
    insert_dt DATE DEFAULT CURRENT_DATE(),
    update_dt DATE DEFAULT CURRENT_DATE()

)
OPTIONS (
    description = "This is a fact table to store the balances",
    expiration_timestamp = "2999-12-31" -- Set a large expiration time

);
