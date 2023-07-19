CREATE TABLE `python-splitwise.budgeting.t_expenses_stage`
(
    exp_id INT64,
    date_dt DATETIME,
    subcat_id INT64,
    subcat_name STRING,
    exp_desc STRING,
    creation_method STRING,
    exp_cost FLOAT64,
    exp_currency STRING,
    user_id INT64,
    first_name STRING,
    last_name STRING,
    net_balance FLOAT64,
    paid_share FLOAT64,
    owed_share FLOAT64,
    deleted_dt DATETIME,
    created_dt DATETIME,
    updated_dt DATETIME,
    etl_insert_dt DATE DEFAULT CURRENT_DATE(),
    etl_update_dt DATE DEFAULT CURRENT_DATE()
)
OPTIONS (
    expiration_timestamp = "2999-12-31"
);
