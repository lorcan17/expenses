MERGE INTO budgeting.t_expenses_fact AS t
USING budgeting.t_expenses_stage AS s
    ON t.exp_id = s.exp_id

WHEN MATCHED AND (
    t.date_dt != s.date_dt
    OR t.subcat_id != s.subcat_id
    OR t.subcat_name != s.subcat_name
    OR t.exp_desc != s.exp_desc
    OR t.creation_method != s.creation_method
    OR t.exp_cost != s.exp_cost
    OR t.exp_currency != s.exp_currency
    OR t.user_id != s.user_id
    OR t.first_name != s.first_name
    OR t.last_name != s.last_name
    OR t.net_balance != s.net_balance
    OR t.paid_share != s.paid_share
    OR t.owed_share != s.owed_share
    OR t.deleted_dt != s.deleted_dt
    OR t.created_dt != s.created_dt
    OR t.updated_dt != s.updated_dt
)
THEN
    UPDATE SET
        t.date_dt = s.date_dt,
        t.subcat_id = s.subcat_id,
        t.subcat_name = s.subcat_name,
        t.exp_desc = s.exp_desc,
        t.creation_method = s.creation_method,
        t.exp_cost = s.exp_cost,
        t.exp_currency = s.exp_currency,
        t.user_id = s.user_id,
        t.first_name = s.first_name,
        t.last_name = s.last_name,
        t.net_balance = s.net_balance,
        t.paid_share = s.paid_share,
        t.owed_share = s.owed_share,
        t.deleted_dt = s.deleted_dt,
        t.created_dt = s.created_dt,
        t.updated_dt = s.updated_dt,
        t.etl_update_dt = CURRENT_DATE()

WHEN NOT MATCHED
THEN
    INSERT
        (
            exp_id,
            date_dt,
            subcat_id,
            subcat_name,
            exp_desc,
            creation_method,
            exp_cost,
            exp_currency,
            user_id,
            first_name,
            last_name,
            net_balance,
            paid_share,
            owed_share,
            deleted_dt,
            created_dt,
            updated_dt,
            etl_insert_dt,
            etl_update_dt
        )
    VALUES (
        s.exp_id,
        s.date_dt,
        s.subcat_id,
        s.subcat_name,
        s.exp_desc,
        s.creation_method,
        s.exp_cost,
        s.exp_currency,
        s.user_id,
        s.first_name,
        s.last_name,
        s.net_balance,
        s.paid_share,
        s.owed_share,
        s.deleted_dt,
        s.created_dt,
        s.updated_dt,
        CURRENT_DATE(),
        CURRENT_DATE()
    );
