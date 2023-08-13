MERGE budgeting.t_balances_fact AS t
USING budgeting.t_balances_stage AS s
    ON
        t.date_dt = s.date_dt
        AND t.person = s.person
        AND t.source = s.source
        AND t.product = s.product
WHEN MATCHED AND (
    t.category != s.category
    OR t.amount != s.amount
    OR t.currency != s.currency
)
THEN
    UPDATE SET
        t.category = s.category,
        t.amount = s.amount,
        t.currency = s.currency,
        t.etl_update_dt = CURRENT_DATE()

WHEN NOT MATCHED THEN
    INSERT (
        id,
        date_dt,
        person,
        source,
        product,
        category,
        amount,
        currency,
        etl_insert_dt,
        etl_update_dt
    )
    VALUES (
        GENERATE_UUID(),
        s.date_dt,
        s.person,
        s.source,
        s.product,
        s.category,
        s.amount,
        s.currency,
        CURRENT_DATE(),
        CURRENT_DATE()

    )
