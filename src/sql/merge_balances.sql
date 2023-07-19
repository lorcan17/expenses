
    MERGE BUDGETING.T_BALANCES_FACT AS t
    USING BUDGETING.T_BALANCES_STAGE AS s
    ON 
        t.date = s.date AND
        t.person = s.person AND
        t.source = s.source AND
        t.product = s.product AND
        t.category = s.category AND
        t.amount = s.amount AND
        t.currency = s.currency
    WHEN MATCHED THEN
        UPDATE SET 
            t.category = s.category,
            t.amount = s.amount,
            t.currency = s.currency,
            t.updated_dt = CURRENT_DATE()
            
    WHEN NOT MATCHED THEN
        INSERT (
            id,
            date,
            person,
            source,
            product,
            category,
            person,
            amount,
            currency,
            insert_dt,
            update_dt
        )
        VALUES (
            GENERATE_UUID(),
            s.date,
            s.person,
            s.source,
            s.product,
            s.category,
            s.person,
            s.amount,
            s.currency,
            CURRENT_DATE(),
            CURRENT_DATE()

        )