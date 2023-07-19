MERGE INTO budgeting.t_income_fact AS t
USING budgeting.t_income_stage AS s
    ON
        t.date_dt = s.date_dt
        AND t.source = s.source
        AND t.category = s.category
        AND t.person = s.person

WHEN MATCHED THEN
    UPDATE SET
        t.amount = s.amount,
        t.currency = s.currency,
        t.process = s.process,
        t.update_dt = CURRENT_DATE()

WHEN NOT MATCHED THEN
    INSERT
        (
            id,
            date_dt,
            source,
            category,
            person,
            amount,
            currency,
            process,
            insert_dt,
            update_dt
        )
    VALUES (
        GENERATE_UUID(),
        s.date_dt,
        s.source,
        s.category,
        s.person,
        s.amount,
        s.currency,
        s.process,
        CURRENT_DATE(),
        CURRENT_DATE()
    );
