WITH
savings AS (
    SELECT * FROM {{ source('bigquery', 't_balances_fact') }}
),

data_entries_stage AS (
    SELECT DISTINCT
        date_dt,
        person
    FROM savings
),

data_entries AS (
    SELECT
        *,
        COALESCE(
            LEAD(date_dt) OVER (PARTITION BY person ORDER BY date_dt),
            '2999-01-01'
        ) AS date_to
    FROM data_entries_stage
)

SELECT
    savings.*,
    data_entries.date_to
FROM savings
INNER JOIN
    data_entries ON
        savings.person = data_entries.person AND savings.date_dt = data_entries.date_dt
