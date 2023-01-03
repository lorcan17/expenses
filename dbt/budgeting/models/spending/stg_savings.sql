WITH
savings AS (
    SELECT * FROM {{ source('bigquery', 'gsheet_savings') }}
),

data_entries_stage AS (
    SELECT DISTINCT
        date,
        person
    FROM savings
),

data_entries AS (
    SELECT
        *,
        COALESCE(
            LEAD(date) OVER (PARTITION BY person ORDER BY date),
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
        savings.person = data_entries.person AND savings.date = data_entries.date
