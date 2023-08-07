WITH expenses AS (
    SELECT * FROM {{ ref('stg_expenses') }}
),

fct AS (
    SELECT
        date,
        exp_id,
        cast(exp_id AS string) AS str_exp_id,
        cat_name AS cat_name_old,
        subcat_name AS subcat_name_old,
        cat_name_new AS cat_name,
        subcat_name_new AS subcat_name,
        exp_desc,
        creation_method,
        exp_cost,
        exp_currency,
        first_name,
        first_name AS person,
        last_name,
        paid_share,
        owed_share,
        owed_share_cad AS amount,
        CASE
            WHEN first_name = 'Lorcan' THEN paid_share
            ELSE 0
        END AS lorcan_paid,
        CASE
            WHEN first_name = 'Grace' THEN paid_share
            ELSE 0
        END AS grace_paid,
        CASE
            WHEN first_name = 'Lorcan' THEN owed_share
            ELSE 0
        END AS lorcan_owed,
        CASE
            WHEN first_name = 'Grace' THEN owed_share
            ELSE 0
        END AS grace_owed
    FROM expenses
),

potential_duplicates AS (
    SELECT
        date,
        exp_cost,
        count(*) AS pd_count
    FROM (
        SELECT DISTINCT
            exp_id,
            date,
            exp_cost
        FROM fct
    )
    GROUP BY 1, 2
),

fct_with_windows AS (
    SELECT
        fct.*,
        sum(
            fct.lorcan_paid
        ) OVER (PARTITION BY fct.str_exp_id) AS ex_lorcan_paid,
        sum(fct.grace_paid) OVER (PARTITION BY fct.str_exp_id) AS ex_grace_paid,
        sum(
            fct.lorcan_owed
        ) OVER (PARTITION BY fct.str_exp_id) AS ex_lorcan_owed,
        sum(fct.grace_owed) OVER (PARTITION BY fct.str_exp_id) AS ex_grace_owed,
        CASE
            WHEN
                fct.cat_name IN ('Holiday', 'Asset', 'Immigration Costs') THEN 0
            ELSE 1
        END AS daily_spending_flag,
        CASE
            WHEN potential_duplicates.pd_count > 1 THEN 1 ELSE 0
        END AS dup_count
    FROM fct
    LEFT JOIN
        potential_duplicates ON
        fct.date = potential_duplicates.date
        AND fct.exp_cost = potential_duplicates.exp_cost
)
,
fct_with_hint AS (
    SELECT
        *,
        CASE
            WHEN ex_grace_owed > 0 AND ex_lorcan_owed > 0 THEN '[S]'
            WHEN ex_grace_owed = 0 THEN '[L]'
            WHEN ex_lorcan_owed = 0 THEN '[G]'
        END AS shared_hint,
        CASE WHEN dup_count = 1 THEN '[PD]' ELSE '' END AS dupe_hint
    FROM fct_with_windows
)

SELECT
    *,
    concat('...', shared_hint, str_exp_id, dupe_hint) AS report_desc
FROM fct_with_hint
