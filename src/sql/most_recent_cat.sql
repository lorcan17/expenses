WITH ranked_expenses AS (
    SELECT
        cat_name_subcat_name,
        exp_desc,
        ROW_NUMBER()
            OVER (PARTITION BY exp_desc ORDER BY date DESC)
            AS row_num
    FROM
        budgeting.stg_expenses AS a
    INNER JOIN
        budgeting.dim_splitwise_category AS b
        ON a.subcat_id = b.subcat_id
    WHERE
        EXTRACT(YEAR FROM date) >= 2023
)

SELECT
    cat_name_subcat_name,
    exp_desc
FROM
    ranked_expenses
WHERE
    row_num = 1;
