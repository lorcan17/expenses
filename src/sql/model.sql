select
    cat_name_subcat_name,
    exp_desc
from budgeting.stg_expenses as a inner join
    budgeting.dim_splitwise_category as b
    on a.subcat_id = b.subcat_id
where extract(year from date) >= 2023
