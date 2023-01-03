with expenses as (

    select * from {{ source('bigquery', 'splitwise_expenses') }}

),

category as (
    select * from {{ source('bigquery', 'dim_splitwise_category') }}
),

exchange_rate as (

    select * from {{ source('bigquery', 'exchange_rate_dim') }}

),

final as (
    select
        expenses.*,
        category.cat_id,
        category.cat_name,
        case
            when LOWER(expenses.exp_desc) like '%.hol%'
                or LOWER(expenses.exp_desc) like '%hol.%' then "Holiday"
            when LOWER(expenses.exp_desc) like '%.asset%'
                or LOWER(expenses.exp_desc) like '%asset.%' then "Asset"
            when LOWER(expenses.exp_desc) like '%.imm%'
                or LOWER(
                    expenses.exp_desc
                ) like '%imm.%' then "Immigration Costs"
            else category.cat_name
        end as cat_name_new,
        case
            when LOWER(expenses.exp_desc) like '%.pub%'
                or LOWER(expenses.exp_desc) like '%pub.%' then "Pub"
            when LOWER(expenses.exp_desc) like '%.togo%'
                or LOWER(
                    expenses.exp_desc
                ) like '%togo.%' then "To go snack / drinks"
            when LOWER(expenses.exp_desc) like '%.self%'
                or LOWER(expenses.exp_desc) like '%self.%' then "Self Care"
            else expenses.subcat_name
        end as subcat_name_new,
        case
            when
                exp_currency = 'CAD' then exp_cost
            else (1 / cad_gbp_rate) * exp_cost
        end as exp_cost_cad,
        case
            when
                exp_currency = 'CAD' then paid_share
            else (1 / cad_gbp_rate) * paid_share
        end as paid_share_cad,
        case
            when
                exp_currency = 'CAD' then owed_share
            else (1 / cad_gbp_rate) * owed_share
        end as owed_share_cad
    from expenses
    left join
        exchange_rate on DATE_TRUNC(expenses.date, month) = exchange_rate.date
    left join category on expenses.subcat_id = category.subcat_id
    where 1 = 1
        and deleted_date is null
        and COALESCE(
            creation_method, 'python'
        ) not in ('debt_consolidation', 'payment')
)

select
    date,
    deleted_date,
    created_date
    as updated_date,
    exp_id,
    cat_id,
    cat_name,
    cat_name_new,
    subcat_id,
    subcat_name,
    subcat_name_new,
    exp_desc,
    user_id,
    first_name,
    last_name,
    exp_currency,
    exp_cost,
    paid_share,
    owed_share,
    exp_cost_cad,
    paid_share_cad,
    owed_share_cad,
    COALESCE(creation_method, 'python') as creation_method
from final
