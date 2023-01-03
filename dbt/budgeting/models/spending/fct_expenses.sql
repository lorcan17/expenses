with expenses as (

    select * from {{ ref('stg_expenses') }}
)
,
fct as (
    select
        date,
        exp_id,
        cat_name as cat_name_old,
        subcat_name as subcat_name_old,
        cat_name_new as cat_name,
        subcat_name_new as subcat_name,
        exp_desc,
        creation_method,
        exp_cost,
        exp_currency,
        first_name,
        first_name as person,
        last_name,
        paid_share,
        owed_share,
        owed_share_cad as amount,
        case
            when first_name = 'Lorcan' then paid_share else 0
        end as lorcan_paid,
        case
            when first_name = 'Grace' then paid_share else 0
        end as grace_paid,
        case
            when first_name = 'Lorcan' then owed_share else 0
        end as lorcan_owed,
        case when first_name = 'Grace' then owed_share else 0 end as grace_owed
    from expenses
),

potential_duplicates as (
    select
        date,
        exp_cost,
        count(*) as pd_count
    from (
        select distinct
            exp_id,
            date,
            exp_cost
        from fct
        )
    group by 1, 2

),

fct_2 as (
    select
        fct.*,
        case
            when cat_name in ('Holiday', 'Asset', 'Immigration Costs')
                then 0
            else 1 end as daily_spending_flag,
        case
            when potential_duplicates.pd_count > 1 then 1 else 0
        end as potential_dup_flag
    from fct
    left join
        potential_duplicates on
            fct.date = potential_duplicates.date and fct.exp_cost = potential_duplicates.exp_cost
)

select * from fct_2
