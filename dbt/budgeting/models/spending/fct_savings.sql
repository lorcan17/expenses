with savings as (

    select * from {{ ref('stg_savings_cad') }}
)

select * from savings
