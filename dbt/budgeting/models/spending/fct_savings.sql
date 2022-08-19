with savings as (

  SELECT * FROM {{ref('stg_savings_cad')}}
)

select * from savings
