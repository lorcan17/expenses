with savings as (
  SELECT *  FROM {{ref('fct_savings')}}
)

SELECT  month_date,
        month_date_to,
        person,
        max(amount) as total_savings_amount
FROM
      (SELECT
          DATE_TRUNC(date, MONTH) as month_date,
          DATE_TRUNC(date_to, MONTH) as month_date_to,
          person,
          SUM(amount_cad) as amount
          fROM savings
          GROUP BY
          1,2,3
        )
GROUP BY
1,2,3
