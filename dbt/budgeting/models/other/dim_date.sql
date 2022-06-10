with max_date as (

      select  date(min(date)) as min_date,
              date(max(date)) as max_date
      from {{ source('bigquery', 'splitwise_expenses') }}

  )
,
generic_date_dim AS (

SELECT
  FORMAT_DATE('%F', d) as id,
  d AS full_date,
  -- date nums
  EXTRACT(YEAR FROM d) AS year_num,
  EXTRACT(QUARTER FROM d) AS quarter_num,
  EXTRACT(MONTH FROM d) AS month_num,
  EXTRACT(WEEK FROM d) AS week_num,
  EXTRACT(DAY FROM d) AS day_num,
  -- date truncs
  DATE_TRUNC(d, YEAR) as year_date,
  DATE_TRUNC(d, QUARTER) as quarter_date,
  DATE_TRUNC(d, MONTH) as month_date,
  DATE_TRUNC(d, WEEK) as week_date,
  -- date strings
  FORMAT_DATE('%Y %b', d) as year_month,
  FORMAT_DATE('%B', d) as month_name,
  FORMAT_DATE('%A', d) AS day_name,
  CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday')
        THEN 0 ELSE 1 END AS day_is_weekday,
FROM (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2014-01-01', '2050-01-01', INTERVAL 1 DAY)) AS d )
)
SELECT *,
CASE
WHEN
  EXTRACT(MONTH FROM dd.full_date) = EXTRACT(MONTH FROM a.max_date) then "Current Month"
WHEN
  EXTRACT(MONTH FROM dd.full_date) = EXTRACT(MONTH FROM a.max_date) then "Previous Month"

ELSE year_month
END AS year_month_report FROM generic_date_dim dd
CROSS JOIN max_date a
WHERE dd.full_date <= a.max_date AND dd.full_date >= a.min_date
