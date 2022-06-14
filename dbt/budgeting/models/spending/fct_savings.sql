WITH
savings as (
  select * from {{ source('bigquery', 'gsheet_savings') }}
),
data_entries_stage as (
  select distinct
  date,
  person
  from savings
),
data_entries as (
select  *,
        IFNULL(
          LEAD(date) OVER (PARTITION BY person ORDER BY date),
          '2999-01-01'
        ) as date_to
 from data_entries_stage
 )

SELECT s.*,
CASE WHEN currency = 'CAD' then amount else amount * 1.57 end as amount_cad,
d.date_to FROM savings s
inner join data_entries d on s.person = d.person and s.date = d.date
