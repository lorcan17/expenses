with max_date as (

    select
        date(min(date_dt)) as min_date_dt,
        date(max(date_dt)) as max_date_dt
    from {{ source('bigquery', 't_expenses_fact') }}

)
,
generic_date_dim as (

    select
        date_dt,
        date_dt as date,
        cast(format_date('%Y%m', date_dt) as NUMERIC) as year_month_id,
        format_date('%F', date_dt) as date_id,
        -- date nums
        date_add(date_dt, interval - 1 year) as date_ly,
        extract(year from date_dt) as year_num,
        extract(quarter from date_dt) as quarter_num,
        extract(month from date_dt) as month_num,
        extract(week from date_dt) as week_num,
        extract(day from date_dt) as day_num,
        -- date truncs
        date_trunc(date_dt, year) as year_date,
        date_trunc(date_dt, quarter) as quarter_date,
        date_trunc(date_dt, month) as month_date,
        date_trunc(
            date_add(date_dt, interval - 1 year), month
        ) as month_date_ly,
        date_trunc(date_dt, week) as week_date,
        -- date strings
        format_date('%Y %b', date_dt) as year_month,
        format_date('%B', date_dt) as month_name,
        format_date('%A', date_dt) as day_name,
        case when format_date('%A', date_dt) in ('Sunday', 'Saturday')
            then 0 else 1 end as day_is_weekday
    from (
            select *
            from
                unnest(
                    generate_date_array(
                        '2014-01-01', '2050-01-01', interval 1 day
                    )
                ) as date_dt
    )
)

select
    generic_date_dim.*,
    case
        when
            date_trunc(
                generic_date_dim.date_dt, month
            ) = date_trunc(max_date.max_date_dt, month) then 'This Month'
        when
            date_trunc(
                date_add(generic_date_dim.date_dt, interval 1 month), month
            ) = date_trunc(max_date.max_date_dt, month) then 'Last Month'

        else generic_date_dim.year_month
    end as year_month_report,
    dense_rank() over (order by generic_date_dim.month_date asc) as month_index
from generic_date_dim
cross join max_date
where 1 = 1
    and generic_date_dim.date <= max_date.max_date_dt
    and generic_date_dim.date >= max_date.min_date_dt
