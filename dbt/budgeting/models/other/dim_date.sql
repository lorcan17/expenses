with max_date as (

    select
        date(min(date)) as min_date,
        date(max(date)) as max_date
    from {{ source('bigquery', 'splitwise_expenses') }}

)
,
generic_date_dim as (

    select
        d as date,
        cast(format_date('%Y%m', d) as NUMERIC) as year_month_id,
        format_date('%F', d) as id,
        -- date nums
        date_add(d, interval - 1 year) as date_ly,
        extract(year from d) as year_num,
        extract(quarter from d) as quarter_num,
        extract(month from d) as month_num,
        extract(week from d) as week_num,
        extract(day from d) as day_num,
        -- date truncs
        date_trunc(d, year) as year_date,
        date_trunc(d, quarter) as quarter_date,
        date_trunc(d, month) as month_date,
        date_trunc(date_add(d, interval - 1 year), month) as month_date_ly,
        date_trunc(d, week) as week_date,
        -- date strings
        format_date('%Y %b', d) as year_month,
        format_date('%B', d) as month_name,
        format_date('%A', d) as day_name,
        case when format_date('%A', d) in ('Sunday', 'Saturday')
            then 0 else 1 end as day_is_weekday
    from (
            select *
            from
                unnest(
                    generate_date_array(
                        '2014-01-01', '2050-01-01', interval 1 day
                    )
                )
    )
)

select
    *,
    case
        when
            date_trunc(
                generic_date_dim.date, month
            ) = date_trunc(max_date.max_date, month) then "This Month"
        when
            date_trunc(
                date_add(generic_date_dim.date, interval 1 month), month
            ) = date_trunc(max_date.max_date, month) then "Last Month"

        else year_month
    end as year_month_report,
    dense_rank() over (order by month_date asc) as month_index
from generic_date_dim
cross join max_date
where
    generic_date_dim.date <= max_date.max_date and generic_date_dim.date >= max_date.min_date
