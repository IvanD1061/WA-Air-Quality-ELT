-- When. One row per calendar day, generated rather than loaded.
--
-- A date dimension looks redundant next to a timestamp column, but it lets
-- reports group by month name or weekend flag without repeating date math in
-- every query, and it has a row for days with no data at all, which is how
-- gaps show up in a chart instead of silently vanishing.

with days as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('dim_date_start') ~ "' as date)",
        end_date="dateadd(year, 1, date_trunc('year', current_date))"
    ) }}

)

select
    {{ date_key('date_day') }} as date_key,
    date_day::date as date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    monthname(date_day) as month_name,
    date_trunc('month', date_day)::date as month_start,
    day(date_day) as day_of_month,
    dayofweekiso(date_day) as day_of_week,
    dayname(date_day) as day_name,
    dayofweekiso(date_day) in (6, 7) as is_weekend
from days
