-- The daily table is derived from the hourly one, so the two must agree:
-- the valid hours counted in the daily table have to equal the distinct
-- valid location-pollutant-hours in the hourly table. If a join in the daily
-- model ever fans out or drops rows, this is the test that notices.

with hourly as (

    select count(distinct location_key || '|' || parameter_key || '|' || measured_at_utc::string) as hours
    from {{ ref('fct_measurements_hourly') }}
    where is_valid

),

daily as (

    select coalesce(sum(valid_hours), 0) as hours
    from {{ ref('fct_air_quality_daily') }}

)

select
    hourly.hours as hourly_valid_hours,
    daily.hours as daily_valid_hours
from hourly
cross join daily
where hourly.hours != daily.hours
