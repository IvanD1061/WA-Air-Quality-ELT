-- Daily summary. Grain: one row per location per pollutant per local day.
--
-- This is an aggregate fact built from the hourly fact. The dashboard reads
-- this table, so its charts scan a few thousand rows instead of every hour.
--
-- Only valid hours are averaged. For PM2.5, days with enough valid hours also
-- get a US EPA Air Quality Index value and category. This is an AQI-style
-- figure computed from the daily mean, useful for comparing days and places.
-- It is not the official AirNow number.

with valid_hours as (

    select * from {{ ref('fct_measurements_hourly') }}
    where is_valid

),

location_hours as (

    -- A location can have two sensors for the same pollutant. Average them
    -- per hour first so that hour counts once, not twice.
    select
        location_key,
        parameter_key,
        date_key,
        measured_at_utc,
        avg(value) as value
    from valid_hours
    group by location_key, parameter_key, date_key, measured_at_utc

),

daily as (

    select
        location_key,
        parameter_key,
        date_key,
        count(*) as valid_hours,
        avg(value) as avg_value,
        min(value) as min_value,
        max(value) as max_value
    from location_hours
    group by location_key, parameter_key, date_key

),

parameters as (

    select * from {{ ref('dim_parameter') }}

),

breakpoints as (

    select
        *,
        lead(concentration_low) over (order by sort_order) as next_concentration_low
    from {{ ref('pm25_aqi_breakpoints') }}

),

prepared as (

    select
        daily.*,
        daily.valid_hours >= {{ var('min_valid_hours_per_day') }} as is_complete_day,
        -- AQI is only defined for PM2.5 here, and only for complete days. EPA
        -- truncates the concentration to one decimal before the lookup.
        -- Instrument noise can make a clean day slightly negative, so it is
        -- floored at zero. The cast to NUMBER makes the comparison with the
        -- breakpoints exact: as a FLOAT, 9.1 is really 9.0999999..., which
        -- would fall into the wrong band.
        case
            when
                parameters.parameter_id = {{ var('pm25_parameter_id') }}
                and daily.valid_hours >= {{ var('min_valid_hours_per_day') }}
                then trunc(greatest(daily.avg_value, 0)::number(18, 6), 1)
        end as aqi_concentration
    from daily
    inner join parameters
        on daily.parameter_key = parameters.parameter_key

)

select
    {{ dbt_utils.generate_surrogate_key(['prepared.location_key', 'prepared.parameter_key', 'prepared.date_key']) }}
        as daily_key,
    prepared.location_key,
    prepared.parameter_key,
    prepared.date_key,
    prepared.valid_hours,
    -- A local day has 25 hours when the clocks go back, hence the cap.
    least(100.0, round(100.0 * prepared.valid_hours / 24, 1)) as completeness_pct,
    prepared.is_complete_day,
    prepared.avg_value,
    prepared.min_value,
    prepared.max_value,
    -- Linear interpolation inside the matching band, capped at the top of the scale.
    least(
        500,
        round(
            breakpoints.aqi_low
            + (prepared.aqi_concentration - breakpoints.concentration_low)
            * (breakpoints.aqi_high - breakpoints.aqi_low)
            / (breakpoints.concentration_high - breakpoints.concentration_low)
        )
    ) as aqi_value,
    breakpoints.aqi_category
from prepared
left join breakpoints
    on
        prepared.aqi_concentration >= breakpoints.concentration_low
        and (
            prepared.aqi_concentration < breakpoints.next_concentration_low
            or breakpoints.next_concentration_low is null
        )
