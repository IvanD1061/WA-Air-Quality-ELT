-- The fact table. Grain: one row per sensor per hour.
--
-- Every row answers "what did this sensor read during this hour", and points
-- at three dimensions: where (location), what (parameter), when (date).
-- sensor_id stays on the fact as a degenerate dimension: a sensor has no
-- attributes of its own beyond its location and its pollutant, and both of
-- those already have dimensions.
--
-- Incremental: each run only processes raw rows loaded since the last run and
-- merges them in on measurement_key. Re-loading a day therefore updates its
-- rows in place instead of duplicating them. `dbt build --full-refresh`
-- rebuilds the table from scratch.

{{
    config(
        materialized='incremental',
        incremental_strategy=var('incremental_strategy'),
        unique_key='measurement_key',
        on_schema_change='append_new_columns'
    )
}}

with measurements as (

    select * from {{ ref('int_measurements__validated') }}

    {% if is_incremental() %}
        where loaded_at > (select coalesce(max(loaded_at), '1900-01-01'::timestamp_ltz) from {{ this }})
    {% endif %}

)

select
    {{ dbt_utils.generate_surrogate_key(['sensor_id', 'measured_at_utc']) }} as measurement_key,

    -- Foreign keys
    {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['parameter_id']) }} as parameter_key,
    -- The date key follows the station's local calendar, because "Tuesday's
    -- air quality" means Tuesday where the station is, not Tuesday in UTC.
    {{ date_key('measured_at_local') }} as date_key,

    -- Degenerate dimension
    sensor_id,

    -- Time
    measured_at_utc,
    measured_at_local,
    hour(measured_at_local) as hour_of_day_local,

    -- Measures
    value,
    value_min,
    value_max,
    value_median,
    value_sd,
    observed_count,
    expected_count,
    percent_complete,

    -- Data quality
    quality_status,
    is_valid,

    -- Audit
    extracted_at,
    loaded_at
from measurements
