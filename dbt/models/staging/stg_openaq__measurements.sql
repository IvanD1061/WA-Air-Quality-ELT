-- One row per sensor per hour.
--
-- The extractor wraps each API record in an envelope:
--   raw_json:sensor_id, raw_json:location_id   who the reading belongs to
--   raw_json:extracted_at                      when we pulled it
--   raw_json:data                              the API record, untouched
--
-- Two cleaning steps happen here and nowhere else:
--   1. Rows with no sensor or no timestamp are dropped. They cannot be placed
--      on a timeline, so they cannot be facts.
--   2. Duplicates are removed. The API can return the same hour twice, and a
--      re-extracted day can overlap its neighbour by an hour. The most
--      recently loaded copy wins.
-- Bad *values* (nulls, negatives, spikes) are kept and flagged downstream.

with source as (

    select * from {{ source('openaq', 'measurements') }}

),

parsed as (

    select
        raw_json:sensor_id::number as sensor_id,
        raw_json:location_id::number as location_id,
        raw_json:data:parameter:id::number as parameter_id,
        lower(raw_json:data:parameter:name::string) as parameter_name,
        raw_json:data:parameter:units::string as parameter_units,

        {{ iso_timestamp('raw_json:data:period:datetimeFrom:utc') }} as measured_at_utc,
        {{ iso_timestamp('raw_json:data:period:datetimeFrom:local') }} as measured_at_local_raw,

        raw_json:data:value::float as value,
        raw_json:data:summary:min::float as value_min,
        raw_json:data:summary:max::float as value_max,
        raw_json:data:summary:median::float as value_median,
        raw_json:data:summary:sd::float as value_sd,

        raw_json:data:coverage:expectedCount::number as expected_count,
        raw_json:data:coverage:observedCount::number as observed_count,
        raw_json:data:coverage:percentComplete::float as percent_complete,
        coalesce(raw_json:data:flagInfo:hasFlags::boolean, false) as has_provider_flags,

        {{ iso_timestamp('raw_json:extracted_at') }} as extracted_at,
        partition_date,
        source_file,
        source_row_number,
        loaded_at
    from source

),

deduplicated as (

    select *
    from parsed
    where
        sensor_id is not null
        and measured_at_utc is not null
    qualify row_number() over (
        partition by sensor_id, measured_at_utc
        order by loaded_at desc, source_row_number desc
    ) = 1

)

select
    sensor_id,
    location_id,
    parameter_id,
    parameter_name,
    parameter_units,
    measured_at_utc,
    -- The local time should always be present when the UTC time is. If it is
    -- not, fall back to UTC so the row still lands on a calendar day.
    coalesce(measured_at_local_raw, measured_at_utc) as measured_at_local,
    value,
    value_min,
    value_max,
    value_median,
    value_sd,
    expected_count,
    observed_count,
    percent_complete,
    has_provider_flags,
    extracted_at,
    partition_date,
    loaded_at
from deduplicated
