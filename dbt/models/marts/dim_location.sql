-- Where a measurement was taken. One row per monitoring location.
--
-- Provider and owner are folded in as plain columns rather than split into
-- their own tables. That is the "star" in star schema: dimensions are wide and
-- flat so a question like "average PM2.5 by provider" needs one join, not three.

with locations as (

    select * from {{ ref('stg_openaq__locations') }}

),

sensor_counts as (

    select
        location_id,
        count(*) as sensor_count
    from {{ ref('stg_openaq__sensors') }}
    group by location_id

)

select
    {{ dbt_utils.generate_surrogate_key(['locations.location_id']) }} as location_key,
    locations.location_id,
    coalesce(locations.location_name, 'Unnamed location ' || locations.location_id::string)
        as location_name,
    locations.locality,
    locations.country_code,
    locations.timezone,
    locations.latitude,
    locations.longitude,
    locations.provider_name,
    locations.owner_name,
    coalesce(locations.is_monitor, false) as is_reference_monitor,
    case
        when locations.is_monitor then 'Reference monitor'
        else 'Low-cost sensor'
    end as station_type,
    coalesce(locations.is_mobile, false) as is_mobile,
    coalesce(sensor_counts.sensor_count, 0) as sensor_count,
    locations.first_measured_at_utc,
    locations.last_measured_at_utc,
    locations.snapshot_date as last_seen_in_snapshot
from locations
left join sensor_counts
    on locations.location_id = sensor_counts.location_id
