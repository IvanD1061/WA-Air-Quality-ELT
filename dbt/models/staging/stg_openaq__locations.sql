-- One row per monitoring location, taken from the most recent snapshot that
-- contains it. Staging only renames, casts and de-duplicates; it never
-- applies business rules.

with source as (

    select * from {{ source('openaq', 'locations') }}

),

parsed as (

    select
        raw_json:data:id::number as location_id,
        -- Names arrive null, empty or padded with spaces. Make them all null.
        nullif(trim(raw_json:data:name::string), '') as location_name,
        nullif(trim(raw_json:data:locality::string), '') as locality,
        raw_json:data:timezone::string as timezone,
        raw_json:data:country:code::string as country_code,
        raw_json:data:provider:id::number as provider_id,
        raw_json:data:provider:name::string as provider_name,
        raw_json:data:owner:id::number as owner_id,
        raw_json:data:owner:name::string as owner_name,
        raw_json:data:isMonitor::boolean as is_monitor,
        raw_json:data:isMobile::boolean as is_mobile,
        raw_json:data:coordinates:latitude::float as latitude,
        raw_json:data:coordinates:longitude::float as longitude,
        {{ iso_timestamp('raw_json:data:datetimeFirst:utc') }} as first_measured_at_utc,
        {{ iso_timestamp('raw_json:data:datetimeLast:utc') }} as last_measured_at_utc,
        partition_date as snapshot_date,
        loaded_at
    from source

),

latest as (

    select *
    from parsed
    where location_id is not null
    qualify row_number() over (
        partition by location_id
        order by snapshot_date desc, loaded_at desc
    ) = 1

)

select * from latest
