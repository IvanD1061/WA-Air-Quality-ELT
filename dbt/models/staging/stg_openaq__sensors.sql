-- One row per sensor. In OpenAQ a sensor is one instrument measuring one
-- pollutant at one location, and each location record carries its sensors as
-- a nested array. LATERAL FLATTEN turns that array into rows: a location with
-- five sensors becomes five rows here.

with source as (

    select * from {{ source('openaq', 'locations') }}

),

flattened as (

    select
        sensor.value:id::number as sensor_id,
        source.raw_json:data:id::number as location_id,
        sensor.value:name::string as sensor_name,
        sensor.value:parameter:id::number as parameter_id,
        lower(sensor.value:parameter:name::string) as parameter_name,
        sensor.value:parameter:units::string as parameter_units,
        sensor.value:parameter:displayName::string as parameter_display_name,
        source.partition_date as snapshot_date,
        source.loaded_at
    from source,
        lateral flatten(input => source.raw_json:data:sensors) as sensor

),

latest as (

    select *
    from flattened
    where sensor_id is not null
    qualify row_number() over (
        partition by sensor_id
        order by snapshot_date desc, loaded_at desc
    ) = 1

)

select * from latest
