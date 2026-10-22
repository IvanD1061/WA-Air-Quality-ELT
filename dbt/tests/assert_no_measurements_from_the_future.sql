-- A reading stamped later than "now" means a time zone was applied the wrong
-- way round somewhere. Two hours of slack covers clock differences.
-- A singular test passes when it returns no rows.

select
    measurement_key,
    sensor_id,
    measured_at_utc
from {{ ref('fct_measurements_hourly') }}
where measured_at_utc > dateadd(hour, 2, convert_timezone('UTC', current_timestamp())::timestamp_ntz)
