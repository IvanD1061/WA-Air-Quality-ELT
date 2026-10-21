-- Every measurement, labelled with whether it can be trusted.
--
-- Nothing is deleted. A bad reading is still a fact about what the sensor
-- sent, and "how often does this sensor send junk" is a useful question.
-- Instead each row gets a quality_status, and downstream models choose to
-- average only the valid ones.
--
-- The checks run in order and the first one that matches wins.

with measurements as (

    select * from {{ ref('stg_openaq__measurements') }}

),

limits as (

    select * from {{ ref('parameter_limits') }}

),

flagged as (

    select
        measurements.*,
        case
            when measurements.value is null
                then 'missing_value'
            -- Monitors legitimately report small negatives (instrument noise
            -- around zero). A large negative is a "no data" code such as -999.
            when measurements.value < limits.min_plausible_value
                then 'below_plausible_range'
            when measurements.value > limits.max_plausible_value
                then 'above_plausible_range'
            when measurements.has_provider_flags
                then 'provider_flagged'
            when measurements.percent_complete < {{ var('min_coverage_pct') }}
                then 'low_coverage'
            else 'valid'
        end as quality_status
    from measurements
    -- Pollutants without a row in the seed simply skip the range checks.
    left join limits
        on measurements.parameter_id = limits.parameter_id

)

select
    *,
    quality_status = 'valid' as is_valid
from flagged
