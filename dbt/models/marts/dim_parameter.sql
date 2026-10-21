-- What was measured. One row per pollutant-and-unit combination.
--
-- OpenAQ gives "ozone in ppm" and "ozone in µg/m³" different ids, so the id
-- already pins down the unit and values under one parameter_key are always
-- comparable with each other.

with parameters as (

    select
        parameter_id,
        max(parameter_name) as parameter_name,
        max(parameter_units) as units,
        max(parameter_display_name) as display_name
    from {{ ref('stg_openaq__sensors') }}
    where parameter_id is not null
    group by parameter_id

),

limits as (

    select * from {{ ref('parameter_limits') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['parameters.parameter_id']) }} as parameter_key,
    parameters.parameter_id,
    parameters.parameter_name,
    coalesce(parameters.display_name, upper(parameters.parameter_name)) as display_name,
    parameters.units,
    limits.min_plausible_value,
    limits.max_plausible_value,
    limits.parameter_id is not null as has_range_check
from parameters
left join limits
    on parameters.parameter_id = limits.parameter_id
