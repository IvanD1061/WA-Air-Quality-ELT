{#
    Turns an ISO 8601 string from the API into a TIMESTAMP_NTZ.

    The API gives every moment twice: once in UTC ("2026-10-06T07:00:00Z") and
    once in the station's local time ("2026-10-06T00:00:00-07:00"). The first
    19 characters of either string are the wall-clock time, which is the part
    we want to keep, so the offset suffix is cut off rather than converted.

    try_cast returns NULL for a malformed string instead of failing the whole run.
#}

{% macro iso_timestamp(expression) -%}
    try_cast(left({{ expression }}::string, 19) as timestamp_ntz)
{%- endmacro %}
