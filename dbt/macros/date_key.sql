{#
    A date as an integer in the form YYYYMMDD, e.g. 2026-10-06 -> 20261006.

    Integer date keys are a data warehousing convention: they sort correctly,
    are readable at a glance, and join faster than strings.
#}

{% macro date_key(expression) -%}
    (year({{ expression }}) * 10000 + month({{ expression }}) * 100 + day({{ expression }}))
{%- endmacro %}
