{#
    Decides which schema each model is built in.

    dbt's default glues the target schema and the custom schema together, so a
    model configured with `schema: marts` would land in ANALYTICS_MARTS in
    production. This override gives production the clean names (STAGING, MARTS)
    and keeps every other target in its own prefixed sandbox (DEV_STAGING,
    DEV_MARTS, CI_STAGING, ...), so working on a model can never overwrite the
    tables the dashboard reads.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim | upper }}
    {%- else -%}
        {{ target.schema | trim }}_{{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
