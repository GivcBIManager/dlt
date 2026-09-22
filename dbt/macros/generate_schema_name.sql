{#
    generate_schema_name -- where a model's relation is created.

    In dbt-clickhouse a "schema" IS a ClickHouse database. dbt's built-in rule
    is to CONCATENATE: a model configured with `+schema: fusion` against the
    `default` target would be created in `default_fusion`. This override takes
    the configured schema verbatim instead, so `+schema: fusion` means the
    `fusion` database and nothing else. (dbt-clickhouse creates the database if
    it does not exist.)

    A model with NO `+schema:` is untouched and still lands in the target's own
    schema -- `default`. That is load-bearing: dbt_project.yml deliberately sets
    no schema on the staging/intermediate/marts layers, because renaming those
    relations would break every BI query already pointed at default.<model>.
    Only the Fusion subtree opts in.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
