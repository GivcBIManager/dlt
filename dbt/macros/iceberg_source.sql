{#
    iceberg_source(table_name)

    Emits the ClickHouse `icebergLocal(...)` table function for a table in the
    Oasis Iceberg lake, AND registers that table as a dbt source so the model
    shows up with real upstream edges in `dbt docs` lineage.

    The `source()` call below is what does the registration: dbt captures
    source()/ref() calls made inside macros while parsing the calling model, so
    a model written as

        from {{ iceberg_source('appointments') }}

    gets an edge from `oasis_lake.appointments` even though the compiled SQL is
    a table function rather than a relation reference. The return value of
    source() is deliberately discarded -- ClickHouse cannot read the lake via a
    plain relation name, only via icebergLocal().

    The lake root comes from the `iceberg_root` var in dbt_project.yml. It is a
    path on the CLICKHOUSE HOST's filesystem, not this machine's.
#}
{% macro iceberg_source(table_name) %}
    {%- set _ = source('oasis_lake', table_name) -%}
    {%- set root = var('iceberg_root') -%}
    icebergLocal('{{ root }}{{ table_name }}')
{%- endmacro %}
