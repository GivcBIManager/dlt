{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id,line_id )',
    partition_by='branch_id'
) }}
SELECT 
*
FROM {{ iceberg_source('docl') }} cd
{% if is_incremental() %}
  {%- set wm = run_query("select max(recorded_updated_at) from " ~ this).columns[0][0] -%}
  WHERE cd.recorded_updated_at > toDateTime64('{{ wm }}', 6)
{% endif %}