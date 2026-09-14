{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id,code)',
    partition_by='branch_id'
) }}
SELECT 
cd.branch_id branch_id
,code 
,user_code
,code_type
,description
,coalesce(m.moh_code, ar.moh_code) moh_code
,prog_code
,cd.recorded_updated_at recorded_updated_at
FROM {{ iceberg_source('codes_data') }} cd
left join  {{ iceberg_source('hnh_disacharge_mode_mapping') }} m on cd.description = m.reason 
left join  {{ iceberg_source('hnh_admission_reason_mapping') }} ar on cd.description = ar.reason 
{% if is_incremental() %}
  {%- set wm = run_query("select max(recorded_updated_at) from " ~ this).columns[0][0] -%}
  WHERE recorded_updated_at > toDateTime64('{{ wm }}', 6) and code is not null
{% endif %}