{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id,patient_id )',
    partition_by='branch_id'
) }}
SELECT 
p.branch_id
,p.patient_id
,mrn
,p.account_no
,p.pat_name_1
,initCap(trim(ifNull(p.pat_name_1,'') ||' ' ||ifNull(p.pat_name_2,'')||' ' ||ifnull(p.pat_name_3,'') ||' ' ||ifNull(p.pat_name_family,''))) patient_name
,p.sex gender
,p.marital_code
,p.occupation_code
,p.nationality_code
,p.phone_1
,p.phone_2
,p.phone_3
,p.mobile_no
,p.date_of_birth::Nullable(Date32)  date_of_birth
,p.date_registered
,p.amend_last_date
,p.recorded_updated_at recorded_updated_at
FROM {{ iceberg_source('patient_master_data') }} p
left join (select branch_id, patient_id,argMax(user_file_id,volume_id) mrn
from {{ iceberg_source('patient_file_master') }}
group by all) f on p.branch_id = f.branch_id::Decimal and p.patient_id = f.patient_id::Decimal
{% if is_incremental() %}
  {%- set wm = run_query("select max(recorded_updated_at) from " ~ this).columns[0][0] -%}
  WHERE p.recorded_updated_at > toDateTime64('{{ wm }}', 6)
{% endif %}