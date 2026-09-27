{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, ios_main)',
    partition_by='branch_id'
) }}

-- oasis_lake.xy_ios -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: XY.XY_IOS (master load).
--
-- Grain: branch_id, ios_main (measured unique over 6,751 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 ios_main,
 report_template_id,
 short_code,
 work_load,
 report_required,
 lmp_sensitive,
 consultant_request_only,
 sub_modality_id,
 schedule_group_id,
 schedule_warning_id,
 time_length,
 avg_cost,
 use_special_consumables,
 attendance_time_before_exam,
 modality_id,
 amend_by_user,
 amend_last_date,
 primary_icd_code,
 secondary_icd_code,
 exam_code,
 xy_check_list_id,
 insert_id,
 lab_id_format_no,
 xy_id_format_no,
 radiologist_order_verification,
 radiologist_presence_req,
 xy_check_list_file,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('xy_ios') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
