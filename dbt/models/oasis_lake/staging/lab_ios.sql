{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, ios)',
    partition_by='branch_id'
) }}

-- oasis_lake.lab_ios -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.LAB_IOS (master load).
--
-- Grain: branch_id, ios (measured unique over 15,951 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 ios,
 lab_dept_no,
 specimen_code,
 container_id,
 required_volume,
 weight,
 result_type,
 dual_code_type,
 culture_code_type,
 differential_code_type,
 ios_minutes_required,
 no_tests_per_day,
 reduandant_request_hours,
 amend_by_user,
 amend_last_date,
 short_name,
 second_approval_flag,
 unit_of_measure,
 line_text,
 lab_id_format_no,
 percentage_or_absolute_flag,
 max_change_value,
 time_period,
 formula_text,
 lab_sequence,
 subgroup_id,
 seq_inside_subgroup,
 chart_print_flag,
 chart_description,
 fill_char,
 ending_by_colon,
 font_used,
 font_size,
 extra_labels_needed,
 highlight_flag,
 need_signature,
 result_is_changeable,
 effect_on_donor_status,
 include_in_bb_inquiry,
 narcotic_flag,
 addicted_period,
 addicted_times,
 staff_auth_flag,
 range_result,
 drug_monitoring,
 df_flag,
 line_text_allignment,
 external_test,
 hide_value,
 stat_time,
 routine_time,
 third_approval_flag,
 virology,
 collection_verification,
 aliniq_ios,
 show_pending,
 hospital_id,
 multi_collc,
 multi_release,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 default_dual_code,
 merge_hash
from {{ iceberg_source('lab_ios') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
