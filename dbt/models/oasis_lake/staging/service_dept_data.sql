{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, service_dept)',
    partition_by='branch_id'
) }}

-- oasis_lake.service_dept_data -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.SERVICE_DEPT_DATA (master load).
--
-- Grain: branch_id, service_dept (measured unique over 458 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 service_dept,
 description,
 eligibility_ios,
 dept_type,
 dummy_service_2,
 amend_by_user,
 amend_last_date,
 max_opd_wait_list_days,
 max_treatment_wait_list_days,
 dept_short_code,
 default_eligibility_type,
 default_staff_id,
 default_work_entity,
 default_printer_id,
 active_period_after_schedule,
 period_type,
 default_work_entity_priv,
 default_staff_id_priv,
 default_eligibility_type_priv,
 receipt_report_name,
 print_patient_request_copy,
 print_dept_request_copy,
 dept_it_manager_id,
 dept_head_id,
 portal_login_start_time,
 portal_login_end_time,
 portal_disabled_days,
 dept_secretary_id,
 booking_display_type,
 booking_days_displayed_after,
 booking_days_displayed,
 max_booking_month,
 appt_msg_resource_key,
 call_sequence,
 image_name,
 hospital_id,
 portal_order,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('service_dept_data') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
