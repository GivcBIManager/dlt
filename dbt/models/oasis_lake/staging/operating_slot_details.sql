{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, operating_slot_code, operation_seq)',
    partition_by='branch_id'
) }}

-- oasis_lake.operating_slot_details -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.OPERATING_SLOT_DETAILS (transaction load).
--
-- Grain: branch_id, operating_slot_code, operation_seq (measured unique over 160,396 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 operating_slot_code,
 operation_seq,
 ios_main,
 operation_status,
 speciality_service_dept,
 operation_staff_id,
 operation_type,
 scrub_nurse_id,
 assistant_nurse_id,
 circulating_nurse_id,
 operation_started,
 operation_end,
 surgeon_assistant,
 anesthesia_type,
 anesthetist_staff_id,
 anesthetist_register,
 anesthetist_resident,
 created_by_user,
 creation_date,
 amend_by_user,
 amend_last_date,
 patient_problem_id,
 notes,
 operating_report_id,
 reported_ios_flag,
 surgeon_assistant_2,
 surgeon_assistant_3,
 extra_description,
 anesthesia_end_flag,
 diagnosis_recorded_flag,
 report_created_flag,
 xy_staff_id,
 scrub_nurse2_id,
 midwife_nurse_id,
 head_nurse_id,
 hospital_id,
 visiting_doctor,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('operating_slot_details') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
