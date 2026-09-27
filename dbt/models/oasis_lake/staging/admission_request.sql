{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, admission_request_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.admission_request -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.ADMISSION_REQUEST (transaction load).
--
-- Grain: branch_id, admission_request_id (measured unique over 41,720 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 admission_request_id,
 patient_id,
 consultant_id,
 bed_class,
 planned_admit_date,
 status,
 est_discharge_date,
 admission_no,
 work_entity,
 team_id,
 service_dept,
 admission_diagnosis,
 diagnosis_code,
 operation_procedure,
 operation_type,
 anesthesia_type,
 date_to_be_operated,
 fasting_required,
 other_requirments,
 units_of_blood,
 blood_group,
 reason_for_admit,
 amend_by_user,
 amend_last_date,
 accept_visit_flag,
 admission_department,
 eligibility_type,
 urgency,
 attendance_type,
 room_specialty,
 ios_main,
 other_admission_reason,
 admission_reason_note,
 date_reviewed,
 created_by_user,
 creation_date,
 admission_type,
 answer_session_id,
 hospital_id,
 or_request_id,
 episode_no,
 pre_admission_no,
 phsyc_addict_stage,
 fit_for_admission,
 session_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 int_sent,
 merge_hash
from {{ iceberg_source('admission_request') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
