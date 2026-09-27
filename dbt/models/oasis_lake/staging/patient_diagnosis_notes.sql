{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, patient_diagnosis_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.patient_diagnosis_notes -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PATIENT_DIAGNOSIS_NOTES (transaction load).
--
-- Grain: branch_id, patient_diagnosis_id (measured unique over 11,216,729 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 patient_diagnosis_id,
 patient_id,
 note_type,
 status,
 staff_id,
 patient_problem_id,
 date_recorded,
 diag_code,
 diag_orientation,
 diagnosis_division,
 episode_no,
 encounter_type,
 encounter_id,
 admit_flag,
 discharge_flag,
 importance_code,
 master_order_no,
 at_risk_flag,
 diagnosis_note,
 active_until_date,
 severity,
 final_flag,
 chronic_flag,
 alert_flag,
 amend_last_date,
 amend_by_user,
 new_diag_code_flag,
 diag_type,
 ios_main,
 diagnosis_outcome_code,
 error_code,
 error_amended_by,
 error_amended_date,
 created_by_user,
 creation_date,
 hospital_id,
 check_list_answers_master_id,
 answer_session_id,
 coded,
 edit_draft,
 prfx,
 code_finder,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash,
 edu_sync
from {{ iceberg_source('patient_diagnosis_notes') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
