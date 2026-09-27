{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, er_visit_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.patient_emergency_visit -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PATIENT_EMERGENCY_VISIT (transaction load).
--
-- Grain: branch_id, er_visit_id (measured unique over 606,113 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 er_visit_id,
 institution_from,
 emergency_event_id,
 session_code,
 julian_date,
 patient_id,
 episode_no,
 er_reg_flag,
 priority,
 time_arrived,
 time_treatment_started,
 time_complete,
 referred_type,
 police_flag,
 police_ref,
 police_officer_no,
 police_custody_flag,
 outcome_code,
 institution_to,
 er_status,
 work_entity,
 time_triaged,
 amend_by_user,
 amend_last_date,
 companion,
 place_of_reporting,
 reporter_name,
 reporting_time,
 alternative_er_visit_id,
 request_file_flag,
 receive_file_flag,
 transfer_letter_doc_no,
 transfer_letter_doc_date,
 transfer_letter_subject_id,
 hl7_sent,
 treated_by,
 er_serial_no,
 er_serial_date,
 hospital_id,
 visit_deficieny_status,
 trx_sent,
 backdated_flag,
 delay_notification_sent,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 int_sent,
 merge_hash,
 physical_discharge_date,
 discharge_nurse_user
from {{ iceberg_source('patient_emergency_visit') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
