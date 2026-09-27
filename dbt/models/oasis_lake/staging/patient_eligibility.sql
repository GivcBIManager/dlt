{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, patient_eligibility_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.patient_eligibility -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PATIENT_ELIGIBILITY (transaction load).
--
-- Grain: branch_id, patient_eligibility_id (measured unique over 10,246,962 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 patient_id,
 episode_no,
 sequence,
 eligibility_number,
 eligibility_service_dept,
 eligibility_start,
 eligibility_end,
 eligibility_status,
 consultant_id,
 master_order_no,
 attendance_type,
 admission_no,
 admit_flag,
 discharge_flag,
 responsibility,
 work_entity,
 amend_by_user,
 amend_last_date,
 eligibility_work_entity,
 patient_eligibility_id,
 original_eligibility_id,
 referral_code,
 hl7_sent,
 hospital_id,
 virtual,
 after_discharge_episode_no,
 trx_sent,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('patient_eligibility') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
