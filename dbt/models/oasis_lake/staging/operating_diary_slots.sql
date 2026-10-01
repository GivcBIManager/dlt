{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, operating_slot_code)',
    partition_by='branch_id'
) }}

-- oasis_lake.operating_diary_slots -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.OPERATING_DIARY_SLOTS (transaction load).
--
-- Grain: branch_id, operating_slot_code (measured unique over 3,390,336 lake rows)
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
 operating_session_code,
 work_entity,
 staff_id,
 patient_id,
 operating_start,
 operating_end,
 slot_time,
 cancel_flag,
 cancel_code,
 episode_no,
 time_arrived_to_or,
 anesthesia_started,
 transfered_to_recovery_room_at,
 transfered_to_ward_at,
 anesthesia_ended,
 cancel_reschedule_reason,
 break_code,
 created_by_user,
 creation_date,
 amend_by_user,
 amend_last_date,
 appointment_type,
 operation_started,
 operation_end,
 porter_sent_at,
 delay_reason,
 anesthesia_incident_report,
 cancelled_by,
 time_arrived_to_hall,
 preparation_start,
 preparation_end,
 related_to_opr_slot_code,
 hospital_id,
 confirmed_staff_id,
 confirmed_date,
 entity_type,
 or_request_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('operating_diary_slots') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
