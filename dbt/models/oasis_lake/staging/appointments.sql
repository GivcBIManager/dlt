{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, appointment_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.appointments -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.APPOINTMENTS (transaction load).
--
-- Grain: branch_id, appointment_id (measured unique over 425,387,542 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 work_entity,
 julian_date,
 start_date,
 end_date,
 appt_length,
 patient_id,
 episode_no,
 time_arrived,
 time_seen,
 time_complete,
 consultant,
 new_followup_flag,
 outcome_code,
 referred_to_code,
 referred_from_code,
 long_id,
 amend_by_user,
 amend_last_date,
 walkin_flag,
 session_code,
 break_code,
 wait_list_id,
 appointment_id,
 machines_no,
 order_line,
 time_arrived_2,
 time_last_seen,
 total_minutes,
 delay_reason,
 addition_date,
 walkin_serial,
 workstation_name,
 request_file_flag,
 receive_file_flag,
 hl7_sent,
 created_by_user,
 creation_date,
 on_line_booking,
 treated_by,
 building_no,
 serial_no,
 slot_ser,
 web_book_locked_by,
 web_book_date,
 web_book_status,
 populated_by,
 populate_date,
 call_seq,
 displyed_after_julian,
 displayed_after_time,
 arrived_by_user,
 hospital_id,
 dna_reason,
 dna_booked_by,
 time_triaged,
 virtual,
 virtual_status,
 consultation_type,
 booked_from,
 virtual_cons_url,
 nr_opd_note,
 nr_note_seen,
 nr_chk_priority,
 visit_deficieny_status,
 referral_code,
 trx_sent,
 last_application_name,
 qms_sent,
 trace_text,
 status,
 call_seq_prefix,
 qirat_sent,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 filler_appointment_code,
 mc_sent,
 int_sent,
 merge_hash,
 mental_module_flag
from {{ iceberg_source('appointments') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
