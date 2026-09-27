{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, work_entity)',
    partition_by='branch_id'
) }}

-- oasis_lake.work_entities_data -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.WORK_ENTITIES_DATA (master load).
--
-- Grain: branch_id, work_entity (measured unique over 2,989 lake rows)
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
 description,
 entity_type,
 default_pharmacy,
 max_beds_in_ward,
 clinic_service_dept,
 amend_by_user,
 amend_last_date,
 enforce_eligibility,
 establishment_flag,
 part_of_work_entity,
 staff_roster_flag,
 staff_assignment_flag,
 ext_stock_link,
 gl_section_code,
 paypoint_flag,
 order_services_flag,
 deliver_services_flag,
 restrict_transfer,
 location,
 reserve_bed_after_transfer,
 short_name,
 transfer_medical_files,
 temporary_patient_transfer,
 unit_dose_flag,
 unit_dose_due_time,
 dispensed_days,
 sex,
 from_age,
 to_age,
 private_flag,
 renewal_day,
 opd_trans_with_refund,
 call_sequence,
 call_sequence_prefix,
 apply_unit_dose_per,
 care_level,
 vip_flag,
 sub_service_dept,
 app_credit_walkin_serial,
 hospital_id,
 entity_start_time,
 entity_end_time,
 receive_email_post_number,
 finger_print,
 speciality,
 sub_spec_id,
 virtual_clinic,
 facility_id,
 trx_sent,
 non_invasive,
 online_book_cons_type,
 send_sms,
 deficiency_execlude,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('work_entities_data') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
