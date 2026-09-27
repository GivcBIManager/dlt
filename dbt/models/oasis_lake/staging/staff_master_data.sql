{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, staff_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.staff_master_data -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.STAFF_MASTER_DATA (master load).
--
-- Grain: branch_id, staff_id (measured unique over 23,469 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 staff_id,
 staff_type,
 staff_name_1,
 staff_name_2,
 staff_name_3,
 staff_name_family,
 title,
 religion_code,
 date_of_birth,
 sex,
 nationality_code,
 marital_code,
 occupation_code,
 sdx_staff_name_1,
 sdx_staff_name_2,
 sdx_staff_name_3,
 sdx_name_family,
 decade_of_birth,
 notes_long_id,
 image_id,
 employee_dependant_flag,
 dependant_of_staff,
 amend_by_user,
 amend_last_date,
 pay_award_id,
 date_in_kingdom,
 ni_number,
 bank_name,
 bank_account_number,
 bank_sort_code,
 sponsor_id,
 car_driver_flag,
 car_owner_flag,
 place_born_code,
 start_of_service,
 paypoint_work_entity,
 maiden_name,
 staff_name_1_b,
 staff_name_2_b,
 staff_name_3_b,
 staff_name_familyb,
 bank_address_1,
 bank_address_2,
 bank_address_3,
 bank_address_4,
 bank_telephone,
 bank_fax,
 bank_postcode,
 security_required,
 old_staff_id,
 payment_method,
 final_payment_status,
 final_payment_doc_no,
 email_subscriber,
 email_address,
 doctor_code,
 mobile_no,
 banned_from_traveling,
 send_email_flag,
 portal_extra_notes_e,
 portal_extra_notes_a,
 portal_booking_notes_e,
 portal_booking_notes_a,
 hospital_id,
 final_payment_doc_date,
 full_professional_title_e,
 full_professional_title_a,
 about_doctor_e,
 about_doctor_a,
 trx_sent,
 prv_gosi_period,
 new_gosi_flag,
 mmm_sent,
 portal_order,
 mob_info_log,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash,
 provided_services_en,
 provided_services_ar,
 nationality_type,
 nabd_id
from {{ iceberg_source('staff_master_data') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
