{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, staff_type)',
    partition_by='branch_id'
) }}

-- oasis_lake.staff_types_data -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.STAFF_TYPES_DATA (master load).
--
-- Grain: branch_id, staff_type (measured unique over 2,258 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 staff_type,
 staff_type_description,
 admit_patients_flag,
 order_drugs_flag,
 discharge_patients_flag,
 run_clinic,
 head_team,
 amend_by_user,
 amend_last_date,
 run_cashier,
 book_addition,
 freeze_slots,
 transfer_medical_files,
 double_booking,
 security_officer,
 consultant,
 can_book_walkin,
 book_appt,
 cancel_appt,
 link_shift_to_machine,
 dna_appt,
 max_sickleave_days,
 freeze_slots_1,
 freeze_slots_2,
 freeze_slots_3,
 freeze_slots_4,
 freeze_slots_5,
 freeze_slots_6,
 staff_role,
 freeze_slots_7,
 hospital_id,
 or_confirmation,
 pt_freeze_slots,
 over_totals,
 or_freeze_slots,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('staff_types_data') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
