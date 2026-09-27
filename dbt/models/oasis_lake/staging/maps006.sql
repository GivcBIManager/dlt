{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, user_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.maps006 -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.MAPS006 (master load).
--
-- Grain: branch_id, user_id (measured unique over 16,413 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 user_id,
 user_name,
 first_name,
 middle_name,
 family_name,
 department,
 job_id,
 password,
 psswd_life_months,
 psswd_expiry_date,
 language_id,
 staff_id,
 default_parts_entity,
 default_tools_entity,
 default_labour_entity,
 employee_flag,
 default_stock_entity,
 allow_ward_transfer,
 allow_bplate_modify,
 hegira_flag,
 manual_file_id,
 authorisation_contract,
 created_by_user,
 amend_by_user,
 amend_last_date,
 creation_date,
 pager_no,
 narrative,
 restrict_clinic_flag,
 is_admin,
 account_code,
 accept_falg,
 user_type,
 must_change_password,
 fax_no,
 phone_no,
 email_address,
 hospital_id,
 lexacom_username,
 lexacom_password,
 call_sequence,
 pass_change_date,
 automatic_user,
 lock_flag,
 login_schedule_restricted,
 active,
 manage_security,
 refill_sms_sequence,
 refill_sequence,
 initial_pass,
 pass_case,
 account_type,
 linked_consent_device_account,
 external_user_name,
 hire_type_code,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 bak_restrict_clinic_flag,
 merge_hash,
 view_conceal_patient,
 access_conceal_pat_emr
from {{ iceberg_source('maps006') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
