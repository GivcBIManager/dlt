{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, id)',
    partition_by='branch_id'
) }}

-- oasis_lake.api_pre_approval_res -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.API_PRE_APPROVAL_RES (transaction load).
--
-- Grain: branch_id, id (measured unique over 4,899,307 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 id,
 transaction_id,
 preauthorization_id,
 provider_name,
 insurance_company,
 patient_file_no,
 department,
 provider_fax_no,
 date_of_admission,
 member_name,
 id_card_no,
 age,
 gender,
 policy_holder,
 membership_no,
 member_class,
 expiry_date,
 preauthorisation_status,
 app_validity,
 room_type,
 comments,
 insurance_officer,
 add_comments,
 date_time,
 chronic_ind,
 ext_date,
 ext_ind,
 status,
 api_trans_id,
 created_by_user,
 creation_date,
 amend_by_user,
 amend_last_date,
 hospital_id,
 auth_status,
 error_id,
 error_message,
 req_api_trans_id,
 response_id,
 response_system,
 pre_auth_ref,
 referred_provider,
 referral_period,
 pre_auth_ref_start,
 pre_auth_ref_end,
 trasfer_to_auth_no,
 trasfer_to_provider,
 trasfer_to_provider_lic,
 trasfer_to_auth_start,
 trasfer_to_auth_end,
 authorisation_type,
 advanced_auth_reason,
 supporting_info,
 payer_license,
 patient_identifier_type,
 patient_identifier_value,
 response_identifier,
 treatment_type,
 treatment_sub_type,
 priorauth_response,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('api_pre_approval_res') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
