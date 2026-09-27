{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, purchaser_code)',
    partition_by='branch_id'
) }}

-- oasis_lake.purchasers -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PURCHASERS (master load).
--
-- Grain: branch_id, purchaser_code (measured unique over 2,056 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 purchaser_code,
 description,
 address_1,
 address_2,
 address_3,
 address_4,
 tel_no,
 fax_no,
 contact,
 amend_by_user,
 amend_last_date,
 add_post_code,
 country,
 region_county_state,
 town_city,
 account_code,
 account_type,
 account_c_id,
 id_type_code,
 default_printer_id,
 auto_print_reports,
 eligibility_no_days,
 eligibility_no_visits,
 price_date,
 purchaser_user_code,
 ph_unit_dose_per,
 special_discoiunt,
 email_address,
 hospital_id,
 cchi_no,
 claim_config_id,
 activity_indicator,
 nphies_license,
 provider_id,
 non_mapped_service_code,
 is_tpa,
 referral,
 facility_id,
 send_medication_as_pharmacy,
 shadow_billing_code,
 marge_claim_attachment,
 send_followup_in_claim,
 enable_prescription,
 automation_flg,
 auto_contract_master_id,
 pkg_limit_excl_total,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('purchasers') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
