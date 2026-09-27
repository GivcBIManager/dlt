{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, contract_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.contracts -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.CONTRACTS (master load).
--
-- Grain: branch_id, contract_no (measured unique over 681,392 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 contract_no,
 policy_code,
 account_no,
 min_age,
 max_age,
 description,
 id_req,
 bill_to,
 applies_start,
 applies_end,
 amend_last_user,
 amend_last_date,
 max_opd_wait_list_days,
 max_treatment_wait_list_days,
 default_flag,
 contract_package_flag,
 authorisation_req,
 class_id,
 hospital_id,
 nationality_code,
 referral,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash,
 reviewed_flag,
 reviewed_by_user,
 reviewed_date
from {{ iceberg_source('contracts') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
