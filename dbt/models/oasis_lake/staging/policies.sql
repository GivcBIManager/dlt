{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, policy_code)',
    partition_by='branch_id'
) }}

-- oasis_lake.policies -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.POLICIES (master load).
--
-- Grain: branch_id, policy_code (measured unique over 208,746 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 policy_code,
 purchaser_code,
 account_no,
 description,
 amend_by_user,
 amend_last_date,
 active_flag,
 eligibility_no_days,
 eligibility_no_visits,
 hospital_id,
 nphies_license,
 referral,
 inhosp_accept_virtual_followup,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 etimad,
 merge_hash,
 auto_contract_master_id
from {{ iceberg_source('policies') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
