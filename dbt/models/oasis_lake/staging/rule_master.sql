{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, rule_ios)',
    partition_by='branch_id'
) }}

-- oasis_lake.rule_master -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.RULE_MASTER (master load).
--
-- Grain: branch_id, rule_ios (measured unique over 2,848 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 rule_ios,
 description,
 amend_by_user,
 amend_last_date,
 hospital_id,
 drug_dispensing_type,
 purchaser_code,
 nphies_benefit_category,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('rule_master') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
