{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, contract_no, rule_ios, term_id, rule_seq, applies_start, applies_end, amend_by_user, amend_last_date, price_total, units_total, rule_control_status, hospital_id, created_by_user, creation_date, recorded_updated_at)',
    partition_by='branch_id'
) }}

-- oasis_lake.contract_rules -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.CONTRACT_RULES (master load).
--
-- Grain: branch_id, contract_no, rule_ios, term_id, rule_seq, applies_start, applies_end, amend_by_user, amend_last_date, price_total, units_total, rule_control_status, hospital_id, created_by_user, creation_date, recorded_updated_at (widened -- see below)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- The sources-file unique_key is VIOLATED in the lake, and so is every wider
-- natural key (97 distinct rows still collide). The key is therefore the whole
-- row bar the binary merge_hash, which is measured lossless: it collapses only
-- the 63 byte-identical duplicates. Flag to the data owners.
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 contract_no,
 rule_ios,
 term_id,
 ifNull(applies_start, toDateTime64('1970-01-01 00:00:00', 6)) as applies_start,
 ifNull(applies_end, toDateTime64('1970-01-01 00:00:00', 6)) as applies_end,
 ifNull(amend_by_user, 0) as amend_by_user,
 ifNull(amend_last_date, toDateTime64('1970-01-01 00:00:00', 6)) as amend_last_date,
 rule_seq,
 ifNull(price_total, 0) as price_total,
 ifNull(units_total, 0) as units_total,
 ifNull(rule_control_status, '') as rule_control_status,
 ifNull(hospital_id, 0) as hospital_id,
 ifNull(created_by_user, 0) as created_by_user,
 ifNull(creation_date, toDateTime64('1970-01-01 00:00:00', 6)) as creation_date,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('contract_rules') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
