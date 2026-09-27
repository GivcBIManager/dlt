{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, generic_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.generics -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.GENERICS (master load).
--
-- Grain: branch_id, generic_id (measured unique over 46,304 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 generic_id,
 generic_name,
 generic_name_sdx,
 amend_by_user,
 amend_last_date,
 generic_user_code,
 generic_category,
 generic_pharmacological_id,
 bnf_id,
 cds_code,
 description,
 tall_man_lettering,
 hospital_id,
 antibiotec,
 g_mrid,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('generics') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
