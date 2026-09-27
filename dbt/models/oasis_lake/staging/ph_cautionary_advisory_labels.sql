{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, cautionary_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.ph_cautionary_advisory_labels -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PH_CAUTIONARY_ADVISORY_LABELS (master load).
--
-- Grain: branch_id, cautionary_id (measured unique over 48 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 cautionary_id,
 description,
 description_a,
 text,
 amend_by_user,
 amend_last_date,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('ph_cautionary_advisory_labels') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
