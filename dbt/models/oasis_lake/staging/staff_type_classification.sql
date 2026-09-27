{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, staff_type, type_desc)',
    partition_by='branch_id'
) }}

-- oasis_lake.staff_type_classification -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: punch.staff_type_classification (master load).
--
-- Grain: branch_id, staff_type, type_desc (widened -- see below)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- The sources-file unique_key (staff_type) is VIOLATED in the lake: one
-- staff_type carries two different type_desc values under the same load. The
-- key is widened with type_desc, which is measured unique over today's data.
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 staff_type,
 ifNull(type_desc, '') as type_desc,
 classificaton,
 need_moh,
 need_s_council,
 med_nonmed,
 categorynew,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('staff_type_classification') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
