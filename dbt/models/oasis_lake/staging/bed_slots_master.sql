{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, bed_location)',
    partition_by='branch_id'
) }}

-- oasis_lake.bed_slots_master -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.BED_SLOTS_MASTER (master load).
--
-- Grain: branch_id, bed_location (measured unique over 4,776 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 work_entity,
 room_no,
 bed_location,
 slot_status,
 desirability_level,
 amend_by_user,
 amend_last_date,
 location,
 tel_ext,
 private_flag,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('bed_slots_master') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
