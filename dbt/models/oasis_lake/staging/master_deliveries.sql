{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, master_delivery_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.master_deliveries -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.MASTER_DELIVERIES (transaction load).
--
-- Grain: branch_id, master_delivery_no (measured unique over 25,714,810 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 master_delivery_no,
 master_order_no,
 delivery_date,
 amend_by_user,
 amend_last_date,
 delivery_work_entity,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('master_deliveries') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
