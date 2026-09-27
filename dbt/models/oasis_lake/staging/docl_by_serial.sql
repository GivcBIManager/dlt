{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, product_code, bin_location, c_id, serial_no_1, adj_date, recorded_updated_at, cnt, qty_outstanding, qty_allocated, unit_price)',
    partition_by='branch_id'
) }}

-- oasis_lake.docl_by_serial -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.DOCL_BY_SERIAL (snapshot load).
--
-- Grain: branch_id, product_code, bin_location, c_id, serial_no_1, adj_date, recorded_updated_at, cnt, qty_outstanding, qty_allocated, unit_price (widened -- see below)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- The sources-file unique_key is VIOLATED in the lake: ~252k rows share it
-- within a single load and differ in their measures. No natural key exists, so
-- the key is widened to the full measure set, which is measured unique. That
-- means this model keeps every row and dedups nothing -- deliberately, because
-- the alternative silently destroys real rows. Flag to the data owners.
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 ifNull(product_code, '') as product_code,
 ifNull(c_id, 0) as c_id,
 ifNull(bin_location, '') as bin_location,
 ifNull(cnt, 0) as cnt,
 ifNull(qty_outstanding, 0) as qty_outstanding,
 ifNull(qty_allocated, 0) as qty_allocated,
 ifNull(serial_no_1, '') as serial_no_1,
 ifNull(adj_date, toDateTime64('1970-01-01 00:00:00', 6)) as adj_date,
 ifNull(unit_price, 0) as unit_price,
 hospital_id,
 ifNull(branch_id, 0) as branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 version,
 version_date
from {{ iceberg_source('docl_by_serial') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
