{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, account_code, product_code, c_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.product_supply_detail -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.PRODUCT_SUPPLY_DETAIL (master load).
--
-- Grain: branch_id, account_code, product_code, c_id (measured unique over 278,510 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 c_id,
 currency_code,
 account_code,
 ext_acc_product_code,
 fob_cost,
 future_fob_cost,
 future_fob_cost_date,
 lead_time,
 main_supplier,
 min_order_qty,
 product_code,
 purchase_uom_code,
 ave_lead_time,
 def_ship_via,
 def_ship_to,
 conv_factor,
 purchase_conv_factor,
 manuf_code,
 manuf_name,
 active_flag,
 alternate_supp,
 receive_uom_code,
 created_by_user,
 creation_date,
 amend_last_date,
 amend_by_user,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('product_supply_detail') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
