{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, delivery_line)',
    partition_by='branch_id'
) }}

-- oasis_lake.delivery_lines -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.DELIVERY_LINES (transaction load).
--
-- Grain: branch_id, delivery_line (measured unique over 95,848,169 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 delivery_line,
 master_delivery_no,
 order_line,
 units_delivered,
 unit_price,
 product_code,
 batch_no,
 ios,
 status,
 dosage_code,
 dosage_narrative,
 rec_line_id,
 c_id,
 delivered_flag,
 amend_by_user,
 amend_last_date,
 received_by,
 received_date,
 alternative_flag,
 extra_dose_qty,
 units_administered,
 units_lost,
 orig_units_delivered,
 driving_delivery_line,
 hl7_sent,
 original_unit_price,
 dhs_transaction_no,
 hospital_id,
 trx_sent,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash,
 nphies_cs_sent
from {{ iceberg_source('delivery_lines') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
