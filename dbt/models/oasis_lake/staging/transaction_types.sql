{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, transaction_type)',
    partition_by='branch_id'
) }}

-- oasis_lake.transaction_types -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.TRANSACTION_TYPES (master load).
--
-- Grain: branch_id, transaction_type (measured unique over 354 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 transaction_type,
 allowance_desc,
 allowance_or_deduction,
 included_in_gosi_flag,
 gosi_ceiling,
 payable_type,
 amend_by_user,
 amend_last_date,
 included_in_bonus,
 transaction_uom,
 accrual_flag,
 included_in_pay_leave,
 transaction_sequence,
 paid_against_invoice,
 hospital_id,
 maximum_installments,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('transaction_types') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
