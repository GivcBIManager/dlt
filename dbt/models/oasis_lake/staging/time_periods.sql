{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, sequence_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.time_periods -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.TIME_PERIODS (master load).
--
-- Grain: branch_id, sequence_no (measured unique over 2,340 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 calendar,
 c_id,
 sequence_no,
 time_period,
 year,
 ap_status,
 ar_status,
 description,
 end_date,
 gl_status,
 start_date,
 so_status,
 cb_status,
 py_status,
 cutoff_week,
 dest_flag,
 iset_flag,
 interest_rate,
 fms_no,
 gl_no_2,
 am_status,
 quarter_flag,
 am_period_no,
 new_asset_added,
 im_status,
 manual_voucher_status,
 early_closed_date,
 skip_flag,
 amend_by_user,
 amend_last_date,
 op_code,
 creation_date,
 hospital_id,
 closed_period_status,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('time_periods') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
