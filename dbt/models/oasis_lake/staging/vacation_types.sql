{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, vacation_type_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.vacation_types -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.VACATION_TYPES (master load).
--
-- Grain: branch_id, vacation_type_id (measured unique over 175 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 vacation_type_id,
 description,
 authorization,
 max_days,
 leave_official_flag,
 hour_rate_factor,
 long_id,
 amend_by_user,
 amend_last_date,
 timesheet_type,
 no_of_days_permitted,
 deducted_vacation,
 auto_vacation_return_flag,
 issue_reason,
 travel_allowance_flag,
 backdate_days,
 transaction_type,
 no_of_time,
 min_days,
 count_weekend,
 count_official,
 vacation_types,
 color_name,
 color_rg,
 start_end_weekend,
 prefix,
 needs_attachment,
 hospital_id,
 word_path,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('vacation_types') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
