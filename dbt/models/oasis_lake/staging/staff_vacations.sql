{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, staff_vacation_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.staff_vacations -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.STAFF_VACATIONS (transaction load).
--
-- Grain: branch_id, staff_vacation_id (measured unique over 96,347 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 staff_vacation_id,
 vacation_type_id,
 staff_id,
 staff_contract_no,
 description,
 doc_no,
 doc_date,
 from_date,
 return_date,
 days,
 request_vacation_id,
 amend_by_user,
 amend_last_date,
 long_id,
 unpaid_from_date,
 unpaid_days,
 unpaid_to_date,
 annual_leave_status,
 status,
 deduction_flag,
 exceed_days,
 hospital_id,
 to_date,
 start_work_date,
 official_rowid,
 official_dml_type,
 notes,
 batch_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('staff_vacations') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
