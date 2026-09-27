{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, account_transaction_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.account_transactions -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.ACCOUNT_TRANSACTIONS (transaction load).
--
-- Grain: branch_id, account_transaction_no (measured unique over 1,520,938 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 staff_id,
 account_type,
 transaction_date,
 transaction_type,
 transaction_source,
 payable_type,
 gosi_type,
 amount,
 tp_seq_no,
 staff_timesheet_no,
 regular_pay_no,
 status,
 amend_by_user,
 amend_last_date,
 account_transaction_no,
 transaction_uom,
 trans_time_xref,
 timesheet_type,
 pay_in_payroll,
 loan_payment_id,
 position_type,
 work_entity,
 doc_id,
 payment_method,
 doc_no,
 staff_flag,
 staff_category,
 analysis_code,
 cost_center,
 year,
 period,
 payroll_status,
 employee_dependant_flag,
 nationality_code,
 contract_type,
 trx_type,
 trx_source,
 regular_hour,
 batch_id,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('account_transactions') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
