{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, signon_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.cashiers_signon -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.CASHIERS_SIGNON (master load).
--
-- Grain: branch_id, signon_id (measured unique over 690,472 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 signon_id,
 staff_id,
 shift_start,
 shift_end,
 shift_status,
 shift_begin_amount,
 shift_begin_notes,
 sys_cash_amount,
 sys_cash_no,
 sys_crdt_amount,
 sys_crdt_no,
 act_cash_amount,
 act_cash_no,
 act_crdt_amount,
 act_crdt_no,
 shift_end_notes,
 sys_e_cash_amount,
 sys_e_cash_no,
 act_e_cash_amount,
 act_e_cash_no,
 cancelled_cash_amount,
 cancelled_e_cash_amount,
 cancelled_credit_amount,
 reference_code,
 bank_reference_code,
 machine_name,
 machine_ip,
 hide_from_paytrf,
 closing_journal,
 sys_on_account_amount,
 sys_on_account_no,
 act_on_account_amount,
 act_on_account_no,
 cancelled_on_account_amount,
 cancelled_on_account_no,
 cancelled_credit_no,
 cancelled_cash_no,
 cancelled_e_cash_no,
 created_by_user,
 creation_date,
 amend_by_user,
 amend_last_date,
 settlement_amount,
 manually_settled,
 settlement_note,
 settled_by_user,
 settlement_date,
 cash_invoices_amount,
 cash_invoices_no,
 refund,
 recharge,
 close_staff_id,
 close_date,
 hospital_id,
 auto_open,
 span_ser,
 original_signon_id,
 ready_for_close,
 facility_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('cashiers_signon') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
