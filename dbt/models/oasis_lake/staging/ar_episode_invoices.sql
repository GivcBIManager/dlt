{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, invoice_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.ar_episode_invoices -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.AR_EPISODE_INVOICES (transaction load).
--
-- Grain: branch_id, invoice_no (measured unique over 3,626,586 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 invoice_no,
 invoice_creation_date,
 invoice_start_date,
 invoice_end_date,
 account_code,
 patient_id,
 episode_no,
 attendance_type,
 invoice_gross,
 invoice_net_amount,
 invoice_discount,
 unalloc_amount,
 stat_invoice_no,
 amend_by_user,
 amend_last_date,
 package_deal_flag,
 gosi_invoice_no,
 injury_id,
 treat_id,
 printed_flag,
 invoice_vat,
 invoice_total,
 hospital_id,
 approval_status,
 api_trans_id,
 notes,
 related_api_trans_id,
 claim_type,
 submit_completed,
 e_invoice_sent,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('ar_episode_invoices') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
