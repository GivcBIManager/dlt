{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, master_order_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.orders_master -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.ORDERS_MASTER (transaction load).
--
-- Grain: branch_id, master_order_no (measured unique over 23,402,601 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 master_order_no,
 patient_id,
 episode_no,
 orderer_staff_id,
 order_date,
 status,
 admission_no,
 stock_odoc,
 service_dept,
 attendance_type,
 amend_by_user,
 amend_last_date,
 consultant_related,
 patient_mobility,
 need_portable_machine,
 patient_name,
 hl7_visit_no,
 patient_diagnosis_id,
 bb_master_order_no,
 presc_seq,
 presc_seq_prefix,
 protocol_id,
 cycle_seq,
 hl7_encounter,
 result_seen,
 report_seen,
 hospital_id,
 send_refill_sms,
 serial_no,
 refill_sms_date,
 workstation_name,
 trx_sent,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('orders_master') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
