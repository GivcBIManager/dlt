{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, patient_id, episode_no, responsibility_seq)',
    partition_by='branch_id'
) }}

-- oasis_lake.patient_bill_agreements -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PATIENT_BILL_AGREEMENTS (transaction load).
--
-- Grain: branch_id, patient_id, episode_no, responsibility_seq (measured unique over 40,340,226 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 patient_id,
 episode_no,
 account_no,
 tariff_id,
 purchaser_code,
 policy_code,
 responsibility_seq,
 contract_no,
 status,
 bill_to,
 amend_by_user,
 amend_last_date,
 excluding_diagnosis,
 contract_package_flag,
 close_with_package_flag,
 continue_after_package_flag,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('patient_bill_agreements') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
