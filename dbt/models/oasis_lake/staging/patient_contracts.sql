{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, pat_contract_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.patient_contracts -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PATIENT_CONTRACTS (master load).
--
-- Grain: branch_id, pat_contract_id (measured unique over 3,834,978 lake rows)
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
 contract_no,
 start_date,
 end_date,
 default_flag,
 default_sequence,
 amend_by_user,
 amend_last_date,
 contract_package_flag,
 document_no,
 document_date,
 narrative,
 created_by_user,
 creation_date,
 card_image_id,
 policy_no,
 pat_contract_id,
 hospital_id,
 allow_on_account_for_cash,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 back_card_image_id,
 merge_hash
from {{ iceberg_source('patient_contracts') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
