{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, request_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.authorisations_master -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.AUTHORISATIONS_MASTER (transaction load).
--
-- Grain: branch_id, request_no (measured unique over 3,072,608 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 request_no,
 patient_id,
 episode_no,
 request_date,
 status,
 created_by_user,
 creation_date,
 amend_by_user,
 amend_last_date,
 dhs_transaction_no,
 priority_date,
 priority_machine,
 hospital_id,
 estimated_amount,
 contract_no,
 online_status,
 assigned_staff_id,
 admission_request_id,
 referring_episode,
 facility_id,
 encounter_start,
 encounter_end,
 extend,
 relative_weight,
 drg_code,
 saving_life,
 specialty_doctore_code,
 referal_no,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash,
 portal_reference_id
from {{ iceberg_source('authorisations_master') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
