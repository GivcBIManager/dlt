{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, patient_id, episode_no, encounter_id, order_set_id, hospital_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.patient_order_sets -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PATIENT_ORDER_SETS (master load).
--
-- Grain: branch_id, patient_id, episode_no, encounter_id, order_set_id, hospital_id (measured unique over 49,444 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 hospital_id,
 patient_id,
 episode_no,
 encounter_id,
 encounter_type,
 order_set_id,
 answer_session_id,
 creation_date,
 created_by_user,
 amend_last_date,
 amend_by_user,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('patient_order_sets') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
