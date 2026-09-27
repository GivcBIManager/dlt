{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, patient_id_seq)',
    partition_by='branch_id'
) }}

-- oasis_lake.patient_ids -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PATIENT_IDS (master load).
--
-- Grain: branch_id, patient_id_seq (measured unique over 3,229,079 lake rows)
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
 id_type_code,
 id_number,
 id_where_issued,
 id_when_issued,
 amend_by_user,
 amend_last_date,
 id_when_expired,
 default_flag,
 patient_id_seq,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('patient_ids') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
