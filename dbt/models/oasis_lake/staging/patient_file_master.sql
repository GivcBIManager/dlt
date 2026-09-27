{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, pat_file_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.patient_file_master -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PATIENT_FILE_MASTER (master load).
--
-- Grain: branch_id, pat_file_id (measured unique over 3,399,441 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 pat_file_id,
 patient_id,
 med_rec_type,
 episode_no,
 appointment_id,
 home_work_entity,
 merged_with_pat_file_id,
 merged_date,
 archived_date,
 amend_by_user,
 amend_last_date,
 user_file_id,
 location_id,
 volume_id,
 created_by_user,
 creation_date,
 file_ordered_by_staff_id,
 hospital_id,
 old_user_file_id,
 trx_sent,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('patient_file_master') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
