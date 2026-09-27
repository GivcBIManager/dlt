{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, bed_detail_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.bed_details -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.BED_DETAILS (master load).
--
-- Grain: branch_id, bed_detail_id (measured unique over 1,764,033 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 current_record,
 work_entity,
 room_no,
 bed_location,
 bed_status,
 bed_class,
 start_date,
 end_date,
 patient_id,
 admission_no,
 team_firm,
 episode_no,
 bed_no,
 amend_by_user,
 amend_last_date,
 trans_from_work_entity,
 trans_from_bed_location,
 bed_sex,
 hl7_sent,
 bed_detail_id,
 hospital_id,
 trx_sent,
 mmm_sent,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 int_sent,
 merge_hash
from {{ iceberg_source('bed_details') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
