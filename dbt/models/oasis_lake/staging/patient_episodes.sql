{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, patient_id, episode_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.patient_episodes -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PATIENT_EPISODES (transaction load).
--
-- Grain: branch_id, patient_id, episode_no (measured unique over 4,544,669 lake rows)
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
 start_date,
 end_date,
 admitted_flag,
 invoiced_flag,
 eligibility_type,
 purpose_of_episode,
 visit_reason_text,
 notes,
 amend_by_user,
 amend_last_date,
 emergency_flag,
 injury_id,
 external_flag,
 unsettled_flag,
 batch_id,
 hospital_id,
 reviewed,
 after_discharge_episode_no,
 visit_no,
 clinically_restricted,
 clinically_restricted_by,
 clinically_restricted_date,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash,
 concealed_pass_code
from {{ iceberg_source('patient_episodes') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
