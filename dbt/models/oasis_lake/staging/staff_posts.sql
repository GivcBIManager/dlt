{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, post_number, date_started)',
    partition_by='branch_id'
) }}

-- oasis_lake.staff_posts -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.STAFF_POSTS (master load).
--
-- Grain: branch_id, post_number, date_started (measured unique over 31,570 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 staff_id,
 work_entity,
 position_type,
 fully_qualified,
 date_started,
 date_ended,
 post_number,
 amend_by_user,
 amend_last_date,
 phone,
 recruit_flag,
 user_post_number,
 request_vacation_id,
 posts_id,
 hospital_id,
 end_date,
 reports_to_post_number,
 post_type,
 staff_category,
 shift_category,
 src_branch_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('staff_posts') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
