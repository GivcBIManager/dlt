{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, code, c_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.gl_code -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.GL_CODE (master load).
--
-- Grain: branch_id, code, c_id (measured unique over 243,964 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 c_id,
 code,
 status,
 default_sign,
 history_years,
 description,
 contra_code,
 type,
 alternate_key,
 level_1,
 level_2,
 level_3,
 level_4,
 level_5,
 level_6,
 cost_center,
 main_acc,
 sub_acc,
 created_by_user,
 creation_date,
 amend_by_user,
 amend_last_date,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('gl_code') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
