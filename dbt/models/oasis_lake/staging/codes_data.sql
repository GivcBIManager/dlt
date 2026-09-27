{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, code)',
    partition_by='branch_id'
) }}

-- oasis_lake.codes_data -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.CODES_DATA (master load).
--
-- Grain: branch_id, code (measured unique over 476,396 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 code,
 user_code,
 code_type,
 description,
 description_a,
 amend_by_user,
 amend_last_date,
 system_req,
 prog_code,
 associated_code,
 sub_code_type,
 select_statement,
 hospital_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('codes_data') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
