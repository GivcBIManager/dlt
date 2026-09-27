{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, analysis_codes_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.analysis_codes_data -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.ANALYSIS_CODES_DATA (master load).
--
-- Grain: branch_id, analysis_codes_id (measured unique over 23,669 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 analysis_code,
 category_id,
 description,
 full_name,
 status,
 default_sign,
 type,
 division_1,
 division_2,
 division_3,
 division_4,
 budget_category_code,
 amend_by_user,
 amend_last_date,
 analysis_code_parent,
 category_id_parent,
 group_code,
 group_type,
 short_code,
 hospital_id,
 analysis_codes_id,
 analysis_code_type,
 facility_id,
 c_budget_category_code,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('analysis_codes_data') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
