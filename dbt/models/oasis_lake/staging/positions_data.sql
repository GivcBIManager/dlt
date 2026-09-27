{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, position_type)',
    partition_by='branch_id'
) }}

-- oasis_lake.positions_data -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.POSITIONS_DATA (master load).
--
-- Grain: branch_id, position_type (measured unique over 3,753 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 position_type,
 description,
 maximum_grade,
 maximum_point,
 optimum_grade,
 optimum_point,
 minimum_grade,
 minimum_point,
 amend_by_user,
 amend_last_date,
 gl_section_code,
 patient_care_pct,
 patient_contact_code,
 mental_health_officer_flag,
 staff_category,
 budget_grade_point_id,
 pay_grade_table_id,
 hospital_id,
 role_id,
 post_type,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('positions_data') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
