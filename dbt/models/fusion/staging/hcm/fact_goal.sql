{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(goal_id)'
) }}

-- hcm.fact_goal -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: goal_id (UNVERIFIED -- table is empty)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
-- LAST_UPDATE_DATE also arrives as a STRING from this (empty) table, and
-- ReplacingMergeTree will not take a String version column, so it is parsed to
-- DateTime64(6) here -- the same type the populated models already expose.
--
-- The warehouse table is STILL EMPTY, so this grain could not be measured: the
-- sorting key is inferred from the Fusion table's natural key, and is chosen
-- deliberately WIDE. Under ReplacingMergeTree a key that is too wide only
-- skips dedup, while one that is too narrow silently collapses distinct rows.
-- The `unique_combination_final` test on this key in _ofusion__models.yml
-- fails the run as soon as real data contradicts the guess.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(GOAL_ID, '')         as goal_id
,PERSON_ID                   as person_id
,ASSIGNMENT_ID               as assignment_id
,ORGANIZATION_ID             as organization_id
,ASSIGNED_BY_PERSON_ID       as assigned_by_person_id
,REFERENCE_GOAL_ID           as reference_goal_id
,REFERENCE_CONTENT_ITEM_ID   as reference_content_item_id
,GOAL_NAME                   as goal_name
,GOAL_TYPE_CODE              as goal_type_code
,GOAL_VERSION_TYPE_CODE      as goal_version_type_code
,GOAL_SUB_TYPE_CODE          as goal_sub_type_code
,STATUS_CODE                 as status_code
,APPROVAL_STATUS_CODE        as approval_status_code
,PERCENT_COMPLETE_CODE       as percent_complete_code
,PRIORITY_CODE               as priority_code
,LEVEL_CODE                  as level_code
,CATEGORY_CODE               as category_code
,GOAL_SOURCE_CODE            as goal_source_code
,PRIVATE_FLAG                as private_flag
,ACTIVE_FLAG                 as active_flag
,MEASURE_TYPE_CODE           as measure_type_code
,UOM_CODE                    as uom_code
,TARGET_VALUE                as target_value
,ACTUAL_VALUE                as actual_value
,GOAL_COUNT                  as goal_count
,GOAL_START_DATE             as goal_start_date
,GOAL_START_DATE_KEY         as goal_start_date_key
,TARGET_COMPLETION_DATE      as target_completion_date
,TARGET_COMPLETION_DATE_KEY  as target_completion_date_key
,ACTUAL_COMPLETION_DATE      as actual_completion_date
,ACTUAL_COMPLETION_DATE_KEY  as actual_completion_date_key
,GOAL_CREATION_DATE          as goal_creation_date
,GOAL_CREATION_DATE_KEY      as goal_creation_date_key
,ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_goal', 'hcm') }}
{% if is_incremental() %}
where ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
