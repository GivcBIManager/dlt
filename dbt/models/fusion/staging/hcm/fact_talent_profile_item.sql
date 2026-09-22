{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(profile_item_id)'
) }}

-- hcm.fact_talent_profile_item -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: profile_item_id (UNVERIFIED -- table is empty)
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
 ifNull(PROFILE_ITEM_ID, '') as profile_item_id
,PROFILE_ID              as profile_id
,PERSON_ID               as person_id
,BUSINESS_GROUP_ID       as business_group_id
,CONTENT_TYPE_ID         as content_type_id
,CONTENT_ITEM_ID         as content_item_id
,PARENT_PROFILE_ITEM_ID  as parent_profile_item_id
,SECTION_ID              as section_id
,RATING_LEVEL_ID_1       as rating_level_id_1
,RATING_LEVEL_ID_2       as rating_level_id_2
,RATING_LEVEL_ID_3       as rating_level_id_3
,RATING_MODEL_ID_1       as rating_model_id_1
,INTEREST_LEVEL          as interest_level
,MANDATORY               as mandatory
,IMPORTANCE              as importance
,SOURCE_TYPE             as source_type
,PROFILE_STATUS_CODE     as profile_status_code
,ITEM_START_DATE         as item_start_date
,ITEM_START_DATE_KEY     as item_start_date_key
,ITEM_END_DATE           as item_end_date
,ITEM_END_DATE_KEY       as item_end_date_key
,ITEM_CREATION_DATE      as item_creation_date
,ITEM_CREATION_DATE_KEY  as item_creation_date_key
,ITEM_COUNT              as item_count
,ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_talent_profile_item', 'hcm') }}
{% if is_incremental() %}
where ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
