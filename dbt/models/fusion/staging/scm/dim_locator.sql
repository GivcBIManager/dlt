{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(inventory_location_id, organization_id)'
) }}

-- scm.dim_locator -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: inventory_location_id, organization_id (UNVERIFIED -- table is empty)
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
 ifNull(INVENTORY_LOCATION_ID, '') as inventory_location_id
,ifNull(ORGANIZATION_ID, '') as organization_id
,SUBINVENTORY_ID        as subinventory_id
,SUBINVENTORY_CODE      as subinventory_code
,INVENTORY_ITEM_ID      as inventory_item_id
,STATUS_ID              as status_id
,SEGMENT1               as segment1
,SEGMENT2               as segment2
,SEGMENT3               as segment3
,SEGMENT4               as segment4
,SEGMENT5               as segment5
,SEGMENT6               as segment6
,SEGMENT7               as segment7
,SEGMENT8               as segment8
,SEGMENT9               as segment9
,SEGMENT10              as segment10
,SEGMENT11              as segment11
,SEGMENT12              as segment12
,SEGMENT13              as segment13
,SEGMENT14              as segment14
,SEGMENT15              as segment15
,SEGMENT16              as segment16
,SEGMENT17              as segment17
,SEGMENT18              as segment18
,SEGMENT19              as segment19
,SEGMENT20              as segment20
,DESCRIPTION            as description
,START_DATE_ACTIVE      as start_date_active
,START_DATE_ACTIVE_KEY  as start_date_active_key
,END_DATE_ACTIVE        as end_date_active
,END_DATE_ACTIVE_KEY    as end_date_active_key
,DISABLE_DATE           as disable_date
,DISABLE_DATE_KEY       as disable_date_key
,ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_locator', 'scm') }}
{% if is_incremental() %}
where ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
