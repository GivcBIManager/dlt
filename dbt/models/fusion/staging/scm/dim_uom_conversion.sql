{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(conversion_id)'
) }}

-- scm.dim_uom_conversion -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: conversion_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(CONVERSION_ID, 0) as conversion_id
,INVENTORY_ITEM_ID        as inventory_item_id
,UOM_CODE                 as uom_code
,UOM_CLASS                as uom_class
,CONVERSION_RATE          as conversion_rate
,DEFAULT_CONVERSION_FLAG  as default_conversion_flag
,INVERSE_FLAG             as inverse_flag
,INVERSE_CONVERSION_RATE  as inverse_conversion_rate
,CONTAINED_UOM_CODE       as contained_uom_code
,DISABLE_DATE             as disable_date
,DISABLE_DATE_KEY         as disable_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_uom_conversion', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
