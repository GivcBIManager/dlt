{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(location_id, valid_from)'
) }}

-- conformed.dim_location -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: location_id, valid_from (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(LOCATION_ID, 0) as location_id
,LOCATION_CODE     as location_code
,LOCATION_NAME     as location_name
,ADDRESS_LINE_1    as address_line_1
,ADDRESS_LINE_2    as address_line_2
,TOWN_OR_CITY      as town_or_city
,REGION_1          as region_1
,REGION_2          as region_2
,POSTAL_CODE       as postal_code
,COUNTRY           as country
,ifNull(VALID_FROM, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as valid_from
,VALID_TO          as valid_to
,IS_CURRENT        as is_current
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_location', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
