{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(item_relationship_id)'
) }}

-- scm.dim_item_relationship -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: item_relationship_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(ITEM_RELATIONSHIP_ID, 0) as item_relationship_id
,ITEM_RELATIONSHIP_TYPE  as item_relationship_type
,SUB_TYPE                as sub_type
,INVENTORY_ITEM_ID       as inventory_item_id
,ORGANIZATION_ID         as organization_id
,MASTER_ORGANIZATION_ID  as master_organization_id
,RELATED_ITEM_ID         as related_item_id
,CROSS_REFERENCE         as cross_reference
,UOM_CODE                as uom_code
,TRADING_PARTNER_ID      as trading_partner_id
,RELATIONSHIP_RANK       as relationship_rank
,RECIPROCAL_FLAG         as reciprocal_flag
,INACTIVE_FLAG           as inactive_flag
,START_DATE              as start_date
,START_DATE_KEY          as start_date_key
,END_DATE                as end_date
,END_DATE_KEY            as end_date_key
,VERSION_ID              as version_id
,CHANGE_LINE_ID          as change_line_id
,ACD_TYPE                as acd_type
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_item_relationship', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
