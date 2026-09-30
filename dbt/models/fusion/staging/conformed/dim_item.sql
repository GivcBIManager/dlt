{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(inventory_item_id, organization_id)'
) }}

-- conformed.dim_item -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: inventory_item_id, organization_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(INVENTORY_ITEM_ID, 0) as inventory_item_id
,ifNull(ORGANIZATION_ID, 0)   as organization_id
,ITEM_NUMBER                  as item_number
,ITEM_DESCRIPTION             as item_description
,PRIMARY_UOM_CODE             as primary_uom_code
,ITEM_TYPE                    as item_type
,ITEM_STATUS_CODE             as item_status_code
,LIFECYCLE_PHASE              as lifecycle_phase
,INVENTORY_ITEM_FLAG          as inventory_item_flag
,PURCHASING_ITEM_FLAG         as purchasing_item_flag
,CUSTOMER_ORDER_ENABLED_FLAG  as customer_order_enabled_flag
,STOCK_ENABLED_FLAG           as stock_enabled_flag
,LOT_CONTROL_CODE             as lot_control_code
,SERIAL_NUMBER_CONTROL_CODE   as serial_number_control_code
,ITEM_CATALOG_GROUP_ID        as item_catalog_group_id
,SECONDARY_UOM_CODE           as secondary_uom_code
,PURCHASING_ENABLED_FLAG      as purchasing_enabled_flag
,INTERNAL_ORDER_ENABLED_FLAG  as internal_order_enabled_flag
,BUYER_ID                     as buyer_id
,LIST_PRICE_PER_UNIT          as list_price_per_unit
,MRP_PLANNING_CODE            as mrp_planning_code
,MIN_MINMAX_QUANTITY          as min_minmax_quantity
,MAX_MINMAX_QUANTITY          as max_minmax_quantity
,ATTRIBUTE_CATEGORY           as attribute_category
,ATTRIBUTE1                   as attribute1
,ATTRIBUTE2                   as attribute2
,ATTRIBUTE_NUMBER1            as attribute_number1
,CREATION_DATE                as creation_date
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_item', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
