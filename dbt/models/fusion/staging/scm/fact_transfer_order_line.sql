{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(transfer_order_line_id)'
) }}

-- scm.fact_transfer_order_line -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: transfer_order_line_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(TRANSFER_ORDER_LINE_ID, 0) as transfer_order_line_id
,TRANSFER_ORDER_HEADER_ID       as transfer_order_header_id
,TRANSFER_ORDER_NUMBER          as transfer_order_number
,LINE_NUMBER                    as line_number
,REQUISITION_LINE_ID            as requisition_line_id
,SOURCE_TYPE                    as source_type
,DESTINATION_TYPE               as destination_type
,LINE_STATUS                    as line_status
,INTERFACE_STATUS               as interface_status
,INVENTORY_ITEM_ID              as inventory_item_id
,SOURCE_ORGANIZATION_ID         as source_organization_id
,SOURCE_SUBINVENTORY_CODE       as source_subinventory_code
,DESTINATION_ORGANIZATION_ID    as destination_organization_id
,DESTINATION_SUBINVENTORY_CODE  as destination_subinventory_code
,ORDERED_DATE                   as ordered_date
,ORDERED_DATE_KEY               as ordered_date_key
,NEED_BY_DATE                   as need_by_date
,NEED_BY_DATE_KEY               as need_by_date_key
,UOM_CODE                       as uom_code
,REQUESTED_QTY                  as requested_qty
,INITIAL_REQUESTED_QTY          as initial_requested_qty
,SHIPPED_QTY                    as shipped_qty
,RECEIVED_QTY                   as received_qty
,DELIVERED_QTY                  as delivered_qty
,UNIT_PRICE                     as unit_price
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_transfer_order_line', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
