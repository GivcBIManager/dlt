{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(line_location_id)'
) }}

-- scm.fact_po_schedule -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: line_location_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(LINE_LOCATION_ID, 0)    as line_location_id
,PO_HEADER_ID                   as po_header_id
,PO_LINE_ID                     as po_line_id
,PO_NUMBER                      as po_number
,LINE_NUM                       as line_num
,SHIPMENT_NUM                   as shipment_num
,VENDOR_ID                      as vendor_id
,VENDOR_SITE_ID                 as vendor_site_id
,PRC_BU_ID                      as prc_bu_id
,REQ_BU_ID                      as req_bu_id
,SHIP_TO_ORGANIZATION_ID        as ship_to_organization_id
,SHIP_TO_LOCATION_ID            as ship_to_location_id
,ITEM_ID                        as item_id
,CATEGORY_ID                    as category_id
,UOM_CODE                       as uom_code
,LINE_UOM_CODE                  as line_uom_code
,CURRENCY_CODE                  as currency_code
,DOCUMENT_STATUS                as document_status
,TYPE_LOOKUP_CODE               as type_lookup_code
,REVISION_NUM                   as revision_num
,LINE_STATUS                    as line_status
,SCHEDULE_STATUS                as schedule_status
,SHIPMENT_TYPE                  as shipment_type
,DESTINATION_TYPE_CODE          as destination_type_code
,ORDER_TYPE_LOOKUP_CODE         as order_type_lookup_code
,PURCHASE_BASIS                 as purchase_basis
,MATCHING_BASIS                 as matching_basis
,SCHEDULE_MATCHING_BASIS        as schedule_matching_basis
,VALUE_BASIS                    as value_basis
,MATCH_OPTION                   as match_option
,RECEIPT_REQUIRED_FLAG          as receipt_required_flag
,INSPECTION_REQUIRED_FLAG       as inspection_required_flag
,ACCRUE_ON_RECEIPT_FLAG         as accrue_on_receipt_flag
,RECEIVING_ROUTING_ID           as receiving_routing_id
,OUTSOURCED_ASSEMBLY            as outsourced_assembly
,PO_CANCEL_FLAG                 as po_cancel_flag
,LINE_CANCEL_FLAG               as line_cancel_flag
,SCHEDULE_CANCEL_FLAG           as schedule_cancel_flag
,PO_CREATION_DATE               as po_creation_date
,PO_CREATION_DATE_KEY           as po_creation_date_key
,NEED_BY_DATE                   as need_by_date
,NEED_BY_DATE_KEY               as need_by_date_key
,PROMISED_DATE                  as promised_date
,PROMISED_DATE_KEY              as promised_date_key
,REQUESTED_SHIP_DATE            as requested_ship_date
,REQUESTED_SHIP_DATE_KEY        as requested_ship_date_key
,PROMISED_SHIP_DATE             as promised_ship_date
,PROMISED_SHIP_DATE_KEY         as promised_ship_date_key
,ANTICIPATED_ARRIVAL_DATE       as anticipated_arrival_date
,ANTICIPATED_ARRIVAL_DATE_KEY   as anticipated_arrival_date_key
,LAST_ACCEPT_DATE               as last_accept_date
,LAST_ACCEPT_DATE_KEY           as last_accept_date_key
,PO_CLOSED_DATE                 as po_closed_date
,PO_CLOSED_DATE_KEY             as po_closed_date_key
,SCHEDULE_CLOSED_DATE           as schedule_closed_date
,SCHEDULE_CLOSED_DATE_KEY       as schedule_closed_date_key
,SHIPMENT_CLOSED_DATE           as shipment_closed_date
,SHIPMENT_CLOSED_DATE_KEY       as shipment_closed_date_key
,CLOSED_FOR_RECEIVING_DATE      as closed_for_receiving_date
,CLOSED_FOR_RECEIVING_DATE_KEY  as closed_for_receiving_date_key
,CLOSED_FOR_INVOICE_DATE        as closed_for_invoice_date
,CLOSED_FOR_INVOICE_DATE_KEY    as closed_for_invoice_date_key
,CANCEL_DATE                    as cancel_date
,CANCEL_DATE_KEY                as cancel_date_key
,QUANTITY                       as quantity
,QUANTITY_RECEIVED              as quantity_received
,QUANTITY_ACCEPTED              as quantity_accepted
,QUANTITY_REJECTED              as quantity_rejected
,QUANTITY_BILLED                as quantity_billed
,QUANTITY_CANCELLED             as quantity_cancelled
,QUANTITY_SHIPPED               as quantity_shipped
,LINE_QUANTITY                  as line_quantity
,AMOUNT                         as amount
,AMOUNT_RECEIVED                as amount_received
,AMOUNT_ACCEPTED                as amount_accepted
,AMOUNT_REJECTED                as amount_rejected
,AMOUNT_BILLED                  as amount_billed
,AMOUNT_CANCELLED               as amount_cancelled
,AMOUNT_SHIPPED                 as amount_shipped
,PRICE_OVERRIDE                 as price_override
,TAX_EXCLUSIVE_PRICE            as tax_exclusive_price
,UNIT_PRICE                     as unit_price
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_po_schedule', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
