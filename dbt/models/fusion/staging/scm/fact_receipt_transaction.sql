{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(transaction_id)'
) }}

-- scm.fact_receipt_transaction -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: transaction_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(TRANSACTION_ID, 0)     as transaction_id
,PARENT_TRANSACTION_ID         as parent_transaction_id
,SHIPMENT_HEADER_ID            as shipment_header_id
,SHIPMENT_LINE_ID              as shipment_line_id
,RECEIPT_NUM                   as receipt_num
,LINE_NUM                      as line_num
,PO_HEADER_ID                  as po_header_id
,PO_LINE_ID                    as po_line_id
,PO_LINE_LOCATION_ID           as po_line_location_id
,PO_DISTRIBUTION_ID            as po_distribution_id
,PO_REVISION_NUM               as po_revision_num
,REQUISITION_LINE_ID           as requisition_line_id
,REQ_DISTRIBUTION_ID           as req_distribution_id
,INV_TRANSACTION_ID            as inv_transaction_id
,ITEM_ID                       as item_id
,CATEGORY_ID                   as category_id
,ORGANIZATION_ID               as organization_id
,TO_ORGANIZATION_ID            as to_organization_id
,SUBINVENTORY                  as subinventory
,LOCATOR_ID                    as locator_id
,VENDOR_ID                     as vendor_id
,VENDOR_SITE_ID                as vendor_site_id
,SHIPMENT_VENDOR_ID            as shipment_vendor_id
,SHIPMENT_VENDOR_SITE_ID       as shipment_vendor_site_id
,VENDOR_LOT_NUM                as vendor_lot_num
,TRANSACTION_TYPE              as transaction_type
,DESTINATION_TYPE_CODE         as destination_type_code
,SOURCE_DOCUMENT_CODE          as source_document_code
,RECEIPT_SOURCE_CODE           as receipt_source_code
,SHIPMENT_LINE_STATUS_CODE     as shipment_line_status_code
,INSPECTION_STATUS_CODE        as inspection_status_code
,ACCRUAL_STATUS_CODE           as accrual_status_code
,INVOICE_STATUS_CODE           as invoice_status_code
,ROUTING_HEADER_ID             as routing_header_id
,UOM_CODE                      as uom_code
,PRIMARY_UOM_CODE              as primary_uom_code
,BILLING_UOM_CODE              as billing_uom_code
,LINE_UOM_CODE                 as line_uom_code
,CURRENCY_CODE                 as currency_code
,CURRENCY_CONVERSION_TYPE      as currency_conversion_type
,CURRENCY_CONVERSION_RATE      as currency_conversion_rate
,TRANSACTION_DATE              as transaction_date
,TRANSACTION_DATE_KEY          as transaction_date_key
,CURRENCY_CONVERSION_DATE      as currency_conversion_date
,CURRENCY_CONVERSION_DATE_KEY  as currency_conversion_date_key
,EXPECTED_RECEIPT_DATE         as expected_receipt_date
,EXPECTED_RECEIPT_DATE_KEY     as expected_receipt_date_key
,SHIPPED_DATE                  as shipped_date
,SHIPPED_DATE_KEY              as shipped_date_key
,QUANTITY                      as quantity
,PRIMARY_QUANTITY              as primary_quantity
,QUANTITY_BILLED               as quantity_billed
,AMOUNT                        as amount
,AMOUNT_BILLED                 as amount_billed
,PO_UNIT_PRICE                 as po_unit_price
,LINE_QUANTITY_SHIPPED         as line_quantity_shipped
,LINE_QUANTITY_RECEIVED        as line_quantity_received
,LINE_QUANTITY_DELIVERED       as line_quantity_delivered
,LINE_QUANTITY_RETURNED        as line_quantity_returned
,LINE_QUANTITY_ACCEPTED        as line_quantity_accepted
,LINE_QUANTITY_REJECTED        as line_quantity_rejected
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_receipt_transaction', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
