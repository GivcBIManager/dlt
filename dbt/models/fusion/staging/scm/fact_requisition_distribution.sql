{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(distribution_id)'
) }}

-- scm.fact_requisition_distribution -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: distribution_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(DISTRIBUTION_ID, 0)       as distribution_id
,DISTRIBUTION_NUMBER              as distribution_number
,REQUISITION_LINE_ID              as requisition_line_id
,REQUISITION_HEADER_ID            as requisition_header_id
,REQUISITION_NUMBER               as requisition_number
,LINE_NUMBER                      as line_number
,REQ_BU_ID                        as req_bu_id
,PRC_BU_ID                        as prc_bu_id
,CODE_COMBINATION_ID              as code_combination_id
,PRIMARY_LEDGER_ID                as primary_ledger_id
,PREPARER_ID                      as preparer_id
,REQUESTER_ID                     as requester_id
,SUGGESTED_BUYER_ID               as suggested_buyer_id
,ASSIGNED_BUYER_ID                as assigned_buyer_id
,ITEM_ID                          as item_id
,CATEGORY_ID                      as category_id
,ITEM_DESCRIPTION                 as item_description
,ITEM_SOURCE                      as item_source
,UOM_CODE                         as uom_code
,CURRENCY_CODE                    as currency_code
,VENDOR_ID                        as vendor_id
,VENDOR_SITE_ID                   as vendor_site_id
,DESTINATION_ORGANIZATION_ID      as destination_organization_id
,SOURCE_ORGANIZATION_ID           as source_organization_id
,DELIVER_TO_LOCATION_ID           as deliver_to_location_id
,PO_HEADER_ID                     as po_header_id
,PO_LINE_ID                       as po_line_id
,LINE_LOCATION_ID                 as line_location_id
,DOCUMENT_STATUS                  as document_status
,LIFECYCLE_STATUS                 as lifecycle_status
,HAS_WITHDRAWN_LINES              as has_withdrawn_lines
,HAS_CANCELED_LINES               as has_canceled_lines
,HAS_RETURNED_LINES               as has_returned_lines
,HAS_REJECTED_LINES               as has_rejected_lines
,HAS_INCOMPLETE_LINES             as has_incomplete_lines
,LINE_STATUS                      as line_status
,LINE_LIFECYCLE_STATUS            as line_lifecycle_status
,LINE_LIFECYCLE_SECONDARY_STATUS  as line_lifecycle_secondary_status
,CANCEL_FLAG                      as cancel_flag
,SOURCE_TYPE_CODE                 as source_type_code
,DESTINATION_TYPE_CODE            as destination_type_code
,ORDER_TYPE_LOOKUP_CODE           as order_type_lookup_code
,PURCHASE_BASIS                   as purchase_basis
,MATCHING_BASIS                   as matching_basis
,FUNDS_STATUS                     as funds_status
,PJC_PROJECT_ID                   as pjc_project_id
,PJC_TASK_ID                      as pjc_task_id
,REQUISITION_CREATION_DATE        as requisition_creation_date
,REQUISITION_CREATION_DATE_KEY    as requisition_creation_date_key
,SUBMISSION_DATE                  as submission_date
,SUBMISSION_DATE_KEY              as submission_date_key
,APPROVED_DATE                    as approved_date
,APPROVED_DATE_KEY                as approved_date_key
,NEED_BY_DATE                     as need_by_date
,NEED_BY_DATE_KEY                 as need_by_date_key
,REQUESTED_SHIP_DATE              as requested_ship_date
,REQUESTED_SHIP_DATE_KEY          as requested_ship_date_key
,CANCEL_DATE                      as cancel_date
,CANCEL_DATE_KEY                  as cancel_date_key
,BUDGET_DATE                      as budget_date
,BUDGET_DATE_KEY                  as budget_date_key
,RATE_DATE                        as rate_date
,RATE_DATE_KEY                    as rate_date_key
,DISTRIBUTION_QUANTITY            as distribution_quantity
,DISTRIBUTION_AMOUNT              as distribution_amount
,DISTRIBUTION_CURRENCY_AMOUNT     as distribution_currency_amount
,DISTRIBUTION_PERCENT             as distribution_percent
,LINE_QUANTITY                    as line_quantity
,LINE_QUANTITY_DELIVERED          as line_quantity_delivered
,LINE_QUANTITY_RECEIVED           as line_quantity_received
,LINE_QUANTITY_CANCELLED          as line_quantity_cancelled
,LINE_AMOUNT                      as line_amount
,LINE_CURRENCY_AMOUNT             as line_currency_amount
,UNIT_PRICE                       as unit_price
,CURRENCY_UNIT_PRICE              as currency_unit_price
,RATE                             as rate
,RATE_TYPE                        as rate_type
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_requisition_distribution', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
