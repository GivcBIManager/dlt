{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(po_distribution_id)'
) }}

-- scm.fact_po_distribution -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: po_distribution_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(PO_DISTRIBUTION_ID, 0) as po_distribution_id
,LINE_LOCATION_ID             as line_location_id
,PO_HEADER_ID                 as po_header_id
,PO_LINE_ID                   as po_line_id
,DISTRIBUTION_NUM             as distribution_num
,PO_NUMBER                    as po_number
,LINE_NUM                     as line_num
,SHIPMENT_NUM                 as shipment_num
,REQ_DISTRIBUTION_ID          as req_distribution_id
,VENDOR_ID                    as vendor_id
,VENDOR_SITE_ID               as vendor_site_id
,PRC_BU_ID                    as prc_bu_id
,REQ_BU_ID                    as req_bu_id
,SET_OF_BOOKS_ID              as set_of_books_id
,CODE_COMBINATION_ID          as code_combination_id
,DESTINATION_ORGANIZATION_ID  as destination_organization_id
,DELIVER_TO_LOCATION_ID       as deliver_to_location_id
,DELIVER_TO_PERSON_ID         as deliver_to_person_id
,AGENT_ID                     as agent_id
,ITEM_ID                      as item_id
,CATEGORY_ID                  as category_id
,UOM_CODE                     as uom_code
,LINE_UOM_CODE                as line_uom_code
,CURRENCY_CODE                as currency_code
,PJC_PROJECT_ID               as pjc_project_id
,PJC_TASK_ID                  as pjc_task_id
,DOCUMENT_STATUS              as document_status
,TYPE_LOOKUP_CODE             as type_lookup_code
,STYLE_ID                     as style_id
,REVISION_NUM                 as revision_num
,PO_CANCEL_FLAG               as po_cancel_flag
,LINE_CANCEL_FLAG             as line_cancel_flag
,SCHEDULE_CANCEL_FLAG         as schedule_cancel_flag
,LINE_STATUS                  as line_status
,SCHEDULE_STATUS              as schedule_status
,DESTINATION_TYPE_CODE        as destination_type_code
,ACCRUE_ON_RECEIPT_FLAG       as accrue_on_receipt_flag
,MATCHING_BASIS               as matching_basis
,PO_CREATION_DATE             as po_creation_date
,PO_CREATION_DATE_KEY         as po_creation_date_key
,APPROVED_DATE                as approved_date
,APPROVED_DATE_KEY            as approved_date_key
,PO_CLOSED_DATE               as po_closed_date
,PO_CLOSED_DATE_KEY           as po_closed_date_key
,SCHEDULE_CLOSED_DATE         as schedule_closed_date
,SCHEDULE_CLOSED_DATE_KEY     as schedule_closed_date_key
,GL_CLOSED_DATE               as gl_closed_date
,GL_CLOSED_DATE_KEY           as gl_closed_date_key
,GL_CANCELLED_DATE            as gl_cancelled_date
,GL_CANCELLED_DATE_KEY        as gl_cancelled_date_key
,RATE_DATE                    as rate_date
,RATE_DATE_KEY                as rate_date_key
,BUDGET_DATE                  as budget_date
,BUDGET_DATE_KEY              as budget_date_key
,NEED_BY_DATE                 as need_by_date
,NEED_BY_DATE_KEY             as need_by_date_key
,PROMISED_DATE                as promised_date
,PROMISED_DATE_KEY            as promised_date_key
,QUANTITY_ORDERED             as quantity_ordered
,QUANTITY_DELIVERED           as quantity_delivered
,QUANTITY_BILLED              as quantity_billed
,QUANTITY_CANCELLED           as quantity_cancelled
,QUANTITY_OPEN_FOR_DELIVERY   as quantity_open_for_delivery
,QUANTITY_OPEN_FOR_BILLING    as quantity_open_for_billing
,AMOUNT_ORDERED               as amount_ordered
,AMOUNT_DELIVERED             as amount_delivered
,AMOUNT_BILLED                as amount_billed
,AMOUNT_CANCELLED             as amount_cancelled
,RATE                         as rate
,RATE_TYPE                    as rate_type
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_po_distribution', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
