{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(invoice_distribution_id)'
) }}

-- finance.fact_ap_invoice_distribution -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: invoice_distribution_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(INVOICE_DISTRIBUTION_ID, 0) as invoice_distribution_id
,INVOICE_ID                as invoice_id
,INVOICE_NUM               as invoice_num
,INVOICE_LINE_NUMBER       as invoice_line_number
,DISTRIBUTION_LINE_NUMBER  as distribution_line_number
,LINE_TYPE_LOOKUP_CODE     as line_type_lookup_code
,PO_DISTRIBUTION_ID        as po_distribution_id
,RCV_TRANSACTION_ID        as rcv_transaction_id
,POSTED_FLAG               as posted_flag
,MATCH_STATUS_FLAG         as match_status_flag
,REVERSAL_FLAG             as reversal_flag
,PARENT_REVERSAL_ID        as parent_reversal_id
,XINV_PARENT_REVERSAL_ID   as xinv_parent_reversal_id
,CANCELLATION_FLAG         as cancellation_flag
,INVOICE_TYPE_LOOKUP_CODE  as invoice_type_lookup_code
,VENDOR_ID                 as vendor_id
,VENDOR_SITE_ID            as vendor_site_id
,BUSINESS_UNIT_ID          as business_unit_id
,LEDGER_ID                 as ledger_id
,CODE_COMBINATION_ID       as code_combination_id
,LEGAL_ENTITY_ID           as legal_entity_id
,INVOICE_DATE              as invoice_date
,INVOICE_DATE_KEY          as invoice_date_key
,ACCOUNTING_DATE           as accounting_date
,ACCOUNTING_DATE_KEY       as accounting_date_key
,ENTERED_CURRENCY_CODE     as entered_currency_code
,ENTERED_AMOUNT            as entered_amount
,LEDGER_CURRENCY_CODE      as ledger_currency_code
,ACCOUNTED_AMOUNT          as accounted_amount
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_ap_invoice_distribution', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
