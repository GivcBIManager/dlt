{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(customer_trx_line_id)'
) }}

-- finance.fact_ar_invoice_line -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: customer_trx_line_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(CUSTOMER_TRX_LINE_ID, 0) as customer_trx_line_id
,CUSTOMER_TRX_ID        as customer_trx_id
,TRX_NUMBER             as trx_number
,LINE_NUMBER            as line_number
,LINE_TYPE              as line_type
,COMPLETE_FLAG          as complete_flag
,STATUS_TRX             as status_trx
,BILL_TO_CUSTOMER_ID    as bill_to_customer_id
,BILL_TO_SITE_USE_ID    as bill_to_site_use_id
,BUSINESS_UNIT_ID       as business_unit_id
,LEDGER_ID              as ledger_id
,LEGAL_ENTITY_ID        as legal_entity_id
,TRX_DATE               as trx_date
,TRX_DATE_KEY           as trx_date_key
,GL_DATE                as gl_date
,GL_DATE_KEY            as gl_date_key
,ENTERED_CURRENCY_CODE  as entered_currency_code
,ENTERED_AMOUNT         as entered_amount
,LEDGER_CURRENCY_CODE   as ledger_currency_code
,ACCOUNTED_AMOUNT       as accounted_amount
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_ar_invoice_line', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
