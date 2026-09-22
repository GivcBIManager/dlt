{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(invoice_payment_id)'
) }}

-- finance.fact_ap_payment -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: invoice_payment_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(INVOICE_PAYMENT_ID, 0) as invoice_payment_id
,INVOICE_ID             as invoice_id
,CHECK_ID               as check_id
,PAYMENT_NUM            as payment_num
,CHECK_NUMBER           as check_number
,PAYMENT_METHOD_CODE    as payment_method_code
,PAYMENT_STATUS         as payment_status
,VOID_DATE              as void_date
,VOID_DATE_KEY          as void_date_key
,POSTED_FLAG            as posted_flag
,REVERSAL_FLAG          as reversal_flag
,REVERSAL_INV_PMT_ID    as reversal_inv_pmt_id
,VENDOR_ID              as vendor_id
,VENDOR_SITE_ID         as vendor_site_id
,BUSINESS_UNIT_ID       as business_unit_id
,LEDGER_ID              as ledger_id
,BANK_ACCOUNT_ID        as bank_account_id
,ACCOUNTING_DATE        as accounting_date
,ACCOUNTING_DATE_KEY    as accounting_date_key
,ENTERED_CURRENCY_CODE  as entered_currency_code
,ENTERED_AMOUNT         as entered_amount
,LEDGER_CURRENCY_CODE   as ledger_currency_code
,ACCOUNTED_AMOUNT       as accounted_amount
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_ap_payment', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
