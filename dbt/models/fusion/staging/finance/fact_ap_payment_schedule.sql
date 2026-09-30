{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(invoice_id)'
) }}

-- finance.fact_ap_payment_schedule -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: invoice_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(INVOICE_ID, 0)     as invoice_id
,PAYMENT_NUM                 as payment_num
,VENDOR_ID                   as vendor_id
,VENDOR_SITE_ID              as vendor_site_id
,PARTY_ID                    as party_id
,INVOICE_NUM                 as invoice_num
,INVOICE_TYPE_LOOKUP_CODE    as invoice_type_lookup_code
,INVOICE_APPROVAL_STATUS     as invoice_approval_status
,INVOICE_PAYMENT_STATUS_FLAG as invoice_payment_status_flag
,INVOICE_DATE                as invoice_date
,INVOICE_DATE_KEY            as invoice_date_key
,CANCELLED_DATE              as cancelled_date
,CANCELLED_DATE_KEY          as cancelled_date_key
,BUSINESS_UNIT_ID            as business_unit_id
,DUE_DATE                    as due_date
,DUE_DATE_KEY                as due_date_key
,ENTERED_CURRENCY_CODE       as entered_currency_code
,ENTERED_GROSS_AMOUNT        as entered_gross_amount
,ENTERED_AMOUNT_REMAINING    as entered_amount_remaining
,PAYMENT_STATUS_FLAG         as payment_status_flag
,HOLD_FLAG                   as hold_flag
,INVOICE_CURRENCY_CODE       as invoice_currency_code
,INVOICE_EXCHANGE_RATE       as invoice_exchange_rate
,PAYMENT_CROSS_RATE          as payment_cross_rate
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_ap_payment_schedule', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
