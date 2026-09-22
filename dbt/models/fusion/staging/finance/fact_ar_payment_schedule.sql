{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(payment_schedule_id)'
) }}

-- finance.fact_ar_payment_schedule -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: payment_schedule_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(PAYMENT_SCHEDULE_ID, 0)  as payment_schedule_id
,CUSTOMER_TRX_ID                 as customer_trx_id
,CASH_RECEIPT_ID                 as cash_receipt_id
,CLASS                           as class
,STATUS                          as status
,RECEIPT_CONFIRMED_FLAG          as receipt_confirmed_flag
,CUSTOMER_ID                     as customer_id
,CUSTOMER_SITE_USE_ID            as customer_site_use_id
,BUSINESS_UNIT_ID                as business_unit_id
,LEDGER_ID                       as ledger_id
,DUE_DATE                        as due_date
,DUE_DATE_KEY                    as due_date_key
,ENTERED_CURRENCY_CODE           as entered_currency_code
,ENTERED_AMOUNT_DUE_ORIGINAL     as entered_amount_due_original
,ENTERED_AMOUNT_DUE_REMAINING    as entered_amount_due_remaining
,LEDGER_CURRENCY_CODE            as ledger_currency_code
,ACCOUNTED_AMOUNT_DUE_REMAINING  as accounted_amount_due_remaining
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_ar_payment_schedule', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
