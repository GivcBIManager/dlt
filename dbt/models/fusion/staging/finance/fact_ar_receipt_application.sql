{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(receivable_application_id)'
) }}

-- finance.fact_ar_receipt_application -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: receivable_application_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(RECEIVABLE_APPLICATION_ID, 0) as receivable_application_id
,CASH_RECEIPT_ID              as cash_receipt_id
,CUSTOMER_TRX_ID              as customer_trx_id
,APPLIED_CUSTOMER_TRX_ID      as applied_customer_trx_id
,APPLIED_PAYMENT_SCHEDULE_ID  as applied_payment_schedule_id
,PAYMENT_SCHEDULE_ID          as payment_schedule_id
,STATUS                       as status
,APPLICATION_TYPE             as application_type
,DISPLAY_FLAG                 as display_flag
,REVERSAL_GL_DATE             as reversal_gl_date
,REVERSAL_GL_DATE_KEY         as reversal_gl_date_key
,RECEIPT_STATUS               as receipt_status
,RECEIPT_NUMBER               as receipt_number
,CUSTOMER_ID                  as customer_id
,CUSTOMER_SITE_USE_ID         as customer_site_use_id
,BUSINESS_UNIT_ID             as business_unit_id
,LEDGER_ID                    as ledger_id
,BANK_ACCOUNT_ID              as bank_account_id
,GL_DATE                      as gl_date
,GL_DATE_KEY                  as gl_date_key
,APPLY_DATE                   as apply_date
,APPLY_DATE_KEY               as apply_date_key
,ENTERED_CURRENCY_CODE        as entered_currency_code
,ENTERED_AMOUNT               as entered_amount
,LEDGER_CURRENCY_CODE         as ledger_currency_code
,ACCOUNTED_AMOUNT             as accounted_amount
,APPLIED_TO_AMOUNT            as applied_to_amount
,APPLIED_TO_CURRENCY_CODE     as applied_to_currency_code
,ACCOUNTED_APPLIED_TO_AMOUNT  as accounted_applied_to_amount
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_ar_receipt_application', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
