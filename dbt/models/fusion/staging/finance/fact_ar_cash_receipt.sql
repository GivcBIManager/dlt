{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(cash_receipt_id)'
) }}

-- finance.fact_ar_cash_receipt -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: cash_receipt_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(CASH_RECEIPT_ID, 0) as cash_receipt_id
,RECEIPT_NUMBER          as receipt_number
,DOC_SEQUENCE_VALUE      as doc_sequence_value
,RECEIPT_TYPE            as receipt_type
,STATUS                  as status
,CONFIRMED_FLAG          as confirmed_flag
,RECEIPT_METHOD_ID       as receipt_method_id
,RECEIPT_DATE            as receipt_date
,RECEIPT_DATE_KEY        as receipt_date_key
,DEPOSIT_DATE            as deposit_date
,DEPOSIT_DATE_KEY        as deposit_date_key
,REVERSAL_DATE           as reversal_date
,REVERSAL_DATE_KEY       as reversal_date_key
,REVERSAL_CATEGORY       as reversal_category
,REVERSAL_REASON_CODE    as reversal_reason_code
,CUSTOMER_ID             as customer_id
,CUSTOMER_SITE_USE_ID    as customer_site_use_id
,BUSINESS_UNIT_ID        as business_unit_id
,LEDGER_ID               as ledger_id
,LEGAL_ENTITY_ID         as legal_entity_id
,REMIT_BANK_ACCT_USE_ID  as remit_bank_acct_use_id
,ENTERED_CURRENCY_CODE   as entered_currency_code
,ENTERED_AMOUNT          as entered_amount
,TAX_AMOUNT              as tax_amount
,EXCHANGE_RATE_TYPE      as exchange_rate_type
,EXCHANGE_RATE           as exchange_rate
,EXCHANGE_DATE           as exchange_date
,EXCHANGE_DATE_KEY       as exchange_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_ar_cash_receipt', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
