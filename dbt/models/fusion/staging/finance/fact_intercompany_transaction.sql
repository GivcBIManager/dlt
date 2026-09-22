{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(line_id)'
) }}

-- finance.fact_intercompany_transaction -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: line_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(LINE_ID, 0)         as line_id
,TRX_ID                     as trx_id
,BATCH_ID                   as batch_id
,LINE_NUMBER                as line_number
,LINE_TYPE_FLAG             as line_type_flag
,INITIATOR_ORGANIZATION_ID  as initiator_organization_id
,RECIPIENT_ORGANIZATION_ID  as recipient_organization_id
,INITIATOR_LEGAL_ENTITY_ID  as initiator_legal_entity_id
,RECIPIENT_LEGAL_ENTITY_ID  as recipient_legal_entity_id
,INITIATOR_LEDGER_ID        as initiator_ledger_id
,RECIPIENT_LEDGER_ID        as recipient_ledger_id
,BATCH_NUMBER               as batch_number
,TRX_NUMBER                 as trx_number
,LINE_DESCRIPTION           as line_description
,TRX_STATUS                 as trx_status
,BATCH_STATUS               as batch_status
,REJECT_REASON              as reject_reason
,TRX_ORIGINAL_TRX_ID        as trx_original_trx_id
,TRX_REVERSED_TRX_ID        as trx_reversed_trx_id
,ORIGINAL_BATCH_ID          as original_batch_id
,REVERSED_BATCH_ID          as reversed_batch_id
,GL_DATE                    as gl_date
,GL_DATE_KEY                as gl_date_key
,BATCH_DATE                 as batch_date
,BATCH_DATE_KEY             as batch_date_key
,ENTERED_CURRENCY_CODE      as entered_currency_code
,ENTERED_INITIATOR_DR       as entered_initiator_dr
,ENTERED_INITIATOR_CR       as entered_initiator_cr
,ENTERED_INITIATOR_AMOUNT   as entered_initiator_amount
,ENTERED_RECIPIENT_DR       as entered_recipient_dr
,ENTERED_RECIPIENT_CR       as entered_recipient_cr
,ENTERED_RECIPIENT_AMOUNT   as entered_recipient_amount
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_intercompany_transaction', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
