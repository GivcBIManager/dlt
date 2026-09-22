{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(tax_line_id)'
) }}

-- finance.fact_tax_line -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: tax_line_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(TAX_LINE_ID, 0)      as tax_line_id
,APPLICATION_ID              as application_id
,ENTITY_CODE                 as entity_code
,EVENT_CLASS_CODE            as event_class_code
,TRX_ID                      as trx_id
,TRX_LINE_ID                 as trx_line_id
,TRX_LEVEL_TYPE              as trx_level_type
,TAX_REGIME_CODE             as tax_regime_code
,TAX                         as tax
,TAX_STATUS_CODE             as tax_status_code
,TAX_RATE_CODE               as tax_rate_code
,TAX_RATE                    as tax_rate
,CANCEL_FLAG                 as cancel_flag
,DELETE_FLAG                 as delete_flag
,MRC_TAX_LINE_FLAG           as mrc_tax_line_flag
,MRC_LINK_TO_TAX_LINE_ID     as mrc_link_to_tax_line_id
,OFFSET_FLAG                 as offset_flag
,OFFSET_LINK_TO_TAX_LINE_ID  as offset_link_to_tax_line_id
,SELF_ASSESSED_FLAG          as self_assessed_flag
,REPORTING_ONLY_FLAG         as reporting_only_flag
,TAX_AMT_INCLUDED_FLAG       as tax_amt_included_flag
,REVERSED_TAX_LINE_ID        as reversed_tax_line_id
,LEGAL_ENTITY_ID             as legal_entity_id
,LEDGER_ID                   as ledger_id
,TRX_DATE                    as trx_date
,TRX_DATE_KEY                as trx_date_key
,LINE_CREATION_DATE          as line_creation_date
,LINE_CREATION_DATE_KEY      as line_creation_date_key
,ENTERED_CURRENCY_CODE       as entered_currency_code
,ENTERED_TAX_AMOUNT          as entered_tax_amount
,TAX_CURRENCY_CODE           as tax_currency_code
,TAX_AMOUNT_TAX_CURR         as tax_amount_tax_curr
,LEDGER_CURRENCY_CODE        as ledger_currency_code
,ACCOUNTED_TAX_AMOUNT        as accounted_tax_amount
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_tax_line', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
