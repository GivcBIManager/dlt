{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(statement_line_id)'
) }}

-- finance.fact_bank_statement_line -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: statement_line_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(STATEMENT_LINE_ID, 0) as statement_line_id
,STATEMENT_HEADER_ID          as statement_header_id
,BANK_ACCOUNT_ID              as bank_account_id
,LINE_NUMBER                  as line_number
,STATEMENT_NUMBER             as statement_number
,STATEMENT_TYPE               as statement_type
,INTRADAY_FLAG                as intraday_flag
,TRX_TYPE                     as trx_type
,FLOW_INDICATOR               as flow_indicator
,REVERSAL_IND_FLAG            as reversal_ind_flag
,RECON_STATUS                 as recon_status
,STATEMENT_RECON_STATUS_CODE  as statement_recon_status_code
,ENTERED_CURRENCY_CODE        as entered_currency_code
,ENTERED_AMOUNT               as entered_amount
,ENTERED_RECORDED_AMOUNT      as entered_recorded_amount
,TRX_AMOUNT                   as trx_amount
,TRX_CURRENCY_CODE            as trx_currency_code
,BOOKING_DATE                 as booking_date
,BOOKING_DATE_KEY             as booking_date_key
,STATEMENT_DATE               as statement_date
,STATEMENT_DATE_KEY           as statement_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_bank_statement_line', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
