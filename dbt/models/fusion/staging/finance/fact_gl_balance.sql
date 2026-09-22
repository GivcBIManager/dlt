{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(code_combination_id, period_set_name, period_type, period_name)'
) }}

-- finance.fact_gl_balance -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: code_combination_id, period_set_name, period_type, period_name (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 LEDGER_ID               as ledger_id
,ifNull(CODE_COMBINATION_ID, 0) as code_combination_id
,ifNull(PERIOD_SET_NAME, '') as period_set_name
,ifNull(PERIOD_TYPE, '') as period_type
,ifNull(PERIOD_NAME, '') as period_name
,PERIOD_START_DATE       as period_start_date
,PERIOD_START_DATE_KEY   as period_start_date_key
,ACTUAL_FLAG             as actual_flag
,BUDGET_VERSION_ID       as budget_version_id
,ENCUMBRANCE_TYPE_ID     as encumbrance_type_id
,TRANSLATED_FLAG         as translated_flag
,CURRENCY_BALANCE_TYPE   as currency_balance_type
,ENTERED_CURRENCY_CODE   as entered_currency_code
,ENTERED_DR              as entered_dr
,ENTERED_CR              as entered_cr
,ENTERED_AMOUNT          as entered_amount
,ENTERED_BEGIN_DR        as entered_begin_dr
,ENTERED_BEGIN_CR        as entered_begin_cr
,ENTERED_BEGIN_AMOUNT    as entered_begin_amount
,LEDGER_CURRENCY_CODE    as ledger_currency_code
,ACCOUNTED_DR            as accounted_dr
,ACCOUNTED_CR            as accounted_cr
,ACCOUNTED_AMOUNT        as accounted_amount
,ACCOUNTED_BEGIN_DR      as accounted_begin_dr
,ACCOUNTED_BEGIN_CR      as accounted_begin_cr
,ACCOUNTED_BEGIN_AMOUNT  as accounted_begin_amount
,CONVERTED_DR            as converted_dr
,CONVERTED_CR            as converted_cr
,CONVERTED_AMOUNT        as converted_amount
,CONVERTED_BEGIN_DR      as converted_begin_dr
,CONVERTED_BEGIN_CR      as converted_begin_cr
,CONVERTED_BEGIN_AMOUNT  as converted_begin_amount
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_gl_balance', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
