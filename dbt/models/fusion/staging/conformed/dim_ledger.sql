{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(ledger_id)'
) }}

-- conformed.dim_ledger -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: ledger_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(LEDGER_ID, 0)      as ledger_id
,LEDGER_NAME               as ledger_name
,LEDGER_SHORT_NAME         as ledger_short_name
,LEDGER_CATEGORY_CODE      as ledger_category_code
,CURRENCY_CODE             as currency_code
,CHART_OF_ACCOUNTS_ID      as chart_of_accounts_id
,PERIOD_SET_NAME           as period_set_name
,ACCOUNTED_PERIOD_TYPE     as accounted_period_type
,FIRST_LEDGER_PERIOD_NAME  as first_ledger_period_name
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_ledger', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
