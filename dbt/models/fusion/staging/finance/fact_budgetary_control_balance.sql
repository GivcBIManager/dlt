{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(code_combination_id, period_name)'
) }}

-- finance.fact_budgetary_control_balance -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: code_combination_id, period_name (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 CONTROL_BUDGET_ID           as control_budget_id
,ifNull(CODE_COMBINATION_ID, 0) as code_combination_id
,PERIOD_SET_NAME             as period_set_name
,PERIOD_TYPE                 as period_type
,ifNull(PERIOD_NAME, '')     as period_name
,CONTROL_BUDGET_NAME         as control_budget_name
,CONTROL_BUDGET_STATUS_CODE  as control_budget_status_code
,LEDGER_ID                   as ledger_id
,PROJECT_ID                  as project_id
,CURRENCY_CODE               as currency_code
,BUDGET_AMOUNT               as budget_amount
,COMMITMENT_AMOUNT           as commitment_amount
,OBLIGATION_AMOUNT           as obligation_amount
,OTHER_AMOUNT                as other_amount
,ACTUAL_AMOUNT               as actual_amount
,FUNDS_AVAILABLE_AMOUNT      as funds_available_amount
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_budgetary_control_balance', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
