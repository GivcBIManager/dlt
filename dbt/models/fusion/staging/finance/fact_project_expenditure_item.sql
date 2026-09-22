{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(expenditure_item_id)'
) }}

-- finance.fact_project_expenditure_item -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: expenditure_item_id (UNVERIFIED -- table is empty)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
-- LAST_UPDATE_DATE also arrives as a STRING from this (empty) table, and
-- ReplacingMergeTree will not take a String version column, so it is parsed to
-- DateTime64(6) here -- the same type the populated models already expose.
--
-- The warehouse table is STILL EMPTY, so this grain could not be measured: the
-- sorting key is inferred from the Fusion table's natural key, and is chosen
-- deliberately WIDE. Under ReplacingMergeTree a key that is too wide only
-- skips dedup, while one that is too narrow silently collapses distinct rows.
-- The `unique_combination_final` test on this key in _ofusion__models.yml
-- fails the run as soon as real data contradicts the guess.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(EXPENDITURE_ITEM_ID, '')   as expenditure_item_id
,PROJECT_ID                        as project_id
,TASK_ID                           as task_id
,BUSINESS_UNIT_ID                  as business_unit_id
,EXPENDITURE_ORGANIZATION_ID       as expenditure_organization_id
,INCURRED_BY_PERSON_ID             as incurred_by_person_id
,VENDOR_ID                         as vendor_id
,EXPENDITURE_TYPE_ID               as expenditure_type_id
,SYSTEM_LINKAGE_FUNCTION           as system_linkage_function
,TRANSACTION_SOURCE_ID             as transaction_source_id
,ORIG_TRANSACTION_REFERENCE        as orig_transaction_reference
,BILLABLE_FLAG                     as billable_flag
,CAPITALIZABLE_FLAG                as capitalizable_flag
,ADJUSTED_EXPENDITURE_ITEM_ID      as adjusted_expenditure_item_id
,NET_ZERO_ADJUSTMENT_FLAG          as net_zero_adjustment_flag
,EXPENDITURE_ITEM_DATE             as expenditure_item_date
,EXPENDITURE_ITEM_DATE_KEY         as expenditure_item_date_key
,QUANTITY                          as quantity
,ENTERED_CURRENCY_CODE             as entered_currency_code
,ENTERED_RAW_COST                  as entered_raw_cost
,ENTERED_BURDENED_COST             as entered_burdened_cost
,LEDGER_CURRENCY_CODE              as ledger_currency_code
,ACCOUNTED_RAW_COST                as accounted_raw_cost
,ACCOUNTED_BURDENED_COST           as accounted_burdened_cost
,PROJECT_CURRENCY_CODE             as project_currency_code
,PROJECT_RAW_COST                  as project_raw_cost
,PROJECT_BURDENED_COST             as project_burdened_cost
,PROJECT_FUNCTIONAL_CURRENCY_CODE  as project_functional_currency_code
,PROJECT_FUNCTIONAL_RAW_COST       as project_functional_raw_cost
,PROJECT_FUNCTIONAL_BURDENED_COST  as project_functional_burdened_cost
,ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'))      as last_update_date
from {{ ofusion_source('fact_project_expenditure_item', 'finance') }}
{% if is_incremental() %}
where ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
