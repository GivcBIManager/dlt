{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(distribution_id)'
) }}

-- finance.fact_fa_depreciation -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: distribution_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(DISTRIBUTION_ID, 0) as distribution_id
,ASSET_ID                 as asset_id
,BOOK_TYPE_CODE           as book_type_code
,PERIOD_COUNTER           as period_counter
,PERIOD_NAME              as period_name
,FISCAL_YEAR              as fiscal_year
,PERIOD_NUM               as period_num
,PERIOD_OPEN_DATE         as period_open_date
,PERIOD_OPEN_DATE_KEY     as period_open_date_key
,PERIOD_END_DATE          as period_end_date
,PERIOD_END_DATE_KEY      as period_end_date_key
,CODE_COMBINATION_ID      as code_combination_id
,ASSIGNED_TO_PERSON_ID    as assigned_to_person_id
,FA_LOCATION_ID           as fa_location_id
,BOOK_CLASS               as book_class
,LEDGER_ID                as ledger_id
,LEDGER_CURRENCY_CODE     as ledger_currency_code
,DEPRN_SOURCE_CODE        as deprn_source_code
,ACCOUNTED_DEPRN_AMOUNT   as accounted_deprn_amount
,ACCOUNTED_YTD_DEPRN      as accounted_ytd_deprn
,ACCOUNTED_DEPRN_RESERVE  as accounted_deprn_reserve
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_fa_depreciation', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
