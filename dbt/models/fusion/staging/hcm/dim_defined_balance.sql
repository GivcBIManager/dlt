{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(defined_balance_id)'
) }}

-- hcm.dim_defined_balance -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: defined_balance_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(DEFINED_BALANCE_ID, 0) as defined_balance_id
,BALANCE_TYPE_ID            as balance_type_id
,BALANCE_DIMENSION_ID       as balance_dimension_id
,BASE_BALANCE_NAME          as base_balance_name
,BALANCE_NAME               as balance_name
,BALANCE_REPORTING_NAME     as balance_reporting_name
,BALANCE_UOM                as balance_uom
,CURRENCY_CODE              as currency_code
,BALANCE_CATEGORY_ID        as balance_category_id
,DIMENSION_NAME             as dimension_name
,DIMENSION_DB_ITEM_SUFFIX   as dimension_db_item_suffix
,DIMENSION_PERIOD_TYPE      as dimension_period_type
,DIMENSION_LEVEL            as dimension_level
,DIMENSION_TYPE             as dimension_type
,SAVE_RUN_BALANCE           as save_run_balance
,LEGISLATIVE_DATA_GROUP_ID  as legislative_data_group_id
,LEGISLATION_CODE           as legislation_code
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_defined_balance', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
