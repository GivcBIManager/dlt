{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(code_combination_id)'
) }}

-- conformed.dim_gl_account -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: code_combination_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(CODE_COMBINATION_ID, 0) as code_combination_id
,CHART_OF_ACCOUNTS_ID  as chart_of_accounts_id
,SEGMENT1              as segment1
,SEGMENT2              as segment2
,SEGMENT3              as segment3
,SEGMENT4              as segment4
,SEGMENT5              as segment5
,SEGMENT6              as segment6
,SEGMENT7              as segment7
,SEGMENT8              as segment8
,ACCOUNT_TYPE          as account_type
,ENABLED_FLAG          as enabled_flag
,SUMMARY_FLAG          as summary_flag
,START_DATE_ACTIVE     as start_date_active
,END_DATE_ACTIVE       as end_date_active
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
,BRANCH_CODE           as branch_code
from {{ ofusion_source('dim_gl_account', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
