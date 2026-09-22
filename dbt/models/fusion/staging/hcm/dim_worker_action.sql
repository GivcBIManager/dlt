{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(business_group_id, action_code, action_reason_code)'
) }}

-- hcm.dim_worker_action -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: business_group_id, action_code, action_reason_code (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(BUSINESS_GROUP_ID, 0) as business_group_id
,ifNull(ACTION_CODE, '') as action_code
,ifNull(ACTION_REASON_CODE, '') as action_reason_code
,ACTION_ID           as action_id
,ACTION_REASON_ID    as action_reason_id
,ACTION_NAME         as action_name
,ACTION_REASON_NAME  as action_reason_name
,ACTION_TYPE_ID      as action_type_id
,ACTION_START_DATE   as action_start_date
,ACTION_END_DATE     as action_end_date
,USAGE_START_DATE    as usage_start_date
,USAGE_END_DATE      as usage_end_date
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_worker_action', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
