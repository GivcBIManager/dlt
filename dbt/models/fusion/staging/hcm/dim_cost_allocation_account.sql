{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(cost_allocation_keyflex_id)'
) }}

-- hcm.dim_cost_allocation_account -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: cost_allocation_keyflex_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(COST_ALLOCATION_KEYFLEX_ID, 0) as cost_allocation_keyflex_id
,ID_FLEX_NUM                 as id_flex_num
,CONCATENATED_SEGMENTS       as concatenated_segments
,ENABLED_FLAG                as enabled_flag
,COMBINATION_START_DATE      as combination_start_date
,COMBINATION_END_DATE        as combination_end_date
,BRANCH_CODE                 as branch_code
,COST_CENTER_CODE            as cost_center_code
,SEGMENT1                    as segment1
,SEGMENT2                    as segment2
,SEGMENT3                    as segment3
,SEGMENT4                    as segment4
,SEGMENT5                    as segment5
,SEGMENT6                    as segment6
,SEGMENT7                    as segment7
,SEGMENT8                    as segment8
,SEGMENT9                    as segment9
,SEGMENT10                   as segment10
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_cost_allocation_account', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
