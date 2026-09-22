{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(tree_structure_code, tree_code, tree_version_id, tree_node_id)'
) }}

-- conformed.dim_account_hierarchy -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: tree_structure_code, tree_code, tree_version_id, tree_node_id (UNVERIFIED -- table is empty)
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
 ifNull(TREE_STRUCTURE_CODE, '') as tree_structure_code
,ifNull(TREE_CODE, '') as tree_code
,ifNull(TREE_VERSION_ID, '') as tree_version_id
,ifNull(TREE_NODE_ID, '') as tree_node_id
,ENTERPRISE_ID         as enterprise_id
,TREE_VERSION_NAME     as tree_version_name
,PARENT_VALUE          as parent_value
,CHILD_VALUE           as child_value
,DEPTH                 as depth
,VERSION_STATUS        as version_status
,EFFECTIVE_START_DATE  as effective_start_date
,EFFECTIVE_END_DATE    as effective_end_date
,ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_account_hierarchy', 'conformed') }}
{% if is_incremental() %}
where ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
