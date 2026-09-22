{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(assign_work_measure_id, action_occurrence_id)'
) }}

-- hcm.fact_assignment_work_measure -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: assign_work_measure_id, action_occurrence_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(ASSIGN_WORK_MEASURE_ID, 0) as assign_work_measure_id
,ASSIGNMENT_ID             as assignment_id
,UNIT                      as unit
,WORK_MEASURE_VALUE        as work_measure_value
,ADDS_TO_BUDGET            as adds_to_budget
,ifNull(ACTION_OCCURRENCE_ID, 0) as action_occurrence_id
,LEGISLATION_CODE          as legislation_code
,EFFECTIVE_START_DATE      as effective_start_date
,EFFECTIVE_START_DATE_KEY  as effective_start_date_key
,EFFECTIVE_END_DATE        as effective_end_date
,EFFECTIVE_END_DATE_KEY    as effective_end_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_assignment_work_measure', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
