{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(assignment_record_id)'
) }}

-- hcm.fact_learning_record -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: assignment_record_id (UNVERIFIED -- table is empty)
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
 ifNull(ASSIGNMENT_RECORD_ID, '') as assignment_record_id
,ASSIGNMENT_RECORD_NUMBER  as assignment_record_number
,LEARNER_PERSON_ID         as learner_person_id
,LEARNING_ITEM_ID          as learning_item_id
,EVENT_ASSIGNMENT_ID       as event_assignment_id
,ASSIGNMENT_RULE_ID        as assignment_rule_id
,RECORD_STATUS             as record_status
,RECORD_SUB_STATUS         as record_sub_status
,IS_HISTORY_FLAG           as is_history_flag
,EVENT_TYPE                as event_type
,EVENT_SUB_TYPE            as event_sub_type
,ATTRIBUTION_TYPE          as attribution_type
,ASSIGNED_ON_DATE          as assigned_on_date
,ASSIGNED_ON_DATE_KEY      as assigned_on_date_key
,COMPLETION_DATE           as completion_date
,COMPLETION_DATE_KEY       as completion_date_key
,CALCULATED_DUE_DATE       as calculated_due_date
,CALCULATED_DUE_DATE_KEY   as calculated_due_date_key
,EXPIRATION_DATE           as expiration_date
,EXPIRATION_DATE_KEY       as expiration_date_key
,VALIDITY_DATE             as validity_date
,VALIDITY_DATE_KEY         as validity_date_key
,WITHDRAWN_DATE            as withdrawn_date
,WITHDRAWN_DATE_KEY        as withdrawn_date_key
,DELETED_DATE              as deleted_date
,DELETED_DATE_KEY          as deleted_date_key
,RECORD_START_DATE         as record_start_date
,RECORD_START_DATE_KEY     as record_start_date_key
,TOTAL_LEARNING_TIME       as total_learning_time
,TOTAL_ACTUAL_EFFORT       as total_actual_effort
,EFFORT_UOM                as effort_uom
,RECORD_COUNT              as record_count
,ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_learning_record', 'hcm') }}
{% if is_incremental() %}
where ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
