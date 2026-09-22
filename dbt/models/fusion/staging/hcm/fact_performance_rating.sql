{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(eval_rating_id)'
) }}

-- hcm.fact_performance_rating -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: eval_rating_id (UNVERIFIED -- table is empty)
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
 ifNull(EVAL_RATING_ID, '')    as eval_rating_id
,EVALUATION_ID                 as evaluation_id
,BUSINESS_GROUP_ID             as business_group_id
,REFERENCE_TYPE                as reference_type
,REFERENCE_ID                  as reference_id
,ROLE_TYPE_CODE                as role_type_code
,EVAL_PARTICIPANT_ID           as eval_participant_id
,PERFORMANCE_RATING_LEVEL_ID   as performance_rating_level_id
,PROFICIENCY_RATING_LEVEL_ID   as proficiency_rating_level_id
,CALCULATED_RATING             as calculated_rating
,REVIEW_POINTS                 as review_points
,WORKER_PERSON_ID              as worker_person_id
,ASSIGNMENT_ID                 as assignment_id
,MANAGER_PERSON_ID             as manager_person_id
,MANAGER_ASSIGNMENT_ID         as manager_assignment_id
,TEMPLATE_TYPE_CODE            as template_type_code
,TMPL_PERIOD_ID                as tmpl_period_id
,REVIEW_PERIOD_ID              as review_period_id
,EVALUATION_STATUS_CODE        as evaluation_status_code
,EVALUATION_START_DATE         as evaluation_start_date
,EVALUATION_START_DATE_KEY     as evaluation_start_date_key
,EVALUATION_END_DATE           as evaluation_end_date
,EVALUATION_END_DATE_KEY       as evaluation_end_date_key
,EVALUATION_DATE               as evaluation_date
,EVALUATION_DATE_KEY           as evaluation_date_key
,EVALUATION_CREATION_DATE      as evaluation_creation_date
,EVALUATION_CREATION_DATE_KEY  as evaluation_creation_date_key
,RATING_COUNT                  as rating_count
,ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'))  as last_update_date
from {{ ofusion_source('fact_performance_rating', 'hcm') }}
{% if is_incremental() %}
where ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
