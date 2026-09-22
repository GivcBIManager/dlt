{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(assignment_id, effective_end_date_key, effective_sequence)'
) }}

-- hcm.fact_worker_movement -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: assignment_id, effective_end_date_key, effective_sequence (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(ASSIGNMENT_ID, 0)         as assignment_id
,EFFECTIVE_START_DATE             as effective_start_date
,EFFECTIVE_START_DATE_KEY         as effective_start_date_key
,EFFECTIVE_END_DATE               as effective_end_date
,ifNull(EFFECTIVE_END_DATE_KEY, 0) as effective_end_date_key
,ifNull(EFFECTIVE_SEQUENCE, 0)    as effective_sequence
,EFFECTIVE_LATEST_CHANGE          as effective_latest_change
,PERSON_ID                        as person_id
,BUSINESS_GROUP_ID                as business_group_id
,ASSIGNMENT_TYPE                  as assignment_type
,ASSIGNMENT_STATUS_TYPE           as assignment_status_type
,PRIMARY_FLAG                     as primary_flag
,PERIOD_OF_SERVICE_ID             as period_of_service_id
,ACTION_CODE                      as action_code
,ACTION_REASON_CODE               as action_reason_code
,ACTION_OCCURRENCE_ID             as action_occurrence_id
,ACTION_TYPE_CODE                 as action_type_code
,ACTION_DATE                      as action_date
,ACTION_DATE_KEY                  as action_date_key
,ORGANIZATION_ID                  as organization_id
,JOB_ID                           as job_id
,POSITION_ID                      as position_id
,GRADE_ID                         as grade_id
,LOCATION_ID                      as location_id
,PREVIOUS_ORGANIZATION_ID         as previous_organization_id
,PREVIOUS_JOB_ID                  as previous_job_id
,PREVIOUS_POSITION_ID             as previous_position_id
,PREVIOUS_GRADE_ID                as previous_grade_id
,PREVIOUS_LOCATION_ID             as previous_location_id
,PREVIOUS_ASSIGNMENT_STATUS_TYPE  as previous_assignment_status_type
,ORGANIZATION_CHANGED_FLAG        as organization_changed_flag
,JOB_CHANGED_FLAG                 as job_changed_flag
,POSITION_CHANGED_FLAG            as position_changed_flag
,GRADE_CHANGED_FLAG               as grade_changed_flag
,LOCATION_CHANGED_FLAG            as location_changed_flag
,MOVEMENT_COUNT                   as movement_count
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_worker_movement', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
