{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(submission_id)'
) }}

-- hcm.fact_job_application -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: submission_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(SUBMISSION_ID, 0)       as submission_id
,REQUISITION_ID                 as requisition_id
,CANDIDATE_PERSON_ID            as candidate_person_id
,CURRENT_PHASE_ID               as current_phase_id
,CURRENT_STATE_ID               as current_state_id
,PUBLIC_STATE_ID                as public_state_id
,PIPELINE_SUBMISSION_ID         as pipeline_submission_id
,PROFILE_ID                     as profile_id
,SYSTEM_PERSON_TYPE             as system_person_type
,INTERNAL_FLAG                  as internal_flag
,CONFIRMED_FLAG                 as confirmed_flag
,DISQUALIFIED_FLAG              as disqualified_flag
,MERGED_FLAG                    as merged_flag
,DISCARDED_FLAG                 as discarded_flag
,ACTIVE_FLAG                    as active_flag
,IS_COMPLETED_FLAG              as is_completed_flag
,OBJECT_STATUS                  as object_status
,SUBMISSION_DATE                as submission_date
,SUBMISSION_DATE_KEY            as submission_date_key
,SUBMISSION_CONFIRMED_DATE      as submission_confirmed_date
,SUBMISSION_CONFIRMED_DATE_KEY  as submission_confirmed_date_key
,SUBMISSION_CREATION_DATE       as submission_creation_date
,SUBMISSION_CREATION_DATE_KEY   as submission_creation_date_key
,QUESTIONNAIRE_SCORE            as questionnaire_score
,CURRENT_RANK                   as current_rank
,APPLICATION_COUNT              as application_count
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_job_application', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
