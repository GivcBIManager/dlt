{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(period_of_service_id)'
) }}

-- hcm.fact_period_of_service -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: period_of_service_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(PERIOD_OF_SERVICE_ID, 0) as period_of_service_id
,PERSON_ID                       as person_id
,LEGAL_EMPLOYER_ID               as legal_employer_id
,BUSINESS_GROUP_ID               as business_group_id
,PERIOD_TYPE                     as period_type
,PRIMARY_FLAG                    as primary_flag
,WORKER_NUMBER                   as worker_number
,ACTION_OCCURRENCE_ID            as action_occurrence_id
,REHIRE_RECOMMENDATION           as rehire_recommendation
,START_DATE                      as start_date
,START_DATE_KEY                  as start_date_key
,ACTUAL_TERMINATION_DATE         as actual_termination_date
,ACTUAL_TERMINATION_DATE_KEY     as actual_termination_date_key
,ACCEPTED_TERMINATION_DATE       as accepted_termination_date
,ACCEPTED_TERMINATION_DATE_KEY   as accepted_termination_date_key
,NOTIFIED_TERMINATION_DATE       as notified_termination_date
,NOTIFIED_TERMINATION_DATE_KEY   as notified_termination_date_key
,PROJECTED_TERMINATION_DATE      as projected_termination_date
,PROJECTED_TERMINATION_DATE_KEY  as projected_termination_date_key
,LAST_WORKING_DATE               as last_working_date
,LAST_WORKING_DATE_KEY           as last_working_date_key
,ORIGINAL_DATE_OF_HIRE           as original_date_of_hire
,ORIGINAL_DATE_OF_HIRE_KEY       as original_date_of_hire_key
,ADJUSTED_SERVICE_DATE           as adjusted_service_date
,ADJUSTED_SERVICE_DATE_KEY       as adjusted_service_date_key
,TERMINATED_FLAG                 as terminated_flag
,TENURE_DAYS                     as tenure_days
,PERIOD_COUNT                    as period_count
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_period_of_service', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
