{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(requisition_id)'
) }}

-- hcm.fact_recruiting_requisition -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: requisition_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(REQUISITION_ID, 0)       as requisition_id
,REQUISITION_NUMBER              as requisition_number
,REQ_USAGE_CODE                  as req_usage_code
,BUSINESS_UNIT_ID                as business_unit_id
,DEPARTMENT_ID                   as department_id
,ORGANIZATION_ID                 as organization_id
,JOB_ID                          as job_id
,JOB_FAMILY_ID                   as job_family_id
,POSITION_ID                     as position_id
,GRADE_ID                        as grade_id
,LOCATION_ID                     as location_id
,LEGAL_EMPLOYER_ID               as legal_employer_id
,HIRING_MANAGER_PERSON_ID        as hiring_manager_person_id
,RECRUITER_PERSON_ID             as recruiter_person_id
,CURRENT_PHASE_ID                as current_phase_id
,CURRENT_STATE_ID                as current_state_id
,REQUISITION_TITLE               as requisition_title
,OBJECT_STATUS                   as object_status
,RECRUITING_TYPE_CODE            as recruiting_type_code
,WORKER_TYPE_CODE                as worker_type_code
,FULL_PART_TIME                  as full_part_time
,REGULAR_TEMPORARY               as regular_temporary
,HOT_JOB_FLAG                    as hot_job_flag
,UNLIMITED_HIRE_FLAG             as unlimited_hire_flag
,OPEN_DATE                       as open_date
,OPEN_DATE_KEY                   as open_date_key
,APPROVED_DATE                   as approved_date
,APPROVED_DATE_KEY               as approved_date_key
,FILLED_DATE                     as filled_date
,FILLED_DATE_KEY                 as filled_date_key
,INTERNAL_FIRST_POSTED_DATE      as internal_first_posted_date
,INTERNAL_FIRST_POSTED_DATE_KEY  as internal_first_posted_date_key
,EXTERNAL_FIRST_POSTED_DATE      as external_first_posted_date
,EXTERNAL_FIRST_POSTED_DATE_KEY  as external_first_posted_date_key
,REQUISITION_CREATION_DATE       as requisition_creation_date
,REQUISITION_CREATION_DATE_KEY   as requisition_creation_date_key
,NUMBER_TO_HIRE                  as number_to_hire
,HIRED_COUNT                     as hired_count
,SOURCING_BUDGET_AMOUNT          as sourcing_budget_amount
,TRAVEL_BUDGET_AMOUNT            as travel_budget_amount
,RELOCATION_BUDGET_AMOUNT        as relocation_budget_amount
,REFERRAL_BONUS_AMOUNT           as referral_bonus_amount
,BUDGET_CURRENCY_CODE            as budget_currency_code
,MIN_SALARY_AMOUNT               as min_salary_amount
,MAX_SALARY_AMOUNT               as max_salary_amount
,SALARY_CURRENCY_CODE            as salary_currency_code
,SALARY_FREQUENCY_CODE           as salary_frequency_code
,REQUISITION_COUNT               as requisition_count
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_recruiting_requisition', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
