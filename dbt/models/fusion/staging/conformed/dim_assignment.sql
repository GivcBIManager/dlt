{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(assignment_id, valid_from)'
) }}

-- conformed.dim_assignment -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: assignment_id, valid_from (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(ASSIGNMENT_ID, 0)     as assignment_id
,PERSON_ID                    as person_id
,ASSIGNMENT_NUMBER            as assignment_number
,ASSIGNMENT_TYPE              as assignment_type
,ASSIGNMENT_STATUS_TYPE       as assignment_status_type
,PRIMARY_FLAG                 as primary_flag
,ORGANIZATION_ID              as organization_id
,JOB_ID                       as job_id
,POSITION_ID                  as position_id
,GRADE_ID                     as grade_id
,LOCATION_ID                  as location_id
,BUSINESS_UNIT_ID             as business_unit_id
,LEGAL_EMPLOYER_ID            as legal_employer_id
,DEFAULT_CODE_COMBINATION_ID  as default_code_combination_id
,MANAGER_PERSON_ID            as manager_person_id
,BRANCH_CODE                  as branch_code
,COST_CENTER_CODE             as cost_center_code
,ifNull(VALID_FROM, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as valid_from
,VALID_TO                     as valid_to
,IS_CURRENT                   as is_current
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_assignment', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
