{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(asg_responsibility_id)'
) }}

-- hcm.fact_area_of_responsibility -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: asg_responsibility_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(ASG_RESPONSIBILITY_ID, 0) as asg_responsibility_id
,PERSON_ID            as person_id
,ASSIGNMENT_ID        as assignment_id
,RESPONSIBILITY_TYPE  as responsibility_type
,RESPONSIBILITY_NAME  as responsibility_name
,USAGE                as usage
,STATUS               as status
,START_DATE           as start_date
,START_DATE_KEY       as start_date_key
,END_DATE             as end_date
,END_DATE_KEY         as end_date_key
,BUSINESS_UNIT_ID     as business_unit_id
,LEGAL_ENTITY_ID      as legal_entity_id
,ORGANIZATION_ID      as organization_id
,LOCATION_ID          as location_id
,POSITION_ID          as position_id
,JOB_ID               as job_id
,GRADE_ID             as grade_id
,PAYROLL_ID           as payroll_id
,COUNTRY              as country
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_area_of_responsibility', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
