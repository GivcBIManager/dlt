{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(per_absence_entry_id)'
) }}

-- hcm.fact_absence_entry -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: per_absence_entry_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(PER_ABSENCE_ENTRY_ID, 0) as per_absence_entry_id
,ENTERPRISE_ID              as enterprise_id
,PERSON_ID                  as person_id
,ASSIGNMENT_ID              as assignment_id
,ABSENCE_TYPE_ID            as absence_type_id
,ABSENCE_TYPE_REASON_ID     as absence_type_reason_id
,PERIOD_OF_SERVICE_ID       as period_of_service_id
,LEGAL_EMPLOYER_ID          as legal_employer_id
,ABSENCE_CASE_ID            as absence_case_id
,ABSENCE_STATUS_CODE        as absence_status_code
,APPROVAL_STATUS_CODE       as approval_status_code
,PROCESSING_STATUS          as processing_status
,OPEN_ENDED_FLAG            as open_ended_flag
,SINGLE_DAY_FLAG            as single_day_flag
,ABSENCE_RECORD_SOURCE      as absence_record_source
,ABSENCE_START_DATE         as absence_start_date
,ABSENCE_START_DATE_KEY     as absence_start_date_key
,ABSENCE_END_DATE           as absence_end_date
,ABSENCE_END_DATE_KEY       as absence_end_date_key
,PLANNED_END_DATE           as planned_end_date
,PLANNED_END_DATE_KEY       as planned_end_date_key
,SUBMITTED_DATE             as submitted_date
,SUBMITTED_DATE_KEY         as submitted_date_key
,CONFIRMED_DATE             as confirmed_date
,CONFIRMED_DATE_KEY         as confirmed_date_key
,ABSENCE_CREATION_DATE      as absence_creation_date
,ABSENCE_CREATION_DATE_KEY  as absence_creation_date_key
,DURATION                   as duration
,DURATION_UOM               as duration_uom
,ABSENCE_COUNT              as absence_count
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_absence_entry', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
