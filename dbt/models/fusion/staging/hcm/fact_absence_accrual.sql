{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(per_accrual_entry_dtl_id)'
) }}

-- hcm.fact_absence_accrual -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: per_accrual_entry_dtl_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(PER_ACCRUAL_ENTRY_DTL_ID, 0) as per_accrual_entry_dtl_id
,PER_ACCRUAL_ENTRY_ID       as per_accrual_entry_id
,PERSON_ID                  as person_id
,ASSIGNMENT_ID              as assignment_id
,ABSENCE_PLAN_ID            as absence_plan_id
,LEGAL_EMPLOYER_ID          as legal_employer_id
,PER_ABSENCE_ENTRY_ID       as per_absence_entry_id
,PER_PLAN_ENRT_ID           as per_plan_enrt_id
,ACCRUAL_TYPE               as accrual_type
,ADJUSTMENT_REASON          as adjustment_reason
,VOIDED_ACCRUAL             as voided_accrual
,ACCRUAL_STATUS             as accrual_status
,APPROVAL_STATUS_CODE       as approval_status_code
,SOURCE                     as source
,PROCESSED_DATE             as processed_date
,PROCESSED_DATE_KEY         as processed_date_key
,EXPIRATION_DATE            as expiration_date
,EXPIRATION_DATE_KEY        as expiration_date_key
,ACCRUAL_CREATION_DATE      as accrual_creation_date
,ACCRUAL_CREATION_DATE_KEY  as accrual_creation_date_key
,ACCRUAL_AS_OF_DATE         as accrual_as_of_date
,ACCRUAL_AS_OF_DATE_KEY     as accrual_as_of_date_key
,ACCRUAL_VALUE              as accrual_value
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_absence_accrual', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
