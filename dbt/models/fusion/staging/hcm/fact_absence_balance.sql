{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(per_accrual_entry_id)'
) }}

-- hcm.fact_absence_balance -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: per_accrual_entry_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(PER_ACCRUAL_ENTRY_ID, 0) as per_accrual_entry_id
,PER_PLAN_ENRT_ID         as per_plan_enrt_id
,PERSON_ID                as person_id
,ABSENCE_PLAN_ID          as absence_plan_id
,PERIOD_OF_SERVICE_ID     as period_of_service_id
,WORK_TERM_ASG_ID         as work_term_asg_id
,ACCRUAL_PERIOD_DATE      as accrual_period_date
,ACCRUAL_PERIOD_DATE_KEY  as accrual_period_date_key
,STATUS                   as status
,PAYROLL_STATUS           as payroll_status
,BEGIN_BALANCE            as begin_balance
,ACCRUED                  as accrued
,USED                     as used
,END_BALANCE              as end_balance
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_absence_balance', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
