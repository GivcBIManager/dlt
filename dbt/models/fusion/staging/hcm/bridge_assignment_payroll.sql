{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(assigned_payroll_id)'
) }}

-- hcm.bridge_assignment_payroll -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: assigned_payroll_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 RELATIONSHIP_GROUP_ID    as relationship_group_id
,ifNull(ASSIGNED_PAYROLL_ID, 0) as assigned_payroll_id
,ASSIGNMENT_ID            as assignment_id
,PAYROLL_TERM_ID          as payroll_term_id
,PAYROLL_RELATIONSHIP_ID  as payroll_relationship_id
,LEGAL_EMPLOYER_ID        as legal_employer_id
,PAYROLL_ID               as payroll_id
,GROUP_START_DATE         as group_start_date
,GROUP_START_DATE_KEY     as group_start_date_key
,GROUP_END_DATE           as group_end_date
,GROUP_END_DATE_KEY       as group_end_date_key
,PAYROLL_START_DATE       as payroll_start_date
,PAYROLL_START_DATE_KEY   as payroll_start_date_key
,PAYROLL_END_DATE         as payroll_end_date
,PAYROLL_END_DATE_KEY     as payroll_end_date_key
,LSED_DATE                as lsed_date
,LSED_DATE_KEY            as lsed_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('bridge_assignment_payroll', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
