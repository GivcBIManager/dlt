{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(run_result_id, input_value_id)'
) }}

-- hcm.fact_payroll_run_result -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: run_result_id, input_value_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(RUN_RESULT_ID, 0)          as run_result_id
,ifNull(INPUT_VALUE_ID, 0)         as input_value_id
,ELEMENT_TYPE_ID                   as element_type_id
,PAYROLL_ACTION_ID                 as payroll_action_id
,PAYROLL_REL_ACTION_ID             as payroll_rel_action_id
,PAYROLL_RELATIONSHIP_ID           as payroll_relationship_id
,PERSON_ID                         as person_id
,ASSIGNMENT_ID                     as assignment_id
,PAYROLL_ID                        as payroll_id
,LEGAL_EMPLOYER_ID                 as legal_employer_id
,TAX_UNIT_ID                       as tax_unit_id
,EARN_TIME_PERIOD_ID               as earn_time_period_id
,ACTION_TYPE                       as action_type
,PAYROLL_ACTION_STATUS             as payroll_action_status
,REL_ACTION_STATUS                 as rel_action_status
,ACTION_SEQUENCE                   as action_sequence
,RESULT_STATUS                     as result_status
,SOURCE_TYPE                       as source_type
,ENTRY_TYPE                        as entry_type
,ELEMENT_ENTRY_ID                  as element_entry_id
,PAYROLL_ACTION_CREATION_DATE      as payroll_action_creation_date
,PAYROLL_ACTION_CREATION_DATE_KEY  as payroll_action_creation_date_key
,PAYROLL_ACTION_AS_OF_DATE         as payroll_action_as_of_date
,PAYROLL_ACTION_AS_OF_DATE_KEY     as payroll_action_as_of_date_key
,PAYROLL_EFFECTIVE_DATE            as payroll_effective_date
,PAYROLL_EFFECTIVE_DATE_KEY        as payroll_effective_date_key
,DATE_EARNED                       as date_earned
,DATE_EARNED_KEY                   as date_earned_key
,RESULT_START_DATE                 as result_start_date
,RESULT_START_DATE_KEY             as result_start_date_key
,RESULT_END_DATE                   as result_end_date
,RESULT_END_DATE_KEY               as result_end_date_key
,RESULT_VALUE                      as result_value
,RESULT_NUMBER                     as result_number
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_payroll_run_result', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
