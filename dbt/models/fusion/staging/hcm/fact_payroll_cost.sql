{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(cost_id)'
) }}

-- hcm.fact_payroll_cost -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: cost_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(COST_ID, 0)                as cost_id
,PAYROLL_REL_ACTION_ID             as payroll_rel_action_id
,PAYROLL_ACTION_ID                 as payroll_action_id
,PAYROLL_RELATIONSHIP_ID           as payroll_relationship_id
,RUN_RESULT_ID                     as run_result_id
,INPUT_VALUE_ID                    as input_value_id
,COST_ALLOCATION_KEYFLEX_ID        as cost_allocation_keyflex_id
,PERSON_ID                         as person_id
,ASSIGNMENT_ID                     as assignment_id
,SOURCE_ELEMENT_TYPE_ID            as source_element_type_id
,LEGISLATIVE_DATA_GROUP_ID         as legislative_data_group_id
,TAX_UNIT_ID                       as tax_unit_id
,EARN_TIME_PERIOD_ID               as earn_time_period_id
,ACTION_TYPE                       as action_type
,BALANCE_OR_COST                   as balance_or_cost
,DEBIT_OR_CREDIT                   as debit_or_credit
,COST_TYPE                         as cost_type
,COST_STATUS                       as cost_status
,TRANSFER_TO_GL_FLAG               as transfer_to_gl_flag
,TRANSFER_TO_PROJ_FLAG             as transfer_to_proj_flag
,REVERSED_COST_ID                  as reversed_cost_id
,SOURCE_COST_ID                    as source_cost_id
,PAYROLL_ACTION_CREATION_DATE      as payroll_action_creation_date
,PAYROLL_ACTION_CREATION_DATE_KEY  as payroll_action_creation_date_key
,PAYROLL_ACTION_AS_OF_DATE         as payroll_action_as_of_date
,PAYROLL_ACTION_AS_OF_DATE_KEY     as payroll_action_as_of_date_key
,COST_START_DATE                   as cost_start_date
,COST_START_DATE_KEY               as cost_start_date_key
,COST_END_DATE                     as cost_end_date
,COST_END_DATE_KEY                 as cost_end_date_key
,COSTED_AMOUNT                     as costed_amount
,COSTED_HOURS                      as costed_hours
,CURRENCY_CODE                     as currency_code
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_payroll_cost', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
