{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(run_balance_id)'
) }}

-- hcm.fact_payroll_balance -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: run_balance_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(RUN_BALANCE_ID, 0)         as run_balance_id
,DEFINED_BALANCE_ID                as defined_balance_id
,PAYROLL_ACTION_ID                 as payroll_action_id
,PAYROLL_REL_ACTION_ID             as payroll_rel_action_id
,PAYROLL_RELATIONSHIP_ID           as payroll_relationship_id
,PAYROLL_TERM_ID                   as payroll_term_id
,PAYROLL_ASSIGNMENT_ID             as payroll_assignment_id
,PERSON_ID                         as person_id
,ASSIGNMENT_ID                     as assignment_id
,LEGISLATIVE_DATA_GROUP_ID         as legislative_data_group_id
,PAYROLL_ID                        as payroll_id
,LEGAL_EMPLOYER_ID                 as legal_employer_id
,TAX_UNIT_ID                       as tax_unit_id
,ELEMENT_ENTRY_ID                  as element_entry_id
,AREA1                             as area1
,AREA2                             as area2
,AREA3                             as area3
,AREA4                             as area4
,CONTEXT_VALUE1                    as context_value1
,CONTEXT_VALUE2                    as context_value2
,CONTEXT_VALUE3                    as context_value3
,CONTEXT_VALUE4                    as context_value4
,CONTEXT_VALUE5                    as context_value5
,CONTEXT_VALUE6                    as context_value6
,THIRD_PARTY_PAYEE_ID              as third_party_payee_id
,TIME_DEFINITION_ID                as time_definition_id
,CALC_BREAKDOWN_ID                 as calc_breakdown_id
,PROCESSING_SPAN                   as processing_span
,DEDUCTION_TYPE_ID                 as deduction_type_id
,ACTION_TYPE                       as action_type
,ACTION_SEQUENCE                   as action_sequence
,BALANCE_EFFECTIVE_DATE            as balance_effective_date
,BALANCE_EFFECTIVE_DATE_KEY        as balance_effective_date_key
,BALANCE_CONTEXT_DATE              as balance_context_date
,BALANCE_CONTEXT_DATE_KEY          as balance_context_date_key
,PAYROLL_ACTION_CREATION_DATE      as payroll_action_creation_date
,PAYROLL_ACTION_CREATION_DATE_KEY  as payroll_action_creation_date_key
,BALANCE_VALUE                     as balance_value
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_payroll_balance', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
