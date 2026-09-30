{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(contract_id, valid_from)'
) }}

-- hcm.dim_contract -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: contract_id, valid_from (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(CONTRACT_ID, 0) as contract_id
,PERSON_ID                 as person_id
,PERIOD_OF_SERVICE_ID      as period_of_service_id
,ASSIGNMENT_ID             as assignment_id
,CONTRACT_NUMBER           as contract_number
,CONTRACT_TYPE             as contract_type
,STATUS                    as status
,STATUS_REASON             as status_reason
,DESCRIPTION               as description
,DURATION                  as duration
,DURATION_UNITS            as duration_units
,CONTRACT_END_DATE         as contract_end_date
,CONTRACT_END_DATE_KEY     as contract_end_date_key
,LEGISLATION_CODE          as legislation_code
,CTR_INFORMATION_CATEGORY  as ctr_information_category
,CTR_INFORMATION1          as ctr_information1
,CTR_INFORMATION2          as ctr_information2
,CTR_INFORMATION3          as ctr_information3
,CTR_INFORMATION4          as ctr_information4
,CTR_INFORMATION5          as ctr_information5
,CTR_INFORMATION6          as ctr_information6
,CTR_INFORMATION7          as ctr_information7
,CTR_INFORMATION8          as ctr_information8
,CTR_INFORMATION9          as ctr_information9
,CTR_INFORMATION10         as ctr_information10
,ifNull(VALID_FROM, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as valid_from
,VALID_TO                  as valid_to
,IS_CURRENT                as is_current
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_contract', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
