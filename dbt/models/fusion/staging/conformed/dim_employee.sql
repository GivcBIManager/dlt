{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(person_id, valid_from)'
) }}

-- conformed.dim_employee -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: person_id, valid_from (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(PERSON_ID, 0)        as person_id
,PERSON_NUMBER               as person_number
,FULL_NAME                   as full_name
,FIRST_NAME                  as first_name
,LAST_NAME                   as last_name
,DATE_OF_BIRTH               as date_of_birth
,GENDER                      as gender
,MARITAL_STATUS              as marital_status
,EMAIL_ADDRESS               as email_address
,PHONE_TYPE                  as phone_type
,PHONE_NUMBER                as phone_number
,NATIONAL_IDENTIFIER_NUMBER  as national_identifier_number
,NATIONAL_IDENTIFIER_TYPE    as national_identifier_type
,WORKER_TYPE                 as worker_type
,RELIGION                    as religion
,HIRE_DATE                   as hire_date
,ACTUAL_TERMINATION_DATE     as actual_termination_date
,LAST_WORKING_DATE           as last_working_date
,LEGAL_EMPLOYER_ID           as legal_employer_id
,ifNull(VALID_FROM, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as valid_from
,VALID_TO                    as valid_to
,IS_CURRENT                  as is_current
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_employee', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
