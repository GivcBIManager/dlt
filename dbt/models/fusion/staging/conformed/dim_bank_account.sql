{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(bank_account_id)'
) }}

-- conformed.dim_bank_account -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: bank_account_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(BANK_ACCOUNT_ID, 0) as bank_account_id
,BANK_ACCOUNT_NAME    as bank_account_name
,BANK_ACCOUNT_NUMBER  as bank_account_number
,BANK_ID              as bank_id
,BANK_NAME            as bank_name
,BANK_BRANCH_ID       as bank_branch_id
,BANK_BRANCH_NAME     as bank_branch_name
,CURRENCY_CODE        as currency_code
,LEGAL_ENTITY_ID      as legal_entity_id
,ACCOUNT_TYPE         as account_type
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_bank_account', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
