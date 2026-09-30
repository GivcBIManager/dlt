{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(user_role_data_assignment_id)'
) }}

-- conformed.bridge_user_data_access -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: user_role_data_assignment_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(USER_ROLE_DATA_ASSIGNMENT_ID, 0) as user_role_data_assignment_id
,USER_GUID            as user_guid
,ROLE_NAME            as role_name
,BUSINESS_UNIT_ID     as business_unit_id
,LEDGER_ID            as ledger_id
,BOOK_ID              as book_id
,SET_ID               as set_id
,INV_ORGANIZATION_ID  as inv_organization_id
,CST_ORGANIZATION_ID  as cst_organization_id
,ACCESS_SET_ID        as access_set_id
,INTERCO_ORG_ID       as interco_org_id
,ACTIVE_FLAG          as active_flag
,START_DATE           as start_date
,START_DATE_KEY       as start_date_key
,END_DATE             as end_date
,END_DATE_KEY         as end_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('bridge_user_data_access', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
