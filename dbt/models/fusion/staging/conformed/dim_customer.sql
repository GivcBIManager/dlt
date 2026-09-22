{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(party_type, cust_acct_site_id)'
) }}

-- conformed.dim_customer -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: party_type, cust_acct_site_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 CUST_ACCOUNT_ID    as cust_account_id
,ACCOUNT_NUMBER     as account_number
,PARTY_ID           as party_id
,PARTY_NAME         as party_name
,ifNull(PARTY_TYPE, '') as party_type
,ifNull(CUST_ACCT_SITE_ID, 0) as cust_acct_site_id
,SITE_USE_ID        as site_use_id
,SITE_USE_CODE      as site_use_code
,LOCATION           as location
,BILL_TO_FLAG       as bill_to_flag
,SHIP_TO_FLAG       as ship_to_flag
,STATUS             as status
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_customer', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
