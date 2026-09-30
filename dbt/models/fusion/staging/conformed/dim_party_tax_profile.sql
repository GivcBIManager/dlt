{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(party_tax_profile_id)'
) }}

-- conformed.dim_party_tax_profile -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: party_tax_profile_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(PARTY_TAX_PROFILE_ID, 0) as party_tax_profile_id
,PARTY_ID                 as party_id
,PARTY_TYPE_CODE          as party_type_code
,CUSTOMER_FLAG            as customer_flag
,SUPPLIER_FLAG            as supplier_flag
,SITE_FLAG                as site_flag
,REP_REGISTRATION_NUMBER  as rep_registration_number
,REGISTRATION_TYPE_CODE   as registration_type_code
,COUNTRY_CODE             as country_code
,MERGED_TO_PTP_ID         as merged_to_ptp_id
,MERGED_STATUS_CODE       as merged_status_code
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_party_tax_profile', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
