{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(vendor_site_id)'
) }}

-- conformed.dim_supplier -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: vendor_site_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 VENDOR_ID         as vendor_id
,VENDOR_NUMBER     as vendor_number
,VENDOR_NAME       as vendor_name
,PARTY_ID          as party_id
,VENDOR_TYPE_CODE  as vendor_type_code
,SUPPLIER_STATUS   as supplier_status
,ifNull(VENDOR_SITE_ID, 0) as vendor_site_id
,VENDOR_SITE_CODE  as vendor_site_code
,BUSINESS_UNIT_ID  as business_unit_id
,PAY_GROUP_CODE    as pay_group_code
,PAYMENT_TERMS_ID  as payment_terms_id
,COUNTRY           as country
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_supplier', 'conformed') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
