{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(line_type_id)'
) }}

-- scm.dim_po_line_type -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: line_type_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(LINE_TYPE_ID, 0) as line_type_id
,LINE_TYPE_CODE          as line_type_code
,LINE_TYPE_NAME          as line_type_name
,DESCRIPTION             as description
,ORDER_TYPE_LOOKUP_CODE  as order_type_lookup_code
,PURCHASE_BASIS          as purchase_basis
,MATCHING_BASIS          as matching_basis
,PRODUCT_TYPE            as product_type
,RECEIVING_FLAG          as receiving_flag
,CREDIT_FLAG             as credit_flag
,INACTIVE_DATE           as inactive_date
,INACTIVE_DATE_KEY       as inactive_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_po_line_type', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
