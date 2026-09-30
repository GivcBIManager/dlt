{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(transaction_id, lot_number)'
) }}

-- scm.fact_inventory_transaction_lot -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: transaction_id, lot_number (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(TRANSACTION_ID, 0) as transaction_id
,ifNull(LOT_NUMBER, '') as lot_number
,INVENTORY_ITEM_ID               as inventory_item_id
,ORGANIZATION_ID                 as organization_id
,TRANSACTION_DATE                as transaction_date
,TRANSACTION_DATE_KEY            as transaction_date_key
,TRANSACTION_SOURCE_TYPE_ID      as transaction_source_type_id
,TRANSACTION_SOURCE_ID           as transaction_source_id
,TRANSACTION_SOURCE_NAME         as transaction_source_name
,PRODUCT_CODE                    as product_code
,PRODUCT_TRANSACTION_ID          as product_transaction_id
,TRANSACTION_QUANTITY            as transaction_quantity
,PRIMARY_QUANTITY                as primary_quantity
,SECONDARY_TRANSACTION_QUANTITY  as secondary_transaction_quantity
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_inventory_transaction_lot', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
