{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(snapshot_date, snapshot_date_key, onhand_quantities_id)'
) }}

-- scm.fact_inventory_onhand -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: snapshot_date, snapshot_date_key, onhand_quantities_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(SNAPSHOT_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as snapshot_date
,ifNull(SNAPSHOT_DATE_KEY, 0)    as snapshot_date_key
,ifNull(ONHAND_QUANTITIES_ID, 0) as onhand_quantities_id
,INVENTORY_ITEM_ID               as inventory_item_id
,ORGANIZATION_ID                 as organization_id
,SUBINVENTORY_CODE               as subinventory_code
,LOCATOR_ID                      as locator_id
,LOT_NUMBER                      as lot_number
,REVISION                        as revision
,TRANSACTION_QUANTITY            as transaction_quantity
,PRIMARY_TRANSACTION_QUANTITY    as primary_transaction_quantity
,TRANSACTION_UOM_CODE            as transaction_uom_code
,SECONDARY_UOM_CODE              as secondary_uom_code
,SECONDARY_TRANSACTION_QUANTITY  as secondary_transaction_quantity
,OWNING_TYPE                     as owning_type
,OWNING_ENTITY_ID                as owning_entity_id
,LPN_ID                          as lpn_id
,CREATE_TRANSACTION_ID           as create_transaction_id
,UPDATE_TRANSACTION_ID           as update_transaction_id
,DATE_RECEIVED                   as date_received
,DATE_RECEIVED_KEY               as date_received_key
,ORIG_DATE_RECEIVED              as orig_date_received
,ORIG_DATE_RECEIVED_KEY          as orig_date_received_key
,AGING_ONSET_DATE                as aging_onset_date
,AGING_ONSET_DATE_KEY            as aging_onset_date_key
,AGING_EXPIRATION_DATE           as aging_expiration_date
,AGING_EXPIRATION_DATE_KEY       as aging_expiration_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_inventory_onhand', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
