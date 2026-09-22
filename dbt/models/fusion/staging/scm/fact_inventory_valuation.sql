{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(layer_cost_id)'
) }}

-- scm.fact_inventory_valuation -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: layer_cost_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(LAYER_COST_ID, 0)    as layer_cost_id
,TRANSACTION_ID              as transaction_id
,REC_TRXN_ID                 as rec_trxn_id
,DEP_TRXN_ID                 as dep_trxn_id
,TRANSACTION_COST_ID         as transaction_cost_id
,DISTRIBUTION_ID             as distribution_id
,COST_ELEMENT_ID             as cost_element_id
,EXPENSE_POOL_ID             as expense_pool_id
,COST_ORG_ID                 as cost_org_id
,COST_BOOK_ID                as cost_book_id
,INVENTORY_ORG_ID            as inventory_org_id
,INVENTORY_ITEM_ID           as inventory_item_id
,VAL_UNIT_ID                 as val_unit_id
,BASE_TXN_TYPE_ID            as base_txn_type_id
,COST_TRANSACTION_TYPE       as cost_transaction_type
,ADDITIONAL_PROCESSING_CODE  as additional_processing_code
,ABSORPTION_TYPE             as absorption_type
,POSTED_FLAG                 as posted_flag
,VAL_ONHAND_FLAG             as val_onhand_flag
,ONHAND_VALUE_STATUS_FLAG    as onhand_value_status_flag
,QUANTITY                    as quantity
,UOM_CODE                    as uom_code
,UNIT_COST                   as unit_cost
,ONHAND_QUANTITY             as onhand_quantity
,ONHAND_VALUE                as onhand_value
,CURRENCY_CODE               as currency_code
,LAYER_EFF_DATE              as layer_eff_date
,LAYER_EFF_DATE_KEY          as layer_eff_date_key
,LAYER_EFF_AS_OF_DATE        as layer_eff_as_of_date
,LAYER_EFF_AS_OF_DATE_KEY    as layer_eff_as_of_date_key
,COST_DATE                   as cost_date
,COST_DATE_KEY               as cost_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_inventory_valuation', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
