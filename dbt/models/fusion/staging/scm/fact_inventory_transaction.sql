{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(transaction_id)'
) }}

-- scm.fact_inventory_transaction -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: transaction_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(TRANSACTION_ID, 0)       as transaction_id
,PARENT_TRANSACTION_ID           as parent_transaction_id
,TRANSFER_TRANSACTION_ID         as transfer_transaction_id
,TRANSACTION_SET_ID              as transaction_set_id
,RCV_TRANSACTION_ID              as rcv_transaction_id
,INVENTORY_ITEM_ID               as inventory_item_id
,REVISION                        as revision
,ORGANIZATION_ID                 as organization_id
,SUBINVENTORY_CODE               as subinventory_code
,LOCATOR_ID                      as locator_id
,TRANSFER_ORGANIZATION_ID        as transfer_organization_id
,TRANSFER_SUBINVENTORY           as transfer_subinventory
,TRANSFER_LOCATOR_ID             as transfer_locator_id
,TRANSACTION_TYPE_ID             as transaction_type_id
,TRANSACTION_ACTION_ID           as transaction_action_id
,TRANSACTION_SOURCE_TYPE_ID      as transaction_source_type_id
,TRANSACTION_SOURCE_ID           as transaction_source_id
,TRANSACTION_SOURCE_NAME         as transaction_source_name
,TRANSACTION_REFERENCE           as transaction_reference
,REASON_ID                       as reason_id
,COST_GROUP_ID                   as cost_group_id
,ACCT_PERIOD_ID                  as acct_period_id
,ORGANIZATION_TYPE               as organization_type
,OWNING_ORGANIZATION_ID          as owning_organization_id
,OWNING_TP_TYPE                  as owning_tp_type
,COSTED_FLAG                     as costed_flag
,INVOICED_FLAG                   as invoiced_flag
,TRANSACTION_UOM                 as transaction_uom
,TRANSACTION_QUANTITY            as transaction_quantity
,PRIMARY_QUANTITY                as primary_quantity
,SECONDARY_UOM_CODE              as secondary_uom_code
,SECONDARY_TRANSACTION_QUANTITY  as secondary_transaction_quantity
,CURRENCY_CODE                   as currency_code
,CURRENCY_CONVERSION_TYPE        as currency_conversion_type
,CURRENCY_CONVERSION_RATE        as currency_conversion_rate
,INTERCOMPANY_CURRENCY_CODE      as intercompany_currency_code
,ACTUAL_COST                     as actual_cost
,TRANSACTION_COST                as transaction_cost
,PRIOR_COST                      as prior_cost
,NEW_COST                        as new_cost
,PRIOR_COSTED_QUANTITY           as prior_costed_quantity
,VARIANCE_AMOUNT                 as variance_amount
,VALUE_CHANGE                    as value_change
,PERCENTAGE_CHANGE               as percentage_change
,ENCUMBRANCE_AMOUNT              as encumbrance_amount
,TRANSFER_COST                   as transfer_cost
,TRANSPORTATION_COST             as transportation_cost
,TRANSFER_PRICE                  as transfer_price
,INTERCOMPANY_COST               as intercompany_cost
,TRANSACTION_DATE                as transaction_date
,TRANSACTION_DATE_KEY            as transaction_date_key
,CURRENCY_CONVERSION_DATE        as currency_conversion_date
,CURRENCY_CONVERSION_DATE_KEY    as currency_conversion_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_inventory_transaction', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
