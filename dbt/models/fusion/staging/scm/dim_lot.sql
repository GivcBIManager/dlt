{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(inventory_item_id, organization_id, lot_number)'
) }}

-- scm.dim_lot -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: inventory_item_id, organization_id, lot_number (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(INVENTORY_ITEM_ID, 0) as inventory_item_id
,ifNull(ORGANIZATION_ID, 0)  as organization_id
,ifNull(LOT_NUMBER, '')      as lot_number
,EXPIRATION_DATE             as expiration_date
,EXPIRATION_DATE_KEY         as expiration_date_key
,ORIGINATION_DATE            as origination_date
,ORIGINATION_DATE_KEY        as origination_date_key
,RETEST_DATE                 as retest_date
,RETEST_DATE_KEY             as retest_date_key
,MATURITY_DATE               as maturity_date
,MATURITY_DATE_KEY           as maturity_date_key
,BEST_BY_DATE                as best_by_date
,BEST_BY_DATE_KEY            as best_by_date_key
,HOLD_DATE                   as hold_date
,HOLD_DATE_KEY               as hold_date_key
,EXPIRATION_ACTION_CODE      as expiration_action_code
,EXPIRATION_ACTION_DATE      as expiration_action_date
,EXPIRATION_ACTION_DATE_KEY  as expiration_action_date_key
,STATUS_ID                   as status_id
,GRADE_CODE                  as grade_code
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_lot', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
