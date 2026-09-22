{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(transaction_type_id)'
) }}

-- scm.dim_inv_transaction_type -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: transaction_type_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(TRANSACTION_TYPE_ID, 0) as transaction_type_id
,TRANSACTION_TYPE_NAME         as transaction_type_name
,DESCRIPTION                   as description
,TRANSACTION_ACTION_ID         as transaction_action_id
,TRANSACTION_SOURCE_TYPE_ID    as transaction_source_type_id
,TRANSACTION_SOURCE_TYPE_NAME  as transaction_source_type_name
,USER_DEFINED_FLAG             as user_defined_flag
,START_DATE                    as start_date
,START_DATE_KEY                as start_date_key
,END_DATE                      as end_date
,END_DATE_KEY                  as end_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_inv_transaction_type', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
