{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(input_value_id)'
) }}

-- hcm.dim_payroll_input_value -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: input_value_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(INPUT_VALUE_ID, 0) as input_value_id
,ELEMENT_TYPE_ID        as element_type_id
,INPUT_VALUE_BASE_NAME  as input_value_base_name
,INPUT_VALUE_NAME       as input_value_name
,UOM                    as uom
,CURRENCY_CODE          as currency_code
,DISPLAY_SEQUENCE       as display_sequence
,MANDATORY_FLAG         as mandatory_flag
,VALID_FROM             as valid_from
,VALID_TO               as valid_to
,IS_CURRENT             as is_current
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('dim_payroll_input_value', 'hcm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
