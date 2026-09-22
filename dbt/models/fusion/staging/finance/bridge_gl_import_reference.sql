{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(import_reference_id)'
) }}

-- finance.bridge_gl_import_reference -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: import_reference_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(IMPORT_REFERENCE_ID, 0) as import_reference_id
,JE_BATCH_ID                   as je_batch_id
,JE_HEADER_ID                  as je_header_id
,JE_LINE_NUM                   as je_line_num
,HEADER_STATUS                 as header_status
,GL_SL_LINK_ID                 as gl_sl_link_id
,GL_SL_LINK_TABLE              as gl_sl_link_table
,SUBLEDGER_DOC_SEQUENCE_ID     as subledger_doc_sequence_id
,SUBLEDGER_DOC_SEQUENCE_VALUE  as subledger_doc_sequence_value
,REFERENCE_1                   as reference_1
,REFERENCE_2                   as reference_2
,REFERENCE_3                   as reference_3
,REFERENCE_4                   as reference_4
,REFERENCE_5                   as reference_5
,REFERENCE_6                   as reference_6
,REFERENCE_7                   as reference_7
,REFERENCE_8                   as reference_8
,REFERENCE_9                   as reference_9
,REFERENCE_10                  as reference_10
,ACCOUNTING_DATE               as accounting_date
,ACCOUNTING_DATE_KEY           as accounting_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('bridge_gl_import_reference', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
