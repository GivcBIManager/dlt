{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(je_header_id, je_line_num)'
) }}

-- finance.fact_gl_journal_line -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: je_header_id, je_line_num (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(JE_HEADER_ID, 0)       as je_header_id
,ifNull(JE_LINE_NUM, 0)        as je_line_num
,JE_BATCH_ID                   as je_batch_id
,JOURNAL_NAME                  as journal_name
,DOC_SEQUENCE_VALUE            as doc_sequence_value
,LEDGER_ID                     as ledger_id
,LEGAL_ENTITY_ID               as legal_entity_id
,CODE_COMBINATION_ID           as code_combination_id
,PERIOD_SET_NAME               as period_set_name
,PERIOD_TYPE                   as period_type
,PERIOD_NAME                   as period_name
,ACCOUNTING_DATE               as accounting_date
,ACCOUNTING_DATE_KEY           as accounting_date_key
,EFFECTIVE_DATE                as effective_date
,EFFECTIVE_DATE_KEY            as effective_date_key
,POSTED_DATE                   as posted_date
,POSTED_DATE_KEY               as posted_date_key
,JE_SOURCE                     as je_source
,JE_CATEGORY                   as je_category
,ACTUAL_FLAG                   as actual_flag
,LINE_STATUS                   as line_status
,HEADER_STATUS                 as header_status
,BATCH_STATUS                  as batch_status
,ENTERED_CURRENCY_CODE         as entered_currency_code
,ENTERED_DR                    as entered_dr
,ENTERED_CR                    as entered_cr
,ENTERED_AMOUNT                as entered_amount
,LEDGER_CURRENCY_CODE          as ledger_currency_code
,ACCOUNTED_DR                  as accounted_dr
,ACCOUNTED_CR                  as accounted_cr
,ACCOUNTED_AMOUNT              as accounted_amount
,STAT_AMOUNT                   as stat_amount
,CURRENCY_CONVERSION_TYPE      as currency_conversion_type
,CURRENCY_CONVERSION_RATE      as currency_conversion_rate
,CURRENCY_CONVERSION_DATE      as currency_conversion_date
,CURRENCY_CONVERSION_DATE_KEY  as currency_conversion_date_key
,LINE_DESCRIPTION              as line_description
,GL_SL_LINK_ID                 as gl_sl_link_id
,GL_SL_LINK_TABLE              as gl_sl_link_table
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_gl_journal_line', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
