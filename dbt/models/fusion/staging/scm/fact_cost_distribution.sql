{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(distribution_line_id)'
) }}

-- scm.fact_cost_distribution -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: distribution_line_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(DISTRIBUTION_LINE_ID, 0) as distribution_line_id
,DISTRIBUTION_ID             as distribution_id
,LINE_NUMBER                 as line_number
,SOURCE_TABLE                as source_table
,ACCOUNTING_LINE_TYPE        as accounting_line_type
,DR_CR_SIGN                  as dr_cr_sign
,ACCOUNTING_DEFINITION_ID    as accounting_definition_id
,COST_ID                     as cost_id
,TRANSACTION_COST_ID         as transaction_cost_id
,COST_ELEMENT_ID             as cost_element_id
,CODE_COMBINATION_ID         as code_combination_id
,SLA_CODE_COMBINATION_ID     as sla_code_combination_id
,AE_HEADER_ID                as ae_header_id
,AE_LINE_NUM                 as ae_line_num
,COST_ORGANIZATION_ID        as cost_organization_id
,COST_BOOK_ID                as cost_book_id
,LEDGER_ID                   as ledger_id
,LEGAL_ENTITY_ID             as legal_entity_id
,INVENTORY_ITEM_ID           as inventory_item_id
,TRANSACTION_ID              as transaction_id
,REC_TRXN_ID                 as rec_trxn_id
,DEP_TRXN_ID                 as dep_trxn_id
,COST_TRANSACTION_TYPE       as cost_transaction_type
,ADDITIONAL_PROCESSING_CODE  as additional_processing_code
,ACCOUNTED_FLAG              as accounted_flag
,LAYER_QUANTITY              as layer_quantity
,COST_TRANSACTION_UOM        as cost_transaction_uom
,DISTRIBUTION_QUANTITY_RATE  as distribution_quantity_rate
,ENTERED_CURRENCY_CODE       as entered_currency_code
,LEDGER_CURRENCY_CODE        as ledger_currency_code
,ENTERED_CURRENCY_AMOUNT     as entered_currency_amount
,LEDGER_AMOUNT               as ledger_amount
,EXCHANGE_RATE               as exchange_rate
,EXCHANGE_RATE_TYPE          as exchange_rate_type
,GL_DATE                     as gl_date
,GL_DATE_KEY                 as gl_date_key
,EXCHANGE_DATE               as exchange_date
,EXCHANGE_DATE_KEY           as exchange_date_key
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_cost_distribution', 'scm') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
