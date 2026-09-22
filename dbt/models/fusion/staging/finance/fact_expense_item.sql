{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(expense_id)'
) }}

-- finance.fact_expense_item -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: expense_id (measured unique over today's data)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(EXPENSE_ID, 0)          as expense_id
,EXPENSE_REPORT_ID              as expense_report_id
,PERSON_ID                      as person_id
,ASSIGNMENT_ID                  as assignment_id
,BUSINESS_UNIT_ID               as business_unit_id
,EXPENSE_TYPE_ID                as expense_type_id
,EXPENSE_TYPE_CATEGORY_CODE     as expense_type_category_code
,EXPENSE_SOURCE                 as expense_source
,ITEMIZATION_PARENT_EXPENSE_ID  as itemization_parent_expense_id
,MERGED_PARENT_EXPENSE_ID       as merged_parent_expense_id
,REPORT_STATUS_CODE             as report_status_code
,EXPENSE_DATE                   as expense_date
,EXPENSE_DATE_KEY               as expense_date_key
,RECEIPT_DATE                   as receipt_date
,RECEIPT_DATE_KEY               as receipt_date_key
,EXPENSE_CREATION_DATE          as expense_creation_date
,EXPENSE_CREATION_DATE_KEY      as expense_creation_date_key
,REPORT_SUBMIT_DATE             as report_submit_date
,REPORT_SUBMIT_DATE_KEY         as report_submit_date_key
,ENTERED_CURRENCY_CODE          as entered_currency_code
,ENTERED_RECEIPT_AMOUNT         as entered_receipt_amount
,REIMBURSEMENT_CURRENCY_CODE    as reimbursement_currency_code
,REIMBURSABLE_AMOUNT            as reimbursable_amount
,ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_expense_item', 'finance') }}
{% if is_incremental() %}
where ifNull(LAST_UPDATE_DATE, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
