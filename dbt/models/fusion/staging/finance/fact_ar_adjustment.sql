{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(adjustment_id)'
) }}

-- finance.fact_ar_adjustment -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: adjustment_id (UNVERIFIED -- table is empty)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
-- LAST_UPDATE_DATE also arrives as a STRING from this (empty) table, and
-- ReplacingMergeTree will not take a String version column, so it is parsed to
-- DateTime64(6) here -- the same type the populated models already expose.
--
-- The warehouse table is STILL EMPTY, so this grain could not be measured: the
-- sorting key is inferred from the Fusion table's natural key. Under
-- ReplacingMergeTree a key that is too narrow silently collapses distinct rows.
-- The `unique_combination_final` test on this key in _ofusion__models.yml
-- fails the run as soon as real data contradicts the guess.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(ADJUSTMENT_ID, '') as adjustment_id
,ADJUSTMENT_NUMBER      as adjustment_number
,CUSTOMER_TRX_ID        as customer_trx_id
,PAYMENT_SCHEDULE_ID    as payment_schedule_id
,ADJUSTMENT_CLASS       as adjustment_class
,ADJUSTMENT_TYPE        as adjustment_type
,STATUS                 as status
,REASON_CODE            as reason_code
,RECEIVABLES_TRX_ID     as receivables_trx_id
,CODE_COMBINATION_ID    as code_combination_id
,GL_DATE                as gl_date
,GL_DATE_KEY            as gl_date_key
,APPLY_DATE             as apply_date
,APPLY_DATE_KEY         as apply_date_key
,CUSTOMER_ID            as customer_id
,CUSTOMER_SITE_USE_ID   as customer_site_use_id
,BUSINESS_UNIT_ID       as business_unit_id
,LEDGER_ID              as ledger_id
,ENTERED_CURRENCY_CODE  as entered_currency_code
,ENTERED_AMOUNT         as entered_amount
,LEDGER_CURRENCY_CODE   as ledger_currency_code
,ACCOUNTED_AMOUNT       as accounted_amount
,ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) as last_update_date
from {{ ofusion_source('fact_ar_adjustment', 'finance') }}
{% if is_incremental() %}
where ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
