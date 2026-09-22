{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(last_update_date)',
    order_by='(application_id, event_id, ae_header_id, ae_line_num, temp_line_num, source_distribution_type, source_distribution_id_num_1)'
) }}

-- finance.bridge_sla_distribution_link -- Oracle Fusion warehouse, staged 1:1 into `fusion`.
--
-- GENERATED, then yours: this file was written from the warehouse schema, but
-- it is an ordinary model. Edit it freely; nothing regenerates it. (The source
-- declaration and the Iceberg path ARE regenerated -- see gui/ofusion_sources.py.)
--
-- Grain: application_id, event_id, ae_header_id, ae_line_num, temp_line_num, source_distribution_type, source_distribution_id_num_1 (UNVERIFIED -- table is empty)
-- Columns arrive Nullable, so the sorting-key columns are coalesced here:
-- ClickHouse rejects a MergeTree sorting key over nullable columns.
-- LAST_UPDATE_DATE also arrives as a STRING from this (empty) table, and
-- ReplacingMergeTree will not take a String version column, so it is parsed to
-- DateTime64(6) here -- the same type the populated models already expose.
--
-- The warehouse table is STILL EMPTY, so this grain could not be measured: the
-- sorting key is inferred from the Fusion table's natural key, and is chosen
-- deliberately WIDE. Under ReplacingMergeTree a key that is too wide only
-- skips dedup, while one that is too narrow silently collapses distinct rows.
-- The `unique_combination_final` test on this key in _ofusion__models.yml
-- fails the run as soon as real data contradicts the guess.
--
-- Incremental: appends rows whose LAST_UPDATE_DATE is newer than anything
-- loaded, and ReplacingMergeTree(last_update_date) collapses an updated row
-- against its earlier version on merge. A row whose LAST_UPDATE_DATE is NULL
-- loads on the first full build and is not picked up by later runs.

select
 ifNull(APPLICATION_ID, '')     as application_id
,ifNull(EVENT_ID, '')           as event_id
,EVENT_CLASS_CODE               as event_class_code
,EVENT_TYPE_CODE                as event_type_code
,REF_EVENT_ID                   as ref_event_id
,ifNull(AE_HEADER_ID, '')       as ae_header_id
,ifNull(AE_LINE_NUM, '')        as ae_line_num
,REF_AE_HEADER_ID               as ref_ae_header_id
,REF_AE_LINE_NUM                as ref_ae_line_num
,ifNull(TEMP_LINE_NUM, '')      as temp_line_num
,REF_TEMP_LINE_NUM              as ref_temp_line_num
,ACCOUNTING_DATE                as accounting_date
,ACCOUNTING_DATE_KEY            as accounting_date_key
,ACCOUNTING_ENTRY_STATUS_CODE   as accounting_entry_status_code
,ifNull(SOURCE_DISTRIBUTION_TYPE, '') as source_distribution_type
,ifNull(SOURCE_DISTRIBUTION_ID_NUM_1, '') as source_distribution_id_num_1
,SOURCE_DISTRIBUTION_ID_NUM_2   as source_distribution_id_num_2
,SOURCE_DISTRIBUTION_ID_NUM_3   as source_distribution_id_num_3
,SOURCE_DISTRIBUTION_ID_NUM_4   as source_distribution_id_num_4
,SOURCE_DISTRIBUTION_ID_NUM_5   as source_distribution_id_num_5
,SOURCE_DISTRIBUTION_ID_CHAR_1  as source_distribution_id_char_1
,SOURCE_DISTRIBUTION_ID_CHAR_2  as source_distribution_id_char_2
,SOURCE_DISTRIBUTION_ID_CHAR_3  as source_distribution_id_char_3
,SOURCE_DISTRIBUTION_ID_CHAR_4  as source_distribution_id_char_4
,SOURCE_DISTRIBUTION_ID_CHAR_5  as source_distribution_id_char_5
,ENTITY_CODE                    as entity_code
,ENTITY_SOURCE_ID_INT_1         as entity_source_id_int_1
,ENTITY_SOURCE_ID_INT_2         as entity_source_id_int_2
,ENTITY_SOURCE_ID_INT_3         as entity_source_id_int_3
,ENTITY_SOURCE_ID_INT_4         as entity_source_id_int_4
,ENTITY_SOURCE_ID_CHAR_1        as entity_source_id_char_1
,ENTITY_SOURCE_ID_CHAR_2        as entity_source_id_char_2
,ENTITY_SOURCE_ID_CHAR_3        as entity_source_id_char_3
,ENTITY_SOURCE_ID_CHAR_4        as entity_source_id_char_4
,APPLIED_TO_APPLICATION_ID      as applied_to_application_id
,APPLIED_TO_ENTITY_CODE         as applied_to_entity_code
,APPLIED_TO_ENTITY_ID           as applied_to_entity_id
,APPLIED_TO_SOURCE_ID_NUM_1     as applied_to_source_id_num_1
,APPLIED_TO_SOURCE_ID_NUM_2     as applied_to_source_id_num_2
,APPLIED_TO_SOURCE_ID_NUM_3     as applied_to_source_id_num_3
,APPLIED_TO_SOURCE_ID_NUM_4     as applied_to_source_id_num_4
,APPLIED_TO_SOURCE_ID_CHAR_1    as applied_to_source_id_char_1
,APPLIED_TO_SOURCE_ID_CHAR_2    as applied_to_source_id_char_2
,APPLIED_TO_SOURCE_ID_CHAR_3    as applied_to_source_id_char_3
,APPLIED_TO_SOURCE_ID_CHAR_4    as applied_to_source_id_char_4
,APPLIED_TO_DISTRIBUTION_TYPE   as applied_to_distribution_type
,APPLIED_TO_DIST_ID_NUM_1       as applied_to_dist_id_num_1
,APPLIED_TO_DIST_ID_NUM_2       as applied_to_dist_id_num_2
,APPLIED_TO_DIST_ID_NUM_3       as applied_to_dist_id_num_3
,APPLIED_TO_DIST_ID_NUM_4       as applied_to_dist_id_num_4
,APPLIED_TO_DIST_ID_NUM_5       as applied_to_dist_id_num_5
,APPLIED_TO_DIST_ID_CHAR_1      as applied_to_dist_id_char_1
,APPLIED_TO_DIST_ID_CHAR_2      as applied_to_dist_id_char_2
,APPLIED_TO_DIST_ID_CHAR_3      as applied_to_dist_id_char_3
,APPLIED_TO_DIST_ID_CHAR_4      as applied_to_dist_id_char_4
,APPLIED_TO_DIST_ID_CHAR_5      as applied_to_dist_id_char_5
,ENTERED_CURRENCY_CODE          as entered_currency_code
,ENTERED_DR                     as entered_dr
,ENTERED_CR                     as entered_cr
,ENTERED_AMOUNT                 as entered_amount
,LEDGER_CURRENCY_CODE           as ledger_currency_code
,ACCOUNTED_DR                   as accounted_dr
,ACCOUNTED_CR                   as accounted_cr
,ACCOUNTED_AMOUNT               as accounted_amount
,STATISTICAL_AMOUNT             as statistical_amount
,ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'))   as last_update_date
from {{ ofusion_source('bridge_sla_distribution_link', 'finance') }}
{% if is_incremental() %}
where ifNull(parseDateTime64BestEffortOrNull(LAST_UPDATE_DATE, 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) > (select max(last_update_date) from {{ this }})
{% endif %}
