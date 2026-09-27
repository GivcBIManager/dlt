{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, term_id, sequence_no, applies_start)',
    partition_by='branch_id'
) }}

-- oasis_lake.term_details -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.TERM_DETAILS (master load).
--
-- Grain: branch_id, term_id, sequence_no, applies_start (measured unique over 4,539 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 term_id,
 sequence_no,
 applies_start,
 applies_end,
 calc_purchaser,
 calc_patient,
 calc_commission,
 until_code,
 until_figure,
 per_code,
 then_code,
 authorisation_req,
 word_purchaser,
 word_patient,
 word_commission,
 amend_by_user,
 amend_last_date,
 default_authorisation_qty,
 limit_base_price,
 ios_total_base,
 hospital_id,
 authorise_total_order,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('term_details') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
