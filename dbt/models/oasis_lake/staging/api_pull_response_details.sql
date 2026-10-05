{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, response_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.api_pull_response_details -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.API_PULL_RESPONSE_DETAILS (transaction load).
--
-- Grain: branch_id, response_id (measured unique over 7,265,658 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 response_id,
 api_trans_id,
 line_number,
 response_bundle,
 response_type,
 amend_by_user,
 amend_last_date,
 created_by_user,
 creation_date,
 hospital_id,
 about_api_trans_id,
 res_status,
 bundle_id,
 message_header_id,
 message_id,
 sender_identifier,
 req_identifier,
 req_identifier_system,
 res_identifier,
 res_identifier_system,
 status,
 error_message,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('api_pull_response_details') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
