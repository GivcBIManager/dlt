{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, id)',
    partition_by='branch_id'
) }}

-- oasis_lake.api_pre_approval_res_details -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.API_PRE_APPROVAL_RES_DETAILS (transaction load).
--
-- Grain: branch_id, id (measured unique over 6,223,441 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 id,
 api_trans_id,
 service_code,
 service_desc,
 notes,
 created_by_user,
 creation_date,
 amend_by_user,
 amend_last_date,
 hospital_id,
 status,
 item_no,
 error_id,
 error_text,
 error_location,
 outcome_reason_code,
 outcome_reason,
 outcome_reason_system,
 outcome,
 approved_quantity,
 res_id,
 note_number,
 service_type,
 add_item_quantity,
 body_site_system,
 body_site,
 sub_site_system,
 sub_site,
 eligible,
 copay,
 benefit,
 tax,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 approved_amount,
 merge_hash
from {{ iceberg_source('api_pre_approval_res_details') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
