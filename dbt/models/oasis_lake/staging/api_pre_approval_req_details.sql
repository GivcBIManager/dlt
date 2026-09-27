{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, id)',
    partition_by='branch_id'
) }}

-- oasis_lake.api_pre_approval_req_details -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.API_PRE_APPROVAL_REQ_DETAILS (transaction load).
--
-- Grain: branch_id, id (measured unique over 5,668,606 lake rows)
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
 supply_period,
 benefit_head,
 exempt_cat,
 item_no,
 quantity,
 referral_ind,
 request_category,
 service_code,
 service_description,
 service_type,
 ex_detail,
 hospital_id,
 amend_by_user,
 amend_last_date,
 created_by_user,
 creation_date,
 estimated_cost,
 diagnosis_code,
 supply_from,
 supply_to,
 status,
 override_ind,
 override_reason,
 adj_ind,
 duration,
 unit,
 unit_type,
 times,
 per,
 request_id,
 dosage,
 remark,
 notes,
 tooth_no,
 tooth_no_fdi,
 unit_price,
 ios,
 authorisation_no,
 net_tax,
 oasis_ios_user,
 oasis_ios_description,
 qty_stocked_uom,
 unit_price_stocked_uom,
 prescribed_code,
 selection_reason,
 selection_reason_display,
 service_date,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('api_pre_approval_req_details') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
