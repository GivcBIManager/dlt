{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, authorisation_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.authorisations -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.AUTHORISATIONS (transaction load).
--
-- Grain: branch_id, authorisation_no (measured unique over 6,612,597 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 authorisation_no,
 patient_id,
 episode_no,
 ios,
 no_authorised,
 no_used,
 authorised_flag,
 comments,
 amend_by_user,
 amend_last_date,
 doc_no,
 doc_date,
 signed_by1,
 signed_by2,
 fixed_quantity,
 request_no,
 no_requested,
 sms_sent,
 rejection_code,
 hold_code,
 amount_authorised,
 amount_used,
 hospital_id,
 term_id,
 estimated_los,
 api_last_trans_id,
 api_last_date,
 api_last_trans_type,
 com_req_id,
 com_api_trans_id,
 order_line,
 on_line,
 referral,
 referral_pre_auth_ref,
 referring_provider,
 transfer_request,
 pull_response_id,
 adm_req_request_diagnosis_id,
 admitting_episode,
 notes,
 pre_auth_ref_start,
 pre_auth_ref_end,
 related_authorisation_no,
 uom_code,
 conv_factor,
 attendance_type,
 selection_reason,
 auto_approved,
 auto_approved_date,
 extend,
 has_absolute_auth,
 extend_from,
 tooth_no,
 approval_reason_code,
 adt_req_plan_order_line,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 test,
 merge_hash
from {{ iceberg_source('authorisations') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
