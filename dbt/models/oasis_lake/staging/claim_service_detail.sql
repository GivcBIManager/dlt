{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, visit_id, service_id, invoice_number, sequence_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.claim_service_detail -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.CLAIM_SERVICE_DETAIL (transaction load).
--
-- Grain: branch_id, visit_id, service_id, invoice_number, sequence_no (measured unique over 23,074,619 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 hospital_id,
 service_id,
 invoice_number,
 invoice_date,
 service_code,
 service_description,
 oasis_ios_code,
 qty,
 line_claimed_amount,
 line_claimed_amount_sar,
 co_pay,
 co_insurance,
 line_item_discount,
 net_amount,
 net_vat_amount,
 patient_vat_amount,
 vat_indicator,
 vat_percentage,
 treatment_type_indicator,
 service_type,
 service_category,
 invoice_created_by,
 username,
 pre_auth_id,
 tooth_no,
 tooth_surface,
 dental_treatment_type,
 visit_id,
 benefit_type_indicator,
 duration,
 unit,
 unit_type,
 times,
 per,
 medicine_generic_name,
 medicineconcentration,
 is_fetched,
 created_by_user,
 creation_date,
 amend_by_user,
 amend_last_date,
 service_code_system,
 achi_code,
 achi_description,
 oasis_service_type,
 qty_stocked_uom,
 unit_price,
 pre_auth_response_id,
 pre_auth_response_system,
 oasis_ios_user,
 oasis_ios_description,
 package_id,
 sequence_no,
 unit_price_stocked_uom,
 pre_auth_offline,
 pre_auth_offline_date,
 unit_price_net,
 approval_response_id,
 approval_response_system,
 discount_percentage,
 net_with_vat,
 notes,
 outcome,
 approved_qunatity,
 prescribed_code,
 selection_reason,
 selection_reason_display,
 followup,
 ios,
 is_compound,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('claim_service_detail') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
