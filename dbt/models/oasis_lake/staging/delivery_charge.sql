{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, delivery_charge_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.delivery_charge -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.DELIVERY_CHARGE (transaction load).
--
-- Grain: branch_id, delivery_charge_id (measured unique over 106,741,186 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 delivery_line,
 patient_id,
 episode_no,
 ios,
 doc_id,
 docl_id,
 contract_no,
 price_paid_purchaser,
 commission_paid,
 discount_given,
 units_processed,
 admission_no,
 authorisation_no,
 auth_outstanding,
 bill_to,
 attendance_type,
 status,
 delivery_date,
 tariff_id,
 units_delivered,
 payment_type,
 payment_amount,
 orgnl_price_paid_purchaser,
 orgnl_discount_given,
 package_deal_flag,
 package_process_status,
 signon_id,
 invoice_no,
 cancel_flag,
 product_category_code,
 cancelled_qty,
 cancelled_amount,
 cancelled_discount,
 payment_card_no,
 crd_signon_id,
 crd_payment_type,
 crd_payment_card_no,
 crd_doc_id,
 crd_docl_id,
 due_detail_id,
 delivery_charge_id,
 staff_id,
 package_id,
 elig_accomm_ios,
 elig_accomm_ios_price,
 approval_no,
 encounter_id,
 encounter_type,
 new_followup_flag,
 policy_code,
 purchaser_code,
 service_dept,
 ios_main,
 vat_code,
 vat_value,
 hospital_id,
 rule_term_id,
 rule_term_sequence,
 refund_type,
 patient_share_type,
 sub_product_category_code,
 take_home,
 status_reason,
 status_reason_code,
 units_charged,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('delivery_charge') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
