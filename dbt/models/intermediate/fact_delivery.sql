{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id,delivery_charge_id )',
    partition_by='branch_id'
) }}
select 
dc.branch_id branch_id
,dc.delivery_date table_date
,dc.delivery_charge_id 
,dc.delivery_line
,md.master_delivery_no master_delivery_no
,dl.order_line 
,md.delivery_work_entity 
,dc.invoice_no
,dc.doc_id
,dc.docl_id 
,dc.crd_doc_id 
,dc.crd_docl_id 
,dl.c_id
,dc.status status
,dc.bill_to 
,md.delivery_date master_delivery_date
,dl.amend_last_date amend_last_date
,dc.patient_id
,dc.episode_no
,dc.encounter_id
,new_followup_flag followup_flag
,dc.staff_id 
,dc.signon_id 
,dc.crd_signon_id
,dc.tariff_id 
,dc.contract_no
,dc.purchaser_code 
,dc.policy_code 
,dc.package_id
,dc.ios ios
,dc.ios_main
,dc.service_dept
,dl.product_code 
,dl.batch_no 
,dl.unit_price 
,dc.product_category_code 
,COALESCE(dc.package_deal_flag, 'N') package_deal_flag
,COALESCE(dc.cancel_flag, 'X') cancel_flag
,dc.price_paid_purchaser
,dc.payment_amount 
,dc.payment_type
,dc.cancelled_amount
,dc.units_delivered units_delivered
,dc.units_charged
,dc.units_processed
,dc.vat_value 
,dc.vat_code 
,dc.attendance_type
,dc.admission_no
,dc.patient_share_type 
,status_reason_code cancellation_reason_code
,take_home
,dc.recorded_updated_at recorded_updated_at
FROM 
{{ iceberg_source('delivery_charge') }} dc 
INNER JOIN {{ iceberg_source('delivery_lines') }} dl ON dc.delivery_line::decimal = dl.delivery_line  and dc.branch_id = dl.branch_id
INNER JOIN {{ iceberg_source('master_deliveries') }}  md ON dl.master_delivery_no::decimal = md.master_delivery_no and dc.branch_id =  md.branch_id
{% if is_incremental() %}
  {%- set wm = run_query("select max(recorded_updated_at) from " ~ this).columns[0][0] -%}
  WHERE dc.recorded_updated_at > toDateTime64('{{ wm }}', 6)
{% endif %}
