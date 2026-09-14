{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id, admission_no)',
    partition_by='branch_id'
) }}
select 
branch_id
,admission_no
,patient_id
,episode_no
,admit_date
,reason_for_admit
,est_discharge_date
,status
,outcome
,admit_long_id
,discharge_long_id
,physical_discharge_date
,clinical_discharge_date
,financial_discharge_date
,discharge_destination
,discharge_condition,
latest_est_discharge_date,
bed_order_no,
amend_by_user,
amend_last_date,
admit_type,
bed_class,
planned_admit_id,
treatment_code,
accept_visit_flag,
admission_department,
consignment_flag,
companion,
home_leave_flag,
restrict_visit_reason,
accept_phone_call,
patient_admission_category,
referred_type,
admission_mode,
created_by_user,
creation_date,
patient_seen,
discharge_type,
transfer_to,
hl7_sent,
icd_code,
physical_discharge_by,
clinical_discharge_by,
financial_discharge_by,
other_admission_reason,
admission_reason_note,
treated_by,
printed_by,
printed_date,
seen_date,
hospital_id,
ready_for_financial_discharge,
visit_deficieny_status,
trx_sent,
prevent_discharge,
mmm_sent,
need_reconciliation,
reconciliation_stage,
insert_at,
recorded_updated_at
from {{ iceberg_source('patient_ad') }}
{% if is_incremental() %}
WHERE  recorded_updated_at > (SELECT max(recorded_updated_at) FROM {{ this }})
{% endif %}