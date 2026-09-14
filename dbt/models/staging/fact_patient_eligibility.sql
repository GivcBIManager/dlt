{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id, patient_eligibility_id)',
    partition_by='branch_id'
) }}
select 
branch_id,
patient_eligibility_id,
patient_id,
episode_no,
`sequence`,
eligibility_number,
eligibility_service_dept,
eligibility_start,
eligibility_end,
eligibility_status,
consultant_id,
master_order_no,
attendance_type,
admission_no,
admit_flag,
discharge_flag,
responsibility,
work_entity,
amend_by_user,
amend_last_date,
eligibility_work_entity,
original_eligibility_id,
referral_code,
hl7_sent,
hospital_id,
virtual,
after_discharge_episode_no,
trx_sent,
recorded_updated_at
from {{ iceberg_source('patient_eligibility') }}
{% if is_incremental() %}
WHERE  recorded_updated_at > (SELECT max(recorded_updated_at) FROM {{ this }})
{% endif %}