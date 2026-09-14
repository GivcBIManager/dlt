{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id,patient_id,episode_no,responsibility_seq )',
    partition_by='branch_id'
) }}
SELECT 
branch_id,patient_id,episode_no,responsibility_seq,account_no,tariff_id,purchaser_code,policy_code,contract_no,status,bill_to,recorded_updated_at
FROM {{ iceberg_source('patient_bill_agreements') }} ba
{% if is_incremental() %}
WHERE  recorded_updated_at > (SELECT max(recorded_updated_at) FROM {{this}})
{% endif %}