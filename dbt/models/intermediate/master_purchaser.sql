{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id,id)',
    partition_by='branch_id'
) }}
select 
branch_id
,p.purchaser_code id
,initCap(p.description ) description
,initCap(d.account_name) account_name
,p.account_code
,p.account_type 
,d.group_code
,p.amend_last_date
,p.recorded_updated_at
,p.is_tpa 
FROM 
{{ iceberg_source('purchasers') }} p 
INNER JOIN {{ iceberg_source('external_accounts_data') }} d ON p.account_code = d.account_code and p.account_type = d.account_type and p.account_c_id = d.c_id
{% if is_incremental() %}
WHERE  p.recorded_updated_at > (SELECT max(recorded_updated_at) FROM {{this}})
{% endif %}