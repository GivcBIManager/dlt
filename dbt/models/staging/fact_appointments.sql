{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id, appointment_id)',
    partition_by='branch_id'
) }}

select 
branch_id
,toDate32('1970-01-01') + (julian_date - 2440588) table_date
,start_date
,end_date
,appointment_id
,patient_id
,episode_no
,time_arrived
,time_seen
,time_complete
,consultant
,new_followup_flag
,walkin_flag
,outcome_code
,consultation_type
,last_application_name
,on_line_booking
,booked_from
,recorded_updated_at
from {{ iceberg_source('appointments') }}
{% if is_incremental() %}
WHERE  recorded_updated_at > (SELECT max(recorded_updated_at) FROM {{ this }})
{% endif %}