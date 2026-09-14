{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(branch_id,id )',
    partition_by='branch_id'
) }}
SELECT 
bd.branch_id branch_id
,bd.bed_detail_id id
,bd.start_date::Date32 table_date
,bd.bed_location bed_location
,bd.bed_status bed_status_id
,bd.bed_class bed_class_id
,rm.room_class room_class_id
,bc1.description room_class
,bc.description bed_class
,bd.bed_sex bed_gender
,bd.current_record 
,bd.room_no 
,rm.description room_description
,rm.room_status room_status_id
,bd.work_entity
,bd.admission_no 
,bd.patient_id 
,bd.episode_no 
,bd.start_date 
,bd.end_date 
,bd.trans_from_work_entity 
,bd.trans_from_bed_location
, bd.recorded_updated_at recorded_updated_at
FROM {{ iceberg_source('bed_details') }} bd 
LEFT JOIN {{ iceberg_source('bed_class_master_data') }}  bc ON bd.bed_class::Nullable(decimal) = bc.bed_class and bd.branch_id  = bc.branch_id
LEFT JOIN {{ iceberg_source('room_master') }} rm ON bd.room_no::Nullable(decimal) = rm.room_no and bd.work_entity = rm.work_entity and bd.branch_id  = rm.branch_id
LEFT JOIN {{ iceberg_source('bed_class_master_data') }}  bc1 ON rm.room_class::Nullable(decimal) = bc1.bed_class and rm.branch_id  = bc1.branch_id
LEFT JOIN {{ iceberg_source('bed_slots_master') }}  bs ON bd.bed_location = bs.bed_location and bd.branch_id  = bs.branch_id
{% if is_incremental() %}
WHERE  bd.recorded_updated_at > (SELECT max(recorded_updated_at) FROM {{this}})
{% endif %}