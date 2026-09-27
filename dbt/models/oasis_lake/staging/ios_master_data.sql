{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, ios)',
    partition_by='branch_id'
) }}

-- oasis_lake.ios_master_data -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.IOS_MASTER_DATA (master load).
--
-- Grain: branch_id, ios (measured unique over 982,649 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 ios,
 service_3,
 ios_type,
 ios_main,
 ios_user,
 ios_category,
 associate_category,
 pharmacy_type,
 generic_id,
 note_id,
 indication_id,
 dosage_instruction_id,
 special_precaution_id,
 interaction_id,
 adverse_drug_reaction_id,
 inventory_ios,
 inventory_price,
 orderable_flag,
 product_code,
 product_category_code,
 contraindication_id,
 consultant_display_flag,
 amend_by_user,
 amend_last_date,
 no_of_days,
 schedul_flag,
 reduandant_request_hours,
 service_dept,
 delivery_price_changeable,
 associated_pricing_flag,
 procedure_flag,
 order_second_approval,
 approve_first_order,
 reduandant_request_days,
 proc_image_details,
 hospital_id,
 auto_delivery_package_items,
 blood_product_code,
 req_attchment,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 old_ios_user,
 merge_hash
from {{ iceberg_source('ios_master_data') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
