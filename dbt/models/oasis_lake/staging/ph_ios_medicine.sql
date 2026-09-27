{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, ios_main)',
    partition_by='branch_id'
) }}

-- oasis_lake.ph_ios_medicine -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.PH_IOS_MEDICINE (master load).
--
-- Grain: branch_id, ios_main (measured unique over 55,473 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 ios_main,
 medicine_form,
 measure,
 strength,
 medicine_class,
 therpy_type,
 medicine_category,
 verification_flag,
 investigation_flag,
 max_lifetime_dose,
 volume,
 max_no_of_days,
 lifespan,
 alternative_need_approval,
 weight,
 si_meq,
 si_mmol,
 neutrsion_form,
 cautionary_id,
 stability_hours,
 default_admin_unit,
 min_admin_unit,
 min_sales_unit_per_order,
 amend_by_user,
 amend_last_date,
 calculate_dose_flag,
 default_admin_method,
 strength_uom,
 strength_by_int_units,
 strength_by_int_units_uom,
 volume_uom,
 no_of_drops_per_ml,
 formulary_flag,
 default_route,
 unit_dose_flag,
 need_initial_diluent,
 chemo_additive,
 need_additive,
 significant_flag,
 inp_display_uom,
 out_display_uom,
 max_prn_qty,
 auto_adjust_order,
 discard_days,
 high_risk_med_flag,
 thp_class_id,
 inp_max_no_of_days,
 out_max_no_of_days,
 inp_verification_flag,
 out_verification_flag,
 verification_steps,
 hazard_dose_flag,
 thp_class_by_pharmcist,
 required_witness,
 active_period_in,
 period_type_in,
 active_period_out,
 period_type_out,
 max_take_home_days,
 hospital_id,
 extemporaneous,
 extemporaneous_ml,
 printed_strength,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash,
 wasfaty_ios
from {{ iceberg_source('ph_ios_medicine') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
