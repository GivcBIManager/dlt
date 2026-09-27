{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, product_code, c_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.product_base -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.PRODUCT_BASE (snapshot load).
--
-- Grain: branch_id, product_code, c_id (measured unique over 854,749,772 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 short_name,
 abc_analysis_code,
 abc_analysis_code_date,
 activity_indicator,
 agency_code,
 analysis_category_1,
 analysis_code_1,
 analysis_category_2,
 analysis_code_2,
 analysis_category_3,
 analysis_code_3,
 average_cost,
 balance_fwd_date,
 balance_fwd_qty,
 controlled_indicator,
 ifNull(c_id, 0) as c_id,
 cycle_count_code,
 date_created,
 date_last_counted,
 date_last_issue,
 date_last_receipt,
 date_last_sale,
 date_last_transfer,
 default_bin_location,
 dimension_uom_code,
 last_cost,
 minimum_margin,
 product_category_code,
 ifNull(product_code, '') as product_code,
 product_description,
 qty_allocated,
 qty_on_backorder_supplier,
 qty_on_hand,
 qty_on_order,
 standard_cost,
 stocked_indicator,
 stocked_uom_code,
 superseded_by_product,
 supersession_date,
 superseded_product,
 tariff_code,
 tax_code,
 unit_dim_1,
 unit_dim_2,
 unit_dim_3,
 unit_weight,
 weight_uom_code,
 write_down_indicator,
 price,
 product_ind,
 division,
 sales_conv_factor,
 sales_uom_code,
 future_price,
 mtd_usage,
 mtd_value,
 markup_pct,
 gross_weight,
 ent_av_ind,
 tender_id,
 purch_uom_code,
 pr_auto_print_flag,
 max_serial_no,
 last_mnfc_code,
 last_mnfc_part_no,
 last_origin_country,
 qty_on_replacment,
 storing_type,
 print_barcode,
 main_store,
 date_last_grn,
 qty_last_grn,
 hold_min,
 hold_max,
 sub_product_category_code,
 detail_product_category_code,
 hospital_id,
 ifNull(branch_id, 0) as branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 version,
 version_date,
 scan_barcode_flag,
 t_sel_price,
 t_cost,
 t_cost_src,
 cost_updated
from {{ iceberg_source('product_base') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
