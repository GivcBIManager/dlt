{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, c_id)',
    partition_by='branch_id'
) }}

-- oasis_lake.control_contexts_data -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: DEVDBA.CONTROL_CONTEXTS_DATA (master load).
--
-- Grain: branch_id, c_id (measured unique over 1,585 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 control_context,
 c_id,
 currency_code,
 heading,
 ap_wc,
 ar_wc,
 description,
 gl_wc,
 posting_calendar,
 default_exchange_rate_type,
 current_tp_seq_gl,
 current_tp_seq_ar,
 current_tp_seq_ap,
 default_cost_method,
 ar_cat_1,
 ar_cat_2,
 ap_cat_1,
 ap_cat_2,
 im_cat_1,
 im_cat_2,
 current_tp_seq_so,
 add_line_1,
 add_line_2,
 add_line_3,
 add_line_4,
 add_post_code,
 street_line_1,
 street_line_2,
 street_line_3,
 street_line_4,
 street_post_code,
 telephone,
 fax_code,
 telex_code,
 reg_no,
 gst_exemption_no,
 print_add,
 last_pi_post,
 inv_mess_1,
 inv_mess_2,
 winv_mess_1,
 winv_mess_2,
 state_mess_1,
 state_mess_2,
 freight_tax_code,
 so_prices,
 im_tax_code,
 file_room_printer,
 auto_iss_alloc,
 ledger_account,
 standard_cost,
 depot_ind,
 gl_entity,
 gl_restrict,
 current_tp_seq_cb,
 percent_used,
 depreciation_used,
 barcode,
 obsolete_flag,
 floor_area,
 amend_last_date,
 amend_by_user,
 nondepartmental_store,
 im_plan_exclude,
 implnclc_exclude,
 auto_trfreq_exclude,
 trfreq_restrict,
 auto_preq_exclude,
 exclude_from_trf_req,
 current_tp_seq_am,
 created_by_user,
 creation_date,
 hospital_id,
 active_flag,
 unit_dim_1,
 unit_dim_2,
 unit_dim_3,
 facility_id,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('control_contexts_data') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
