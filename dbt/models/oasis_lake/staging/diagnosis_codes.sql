{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, diag_code)',
    partition_by='branch_id'
) }}

-- oasis_lake.diagnosis_codes -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.DIAGNOSIS_CODES (master load).
--
-- Grain: branch_id, diag_code (measured unique over 784,415 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 diag_code,
 diagnosis_division,
 hosp_code,
 level_no,
 children_flag,
 reportable_level,
 chronic_flag,
 alert_flag,
 at_risk_flag,
 amend_by_user,
 amend_last_date,
 father_diag_code,
 allergy,
 allergy_type,
 default_signif_days,
 selectable_flag,
 sex,
 new_diag_code_flag,
 created_by_hosp,
 created_by_dr,
 dosage_detail,
 icd_release_no,
 diagnosis_category_master,
 diagnosis_category_level_1,
 diagnosis_category_level_2,
 er_diagnosis_category_master,
 hospital_id,
 agel,
 ageh,
 mopha_flag,
 accident_flag,
 unacceptpdx,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('diagnosis_codes') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
