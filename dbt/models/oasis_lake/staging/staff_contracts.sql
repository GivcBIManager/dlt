{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(recorded_updated_at)',
    order_by='(branch_id, staff_contract_no)',
    partition_by='branch_id'
) }}

-- oasis_lake.staff_contracts -- Oasis Iceberg lake, staged 1:1 into the `oasis` database.
--
-- Source: OASIS.STAFF_CONTRACTS (master load).
--
-- Grain: branch_id, staff_contract_no (measured unique over 34,000 lake rows)
-- The sorting key IS the dedup key: ReplacingMergeTree collapses on ORDER BY,
-- so every key column is coalesced here (ClickHouse rejects a nullable
-- sorting key, and a nullable version column).
--
-- Incremental: appends rows whose recorded_updated_at -- the dlt load stamp,
-- present on every lake table -- is newer than anything loaded, and
-- ReplacingMergeTree(recorded_updated_at) collapses an older version of a key
-- against the newer one on merge.

select
 staff_contract_no,
 staff_id,
 start_date,
 end_date,
 contracted_hours_per_week,
 contracted_hours_per_day,
 regular_timesheet_type,
 regular_hours_per_month,
 contract_note,
 termination_note,
 termination_date,
 termination_period,
 class_of_ticket,
 return_point,
 amend_by_user,
 amend_last_date,
 payment_transaction_no,
 payment_method,
 contract_type,
 no_of_days_leave,
 days_work_for_leave,
 days_work_for_ticket,
 no_dependants,
 no_dependant_tickets,
 roster_flag,
 availability_note,
 sponsor_id,
 regular_pay_flag,
 base_id,
 residency_entitle_code,
 termination_reason_code,
 staff_category,
 grade_point_id,
 change_grade_point_date,
 superannuation_flag,
 pay_no,
 termination_period_uom,
 contract_user_no,
 contract_user_post,
 position_type,
 contract_sign_date,
 contract_end_date,
 long_id,
 decision_type,
 dept_code,
 ticket_price,
 orig_end_date,
 pay_award_id,
 no_of_year,
 no_of_month,
 num_of_tickets,
 air_line,
 notice_no,
 notice_date,
 copy_contract_flag,
 route,
 assisment_receive,
 contract_status,
 service_days,
 severance,
 last_working_date,
 effective_date,
 ticket_payment_type,
 final_payment_status,
 final_payment_doc_no,
 final_payment_doc_date,
 occupational_change,
 entitle_for_annual_increment,
 sho_contract,
 orientation,
 security_clearance,
 calssification,
 saudi_council,
 transport_flag,
 freeze_incr_end_date,
 career_ladder_code,
 hospital_id,
 service_id,
 renewal_service_id,
 probation_period,
 staff_evaluation_id,
 offboarding_service_id,
 leave_pay_days,
 branch_id,
 insert_at,
 ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) as recorded_updated_at,
 merge_hash
from {{ iceberg_source('staff_contracts') }}
{% if is_incremental() %}
where ifNull(recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)) > (select max(recorded_updated_at) from {{ this }})
{% endif %}
