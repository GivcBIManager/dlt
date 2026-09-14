{{ config(
    materialized      = 'table',
    engine            = 'MergeTree()',
    order_by = ['branch_id', 'id']
) }}

SELECT
    sm.branch_id AS branch_id,
    sm.staff_id  AS id,
    trimBoth(arrayStringConcat(arrayFilter(x -> x != '', [
        initcapUTF8(ifNull(sm.staff_name_1, '')),
        initcapUTF8(ifNull(sm.staff_name_2, '')),
        initcapUTF8(ifNull(sm.staff_name_3, '')),
        initcapUTF8(ifNull(sm.staff_name_family, ''))]), ' ')) AS staff_name_en,
    trimBoth(arrayStringConcat(arrayFilter(x -> x != '', [
        ifNull(sm.staff_name_1_b, ''),
        ifNull(sm.staff_name_2_b, ''),
        ifNull(sm.staff_name_3_b, ''),
        ifNull(sm.staff_name_familyb, '')]), ' ')) AS staff_name_ar,
    initcapUTF8(std.staff_type_description) AS staff_grade,
    initcapUTF8(stc.classificaton) AS classification,
    initcapUTF8(stc.categorynew)   AS category,
    initcapUTF8(stc.med_nonmed)    AS staff_type,
    initcapUTF8(p.position_type)   AS staff_post,
    p.work_entity,
    sm.employee_dependant_flag,
    initcapUTF8(cd.description) AS nationality,
    initcapUTF8(r.description)  AS religion,
    multiIf(initcapUTF8(ifNull(sm.sex, '')) = 'm', 'male',
            initcapUTF8(ifNull(sm.sex, '')) = 'f', 'female', '') AS gender,
    sm.date_of_birth AS date_of_birth,
    c.start_date       AS contract_start_date,
    c.end_date         AS contract_end_date,
    c.termination_date AS termination_date,
    sm.amend_last_date
FROM (
    SELECT
        toInt64(branch_id)  AS branch_id,
        staff_id   AS staff_id,
        toInt64(staff_type) AS staff_type,
        toInt64(nationality_code) AS nationality_code,
        toInt64(religion_code)    AS religion_code,
        staff_name_1, staff_name_2, staff_name_3, staff_name_family,
        staff_name_1_b, staff_name_2_b, staff_name_3_b, staff_name_familyb,
        employee_dependant_flag, sex, date_of_birth, amend_last_date
    FROM {{ iceberg_source('staff_master_data') }}
) AS sm
LEFT JOIN (
    SELECT toInt64(branch_id) AS branch_id,
           toInt64(staff_type) AS staff_type,
           staff_type_description
    FROM {{ iceberg_source('staff_types_data') }}
) AS std
       ON sm.branch_id  = std.branch_id
      AND sm.staff_type = std.staff_type
LEFT JOIN (
    SELECT toInt64(branch_id) AS branch_id,
           toInt64(staff_type) AS staff_type,
           classificaton, categorynew, med_nonmed
    FROM {{ iceberg_source('staff_type_classification') }}
    LIMIT 1 BY branch_id, staff_type
) AS stc
       ON sm.branch_id  = stc.branch_id
      AND sm.staff_type = stc.staff_type
LEFT JOIN (
    SELECT sp.branch_id AS branch_id,
           sp.staff_id  AS staff_id,
           pd.description AS position_type,
           sp.work_entity
    FROM (
        SELECT toInt64(branch_id) AS branch_id,
               staff_id  AS staff_id,
               toInt64(position_type) AS position_type,
               work_entity
        FROM {{ iceberg_source('staff_posts') }}
        ORDER BY date_started DESC
        LIMIT 1 BY branch_id, staff_id
    ) AS sp
    INNER JOIN (
        SELECT toInt64(branch_id) AS branch_id,
               toInt64(position_type) AS position_type,
               description
        FROM {{ iceberg_source('positions_data') }}
    ) AS pd
            ON sp.branch_id     = pd.branch_id
           AND sp.position_type = pd.position_type
) AS p
       ON sm.branch_id = p.branch_id
      AND sm.staff_id  = p.staff_id
LEFT JOIN (
    SELECT toInt64(branch_id) AS branch_id,
           staff_id  AS staff_id,
           start_date, end_date, termination_date
    FROM {{ iceberg_source('staff_contracts') }}
    ORDER BY start_date DESC
    LIMIT 1 BY branch_id, staff_id
) AS c
       ON sm.branch_id = c.branch_id
      AND sm.staff_id  = c.staff_id
LEFT JOIN (
    SELECT toInt64(branch_id) AS branch_id,
           toInt64(code) AS code,
           description
    FROM {{ iceberg_source('codes_data') }}
    WHERE toInt64(code_type) = 5
) AS cd
       ON sm.branch_id        = cd.branch_id
      AND sm.nationality_code = cd.code
LEFT JOIN (
    SELECT toInt64(branch_id) AS branch_id,
           toInt64(code) AS code,
           description
    FROM {{ iceberg_source('codes_data') }}
    WHERE toInt64(code_type) = 1
) AS r
       ON sm.branch_id     = r.branch_id
      AND sm.religion_code = r.code