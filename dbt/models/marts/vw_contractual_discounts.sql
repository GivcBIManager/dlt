{{ config(materialized='view') }}
SELECT
    branch_id,
    substring(doc_no,1,length(doc_no) - 1) AS discount_doc_no,
    doc_no AS org_doc_no,
    CAST(doc_date,'DATE') AS doc_date,
    sum(total_doc_price) AS discount
FROM {{ ref('fact_doc') }}
WHERE endsWith(doc_no,'D') and doc_type ='CREDITAR'
GROUP BY ALL
HAVING sum(total_doc_price) != 0
ORDER BY
    1 ASC,
    3 ASC
