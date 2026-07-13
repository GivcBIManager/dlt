-- products_count_test: a singular data test. It must return zero rows to pass.
select *
from {{ ref('products_count') }}
where branch_id is null