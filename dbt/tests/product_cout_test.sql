-- product_cout_test: a singular data test. It must return zero rows to pass.
select *
from {{ ref('products_count') }}
where product_code is null
