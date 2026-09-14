-- new_test: a singular data test. It must return zero rows to pass.
select branch_id, id, count() records
from {{ ref('master_staff') }}
group by 1,2
having count()>1
