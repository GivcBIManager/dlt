{#
    unique_combination_final(combination_of_columns, final=true)

    Model-level uniqueness test for the grain of a model.

    Why not the stock `unique` test: the incremental models here are
    ReplacingMergeTree + incremental_strategy='append'. dbt appends the new
    batch and ClickHouse collapses older versions of a key only when it merges
    parts, which happens in the background at an unpredictable time. So
    immediately after a run the table legitimately holds several rows per key,
    and a stock `unique` test fails on data that is actually fine. (Measured on
    fact_doc: 122,709,914 rows stored vs 122,347,235 under FINAL -- ~363k rows
    were awaiting a merge.)

    `FINAL` forces that collapse at read time, so this asserts the grain the
    model is *defined* at rather than the physical row count at this instant.

    Pass `final: false` for models on a plain MergeTree engine. ClickHouse does
    NOT treat FINAL as a harmless no-op there -- it raises
    "Storage MergeTree doesn't support FINAL" (error 181). master_staff is the
    one such model in this project; it is materialized as a full table and
    rebuilt each run, so it needs no dedup pass anyway.

    The column list must match the engine's ORDER BY key for ReplacingMergeTree
    models, since that key is what ClickHouse dedups on.

    Usage (model level, not column level):

        data_tests:
          - unique_combination_final:
              arguments:
                combination_of_columns: [branch_id, appointment_id]
                final: true
#}
{% test unique_combination_final(model, combination_of_columns, final=true) %}

{%- set cols = combination_of_columns | join(', ') %}

select
    {{ cols }},
    count(*) as n_records
from {{ model }}{% if final %} final{% endif %}

group by {{ cols }}
having count(*) > 1

{% endtest %}
