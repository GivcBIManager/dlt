{#
    unique_grain_per_version(combination_of_columns, version_column)

    Grain test for a ReplacingMergeTree model -- the one that can actually fail.

    NOT `unique_combination_final`: that test reads the model with FINAL, and
    FINAL collapses rows on the sorting key. So when the column list IS the
    sorting key (which is what that test asks for), the result is a tautology --
    ClickHouse has already made the key unique by throwing rows away, and the
    test passes however wrong the key was. Verified on a probe table: two rows
    sharing a key but differing in payload collapse to one, and the FINAL test
    reports zero failures.

    What can go wrong is narrower and worth asserting: two DIFFERENT rows that
    share both the sorting key and the version column. ReplacingMergeTree keeps
    one arbitrarily, so the other is lost and no later merge can recover it.
    Rows that share a key across DIFFERENT versions are not a problem -- that is
    ordinary history, and collapsing it is the point of the engine.

    So this reads the model WITHOUT final and looks for key+version collisions.

    Usage (model level):

        data_tests:
          - unique_grain_per_version:
              arguments:
                combination_of_columns: [branch_id, appointment_id]
#}
{% test unique_grain_per_version(model, combination_of_columns, version_column='recorded_updated_at') %}

{%- set cols = combination_of_columns | reject('equalto', version_column) | list %}
{%- set grouped = cols + [version_column] %}
{%- set expr = grouped | join(', ') %}

select
    {{ expr }},
    count(*) as n_records
from {{ model }}
group by {{ expr }}
having count(*) > 1

{% endtest %}
