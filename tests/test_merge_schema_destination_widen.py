"""An incremental merge must resolve a drifting key column to the SAME type the
initial (all-branches) load already wrote to disk, not to whatever this run's
branch subset happens to infer.

Root cause: a merge key like ``RULE_IOS`` is run-stabilised per batch by
``_coerce_keys_run_stable`` -- a *fractional* branch becomes ``string`` (canonical
decimal string), an *integral* branch becomes ``decimal(38, 0)``. ``unify_schemas``
picks "string wins", so an INITIAL run (which covers every branch, at least one
fractional) writes the column as ``string``. An INCREMENTAL run covering only
integral branches unifies to ``decimal(38, 0)`` and dies at load with
``Cannot change column type: rule_ios: string -> decimal(38, 0)`` -- Iceberg does
not allow string -> decimal.

The merge path must widen its unified schema by the destination table's stored
types (exactly the all-branches result the initial load crystallised), so the
integral batch is cast decimal -> string just as the initial load cast it, keeping
the merge-key hash identical and asking Iceberg for no (disallowed) type change.
"""
from __future__ import annotations

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from etl import iceberg_load
from etl.config import CATEGORY_TRANSACTION, TableDef


def _tdef() -> TableDef:
    return TableDef(
        table="OASIS.CONTRACT_RULES", unique_key="CONTRACT_NO,RULE_IOS",
        cdc_column="AMEND_LAST_DATE", where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_TRANSACTION)


class _StoredTable:
    """Destination as the initial load wrote it: RULE_IOS is a string key."""

    def schema(self) -> Schema:
        return Schema(
            NestedField(1, "contract_no", LongType(), required=False),
            NestedField(2, "rule_ios", StringType(), required=False),
            NestedField(3, "branch_id", LongType(), required=False),
        )


def _run_schema() -> pa.Schema:
    # This run covers only integral branches, so RULE_IOS came back decimal(38, 0).
    return pa.schema([
        ("contract_no", pa.int64()),
        ("rule_ios", pa.decimal128(38, 0)),
        ("branch_id", pa.int64()),
    ])


def test_merge_widens_key_to_destination_string(monkeypatch):
    import dlt.common.libs.pyiceberg as ice

    calls = []

    def fake_get_iceberg_tables(pipeline, *names):
        calls.append(names)
        # No name => open ALL tables => a single broken sibling would raise.
        if not names:
            raise RuntimeError("broken sibling table")
        return {names[0]: _StoredTable()}

    monkeypatch.setattr(ice, "get_iceberg_tables", fake_get_iceberg_tables)

    out = iceberg_load._widen_schema_to_destination(object(), _tdef(), _run_schema())

    # The drifting key adopts the on-disk string; other columns are untouched.
    assert out.field("rule_ios").type == pa.string()
    assert out.field("contract_no").type == pa.int64()
    assert out.field("branch_id").type == pa.int64()
    # Only the target table was opened, never the whole dataset.
    assert calls and all(names == ("contract_rules",) for names in calls)


def test_merge_widen_noop_when_types_already_match(monkeypatch):
    import dlt.common.libs.pyiceberg as ice

    class _MatchingStored:
        def schema(self) -> Schema:
            return Schema(
                NestedField(1, "contract_no", LongType(), required=False),
                NestedField(2, "rule_ios", StringType(), required=False),
                NestedField(3, "branch_id", LongType(), required=False),
            )

    monkeypatch.setattr(
        ice, "get_iceberg_tables",
        lambda pipeline, *names: {names[0]: _MatchingStored()} if names else {})

    run = pa.schema([
        ("contract_no", pa.int64()),
        ("rule_ios", pa.string()),
        ("branch_id", pa.int64()),
    ])
    out = iceberg_load._widen_schema_to_destination(object(), _tdef(), run)
    assert out.equals(run)


def test_merge_widen_returns_schema_when_table_absent(monkeypatch):
    import dlt.common.libs.pyiceberg as ice

    # First incremental of a table that does not exist yet, or an unreadable one:
    # nothing to widen against -> return the run schema unchanged (best effort).
    monkeypatch.setattr(
        ice, "get_iceberg_tables", lambda pipeline, *names: {})
    run = _run_schema()
    out = iceberg_load._widen_schema_to_destination(object(), _tdef(), run)
    assert out.equals(run)


def test_merge_widen_survives_read_error(monkeypatch):
    import dlt.common.libs.pyiceberg as ice

    def boom(pipeline, *names):
        raise RuntimeError("catalog unreachable")

    monkeypatch.setattr(ice, "get_iceberg_tables", boom)
    run = _run_schema()
    out = iceberg_load._widen_schema_to_destination(object(), _tdef(), run)
    # Best effort: a dest read failure must never fail the load.
    assert out.equals(run)
