"""Null-column coercion must read the TARGET table's stored types, even when a
sibling Iceberg table in the dataset is broken.

``_coerce_unified_nulls`` opened ``get_iceberg_tables(pipeline)`` with no table
name -> every table in the dataset. One broken table (e.g.
``api_pre_approval_req_details``) made that read raise, so all-null columns fell
back to ``string``. A column stored as ``double`` then failed dlt's
``evolve_table`` with "Cannot promote string to double", failing the whole load.
Opening only the target table isolates the broken sibling.
"""
from __future__ import annotations

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, TableDef


def _tdef() -> TableDef:
    return TableDef(
        table="OASIS.LAB_IOS", unique_key="IOS", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


class _StoredTable:
    def schema(self) -> Schema:
        return Schema(
            NestedField(1, "ios", LongType(), required=False),
            NestedField(2, "default_dual_code", DoubleType(), required=False),
        )


def test_coerce_nulls_uses_target_type_despite_broken_sibling(monkeypatch):
    import dlt.common.libs.pyiceberg as ice

    calls = []

    def fake_get_iceberg_tables(pipeline, *names):
        calls.append(names)
        # No name => open ALL tables => trips over the broken sibling and raises.
        if not names:
            raise RuntimeError("api_pre_approval_req_details")
        return {names[0]: _StoredTable()}

    monkeypatch.setattr(ice, "get_iceberg_tables", fake_get_iceberg_tables)

    # DEFAULT_DUAL_CODE is all-null this run but stored as double.
    schema = pa.schema([("ios", pa.int64()), ("DEFAULT_DUAL_CODE", pa.null())])
    out = iceberg_load._coerce_unified_nulls(object(), _tdef(), schema)

    # Coerced to the stored double, NOT the unsafe string fallback.
    assert out.field("DEFAULT_DUAL_CODE").type == pa.float64()
    # And it asked only for the target table, never for the whole dataset.
    assert calls and all(names == ("lab_ios",) for names in calls)
