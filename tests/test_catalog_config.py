import os

import pytest


@pytest.mark.skipif(not os.environ.get("OASIS_TEST_PG_DSN"),
                    reason="requires a Postgres catalog DB")
def test_get_catalog_returns_sql_catalog_on_postgres():
    from dlt.common.libs.pyiceberg import get_catalog
    from pyiceberg.catalog.sql import SqlCatalog

    dsn = os.environ["OASIS_TEST_PG_DSN"]
    cat = get_catalog(
        iceberg_catalog_name="oasis",
        iceberg_catalog_type="sql",
        iceberg_catalog_config={"type": "sql", "uri": dsn, "warehouse": "file:///tmp/wh_test"},
    )
    assert isinstance(cat, SqlCatalog)
    assert cat.properties["uri"].startswith("postgresql")
