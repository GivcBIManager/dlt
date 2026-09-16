"""Lake -> dbt sources sync: declares every lake table, keeps people's docs."""
import os

import pytest
import yaml

TABLES_JSON = {
    "masters": [{"table": "OASIS.CODES_DATA", "unique_key": "CODE", "cdc_column": "AMEND_LAST_DATE"}],
    "transactions": [{"table": "DEVDBA.DOCL", "unique_key": "LINE_ID"}],
    "snapshots": [],
}


@pytest.fixture
def proj(tmp_path, monkeypatch):
    import config
    import dbt_config
    import tables_store

    dbt = tmp_path / "dbt"
    dbt.mkdir()
    (dbt / "dbt_project.yml").write_text("name: oasis\n", encoding="utf-8")
    lake = tmp_path / "lake"
    for name in ("codes_data", "docl", "etl_control"):
        (lake / name / "metadata").mkdir(parents=True)
    (lake / "_dlt_loads").mkdir()          # dlt state: no Iceberg metadata
    (lake / "scratch").mkdir()             # not a table: no metadata/
    (lake / "init").write_text("", encoding="utf-8")

    doc = {k: [dict(e) for e in v] for k, v in TABLES_JSON.items()}
    monkeypatch.setattr(config, "ICEBERG_ROOT", lake)
    monkeypatch.setattr(dbt_config, "dbt_dir", lambda: dbt)
    monkeypatch.setattr(tables_store, "load_raw", lambda: doc)
    return {"dbt": dbt, "lake": lake, "doc": doc}


def _tables(proj):
    import dbt_sources
    data = yaml.safe_load(dbt_sources.sources_path().read_text(encoding="utf-8"))
    (src,) = data["sources"]
    assert src["name"] == "oasis_lake"
    return {t["name"]: t for t in src["tables"]}


def test_declares_every_lake_table_and_nothing_else(proj):
    import dbt_sources
    r = dbt_sources.sync()
    assert r["added"] == ["codes_data", "docl"] and r["written"]
    tables = _tables(proj)
    assert list(tables) == ["codes_data", "docl"]
    assert tables["codes_data"]["tags"] == ["master"]
    assert tables["codes_data"]["meta"] == {
        "oracle_table": "OASIS.CODES_DATA", "load_type": "master",
        "unique_key": "CODE", "cdc_column": "AMEND_LAST_DATE"}
    assert tables["docl"]["description"] == (
        "Oracle DEVDBA.DOCL, loaded by the dlt pipeline as a transaction table. Not yet documented.")


def test_table_without_tables_json_entry_is_still_declared(proj):
    import dbt_sources
    (proj["lake"] / "adhoc" / "metadata").mkdir(parents=True)
    dbt_sources.sync()
    adhoc = _tables(proj)["adhoc"]
    assert adhoc["description"].startswith("Iceberg table with no tables.json entry")
    assert "meta" not in adhoc and "tags" not in adhoc


def test_keeps_hand_written_docs_and_refreshes_derived_facts(proj):
    import dbt_sources
    dbt_sources.sources_path().parent.mkdir(parents=True)
    dbt_sources.sources_path().write_text("""\
version: 2
sources:
  - name: oasis_lake
    description: Hand-written source description.
    tables:
      - name: codes_data
        description: >
          Generic code lookup. Filter on code_type first.
        tags: ["transaction", "lookup"]
        meta:
          oracle_table: OASIS.CODES_DATA
          unique_key: "OLD_KEY"
          owner: finance
        columns:
          - name: code
            description: The code.
""", encoding="utf-8")
    dbt_sources.sync()
    t = _tables(proj)["codes_data"]
    assert t["description"].strip() == "Generic code lookup. Filter on code_type first."
    assert t["tags"] == ["master", "lookup"]          # load type corrected, own tag kept
    assert t["meta"]["unique_key"] == "CODE"          # refreshed from tables.json
    assert t["meta"]["owner"] == "finance"            # a person's key survives
    assert t["columns"] == [{"name": "code", "description": "The code."}]
    text = dbt_sources.sources_path().read_text(encoding="utf-8")
    assert "description: Hand-written source description." in text
    assert "MAINTAINED BY gui/dbt_sources.py" in text


def test_generated_description_follows_tables_json(proj):
    import dbt_sources
    dbt_sources.sync()
    proj["doc"]["transactions"][0]["table"] = "OASIS.DOCL"
    dbt_sources.sync()
    assert _tables(proj)["docl"]["description"].startswith("Oracle OASIS.DOCL,")


def test_second_sync_changes_nothing(proj):
    import dbt_sources
    dbt_sources.sync()
    path = dbt_sources.sources_path()
    os.utime(path, (1_700_000_000, 1_700_000_000))
    r = dbt_sources.sync()
    assert r["added"] == [] and not r["changed"]
    assert path.stat().st_mtime == 1_700_000_000


def test_table_gone_from_lake_is_kept_and_reported(proj):
    import dbt_sources
    dbt_sources.sync()
    (proj["lake"] / "docl" / "metadata").rmdir()
    r = dbt_sources.sync()
    assert r["stale"] == ["docl"]
    assert "docl" in _tables(proj)


def test_check_mode_does_not_write(proj):
    import dbt_sources
    r = dbt_sources.sync(write=False)
    assert r["changed"] and not r["written"]
    assert not dbt_sources.sources_path().exists()
    assert dbt_sources.main(["--check"]) == 1


def test_sync_safe_skips_non_projects_and_never_raises(proj, monkeypatch):
    import config
    import dbt_sources
    monkeypatch.setattr(config, "ICEBERG_ROOT", proj["lake"] / "missing")
    assert dbt_sources.sync_safe() is None            # unreadable lake: logged, not raised
    (proj["dbt"] / "dbt_project.yml").unlink()
    monkeypatch.setattr(config, "ICEBERG_ROOT", proj["lake"])
    assert dbt_sources.sync_safe() is None            # not a dbt project: untouched
    assert not dbt_sources.sources_path().exists()
