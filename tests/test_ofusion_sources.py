"""Fusion warehouse -> dbt sources sync: path resolution, declarations, docs."""
import os

import pytest
import yaml

STAMP_OLD = "20260920T140015Z-944fce"
STAMP_NEW = "20260921T083725Z-fbf502"


def _make_table(root, domain, table, *stamps, empty=()):
    """Create ``<root>/<domain>/<table>/<stamp>/metadata/`` for each stamp.

    A stamp listed in ``empty`` gets the directory but no metadata file, which
    is what the Fusion pipeline leaves behind when a load dies part way.
    """
    for stamp in stamps:
        meta = root / domain / table / stamp / "metadata"
        meta.mkdir(parents=True)
        if stamp not in empty:
            (meta / "00000-abc.metadata.json").write_text("{}", encoding="utf-8")


@pytest.fixture
def proj(tmp_path, monkeypatch):
    import config
    import dbt_config

    dbt = tmp_path / "dbt"
    dbt.mkdir()
    (dbt / "dbt_project.yml").write_text("name: oasis\n", encoding="utf-8")

    wh = tmp_path / "ofusion_output"
    _make_table(wh, "finance", "fact_ap_payment", STAMP_OLD)
    # Reloaded from scratch: a newer stamp, and an older one left behind empty.
    _make_table(wh, "finance", "fact_gl_balance", STAMP_OLD, STAMP_NEW, empty=(STAMP_OLD,))
    _make_table(wh, "conformed", "dim_branch", STAMP_NEW)
    # Not a table: stamp directory exists but the load never wrote metadata.
    _make_table(wh, "conformed", "dim_aborted", STAMP_OLD, empty=(STAMP_OLD,))
    (wh / "_scratch").mkdir()  # pipeline's own folder: never a source

    monkeypatch.setattr(config, "OFUSION_ROOT", wh)
    monkeypatch.setattr(dbt_config, "dbt_dir", lambda: dbt)
    return {"dbt": dbt, "warehouse": wh}


def _sources(proj):
    import ofusion_sources
    data = yaml.safe_load(ofusion_sources.sources_path().read_text(encoding="utf-8"))
    return {s["name"]: {t["name"]: t for t in s["tables"]} for s in data["sources"]}


def test_declares_every_table_under_a_source_per_domain(proj):
    import ofusion_sources
    r = ofusion_sources.sync()
    assert r["added"] == ["conformed.dim_branch", "finance.fact_ap_payment",
                          "finance.fact_gl_balance"]
    assert r["domains"] == ["conformed", "finance"] and r["written"]
    sources = _sources(proj)
    assert sorted(sources) == ["ofusion_conformed", "ofusion_finance"]
    assert list(sources["ofusion_finance"]) == ["fact_ap_payment", "fact_gl_balance"]
    # A stamp directory with no Iceberg metadata is not a table.
    assert list(sources["ofusion_conformed"]) == ["dim_branch"]


def test_resolves_the_newest_stamp_that_holds_metadata(proj):
    import ofusion_sources
    ofusion_sources.sync()
    meta = _sources(proj)["ofusion_finance"]["fact_gl_balance"]["meta"]
    assert meta["run_stamp"] == STAMP_NEW
    assert meta["iceberg_path"] == str(
        proj["warehouse"] / "finance" / "fact_gl_balance" / STAMP_NEW)
    assert meta["domain"] == "finance"


def test_paths_macro_carries_every_resolved_path(proj):
    import ofusion_sources
    ofusion_sources.sync()
    text = ofusion_sources.paths_macro_path().read_text(encoding="utf-8")
    assert "{% macro ofusion_paths() %}" in text
    assert f"'finance.fact_gl_balance': '{proj['warehouse']}/finance/fact_gl_balance/{STAMP_NEW}'" in text
    assert "'conformed.dim_aborted'" not in text
    assert "GENERATED WHOLE by gui/ofusion_sources.py" in text


def test_a_new_stamp_moves_the_path(proj):
    """The warehouse is written outside this repo, so paths must track it."""
    import ofusion_sources
    ofusion_sources.sync()
    _make_table(proj["warehouse"], "finance", "fact_ap_payment", STAMP_NEW)
    r = ofusion_sources.sync()
    assert r["added"] == [] and r["changed"]
    meta = _sources(proj)["ofusion_finance"]["fact_ap_payment"]["meta"]
    assert meta["run_stamp"] == STAMP_NEW
    assert STAMP_NEW in ofusion_sources.paths_macro_path().read_text(encoding="utf-8")


def test_keeps_hand_written_docs_and_refreshes_derived_facts(proj):
    import ofusion_sources
    path = ofusion_sources.sources_path()
    path.parent.mkdir(parents=True)
    path.write_text("""\
version: 2
sources:
  - name: ofusion_finance
    description: Hand-written source description.
    tables:
      - name: fact_ap_payment
        description: >
          AP payments. One row per payment application, not per check.
        tags: ["finance", "ap"]
        meta:
          run_stamp: "STALE-STAMP"
          owner: finance
        columns:
          - name: invoice_payment_id
            description: The grain.
""", encoding="utf-8")
    ofusion_sources.sync()
    t = _sources(proj)["ofusion_finance"]["fact_ap_payment"]
    assert t["description"].strip() == (
        "AP payments. One row per payment application, not per check.")
    assert t["tags"] == ["finance", "ap"]             # a person's tags are untouched
    assert t["meta"]["run_stamp"] == STAMP_OLD        # refreshed from the warehouse
    assert t["meta"]["owner"] == "finance"            # a person's key survives
    assert t["columns"] == [{"name": "invoice_payment_id", "description": "The grain."}]
    text = path.read_text(encoding="utf-8")
    assert "description: Hand-written source description." in text
    assert "MAINTAINED BY gui/ofusion_sources.py" in text


def test_second_sync_changes_nothing(proj):
    import ofusion_sources
    ofusion_sources.sync()
    paths = [ofusion_sources.sources_path(), ofusion_sources.paths_macro_path()]
    for p in paths:
        os.utime(p, (1_700_000_000, 1_700_000_000))
    r = ofusion_sources.sync()
    assert r["added"] == [] and not r["changed"]
    assert all(p.stat().st_mtime == 1_700_000_000 for p in paths)


def test_table_gone_from_warehouse_is_kept_and_reported(proj):
    import ofusion_sources
    ofusion_sources.sync()
    (proj["warehouse"] / "conformed" / "dim_branch" / STAMP_NEW
     / "metadata" / "00000-abc.metadata.json").unlink()
    r = ofusion_sources.sync()
    assert r["stale"] == ["conformed.dim_branch"]
    t = _sources(proj)["ofusion_conformed"]["dim_branch"]
    assert "meta" not in t                       # the derived facts are no longer true
    assert "conformed.dim_branch" not in ofusion_sources.paths_macro_path().read_text(
        encoding="utf-8")


def test_check_mode_does_not_write(proj):
    import ofusion_sources
    r = ofusion_sources.sync(write=False)
    assert r["changed"] and not r["written"]
    assert not ofusion_sources.sources_path().exists()
    assert not ofusion_sources.paths_macro_path().exists()


def test_sync_safe_swallows_a_missing_warehouse(proj, monkeypatch):
    """Anyone working only on the oasis lake must still be able to run dbt."""
    import config
    import ofusion_sources
    monkeypatch.setattr(config, "OFUSION_ROOT", proj["warehouse"] / "gone")
    assert ofusion_sources.sync_safe() is None
    assert not ofusion_sources.sources_path().exists()
