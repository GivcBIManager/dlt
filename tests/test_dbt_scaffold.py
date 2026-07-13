"""The dbt project scaffold exists and is coherent."""
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_dbt_project_yml_names_oasis_profile():
    text = (REPO / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert "name: 'oasis'" in text or 'name: "oasis"' in text
    assert "profile: 'oasis'" in text or 'profile: "oasis"' in text


def test_a_model_demonstrates_iceberglocal_and_warns():
    # A model demonstrating the icebergLocal(...) -> ClickHouse pattern (with the
    # operator-path warning) must exist; scan models rather than hard-coding a
    # filename so a model rename doesn't stale this test.
    texts = [p.read_text(encoding="utf-8")
             for p in (REPO / "dbt" / "models").glob("*.sql")]
    assert texts, "expected at least one dbt model in dbt/models/"
    assert any("icebergLocal(" in t for t in texts), \
        "expected a model demonstrating the icebergLocal(...) path"
    assert any("CLICKHOUSE" in t.upper() for t in texts), \
        "expected the ClickHouse operator-path warning comment"


def test_requirements_pin_dbt():
    reqs = (REPO / "requirements-gui.txt").read_text(encoding="utf-8")
    assert "dbt-core" in reqs and "dbt-clickhouse" in reqs


def test_gitignore_excludes_generated_dbt_artifacts():
    ig = (REPO / ".gitignore").read_text(encoding="utf-8")
    for pat in ("dbt/profiles.yml", "dbt/target/", "dbt/logs/", "dbt/dbt_packages/"):
        assert pat in ig
