"""dbt model metadata: comment-preserving edits, layer moves, orphan cleanup."""
import pytest

STAGING_YML = """\
version: 2

# Staging layer -- this comment is documentation and must survive a save.

models:

  - name: m1
    description: First model.
    config:
      tags: ["finance", "incremental"]
    columns:
      - name: branch_id
        description: Hospital branch.
        data_tests: [not_null]
"""


@pytest.fixture
def proj(tmp_path, monkeypatch):
    import dbt_config
    d = tmp_path / "dbt"
    for layer in ("staging", "intermediate", "marts"):
        (d / "models" / layer).mkdir(parents=True)
    (d / "models" / "staging" / "m1.sql").write_text("select 1", encoding="utf-8")
    (d / "models" / "staging" / "_staging__models.yml").write_text(STAGING_YML, encoding="utf-8")
    monkeypatch.setattr(dbt_config, "dbt_dir", lambda: d)
    return d


def test_describe_reads_layer_tags_and_columns(proj):
    import dbt_meta
    m = dbt_meta.describe("m1")
    assert m["layer"] == "staging"
    assert m["tags"] == ["finance", "incremental"]
    assert m["columns"] == [{"name": "branch_id", "description": "Hospital branch.",
                             "tests": ["not_null"]}]


def test_save_preserves_comments(proj):
    import dbt_meta
    m = dbt_meta.describe("m1")
    dbt_meta.save("m1", {**m, "tags": ["finance", "edited"]})
    text = (proj / "models" / "staging" / "_staging__models.yml").read_text()
    assert "this comment is documentation and must survive a save" in text
    assert dbt_meta.describe("m1")["tags"] == ["finance", "edited"]


def test_layer_tag_is_never_stored_per_model(proj):
    # The layer tag comes from dbt_project.yml; echoing it into the model's
    # own tags would duplicate it and go stale on the next layer move.
    import dbt_meta
    m = dbt_meta.describe("m1")
    dbt_meta.save("m1", {**m, "tags": ["staging", "finance"]})
    assert dbt_meta.describe("m1")["tags"] == ["finance"]


def test_layer_move_relocates_sql_and_yaml(proj):
    import dbt_meta
    m = dbt_meta.describe("m1")
    dbt_meta.save("m1", {**m, "layer": "marts"})
    assert (proj / "models" / "marts" / "m1.sql").exists()
    assert not (proj / "models" / "staging" / "m1.sql").exists()
    assert "m1" not in (proj / "models" / "staging" / "_staging__models.yml").read_text()
    moved = dbt_meta.describe("m1")
    assert moved["layer"] == "marts" and moved["description"] == "First model."


def test_unknown_layer_rejected(proj):
    import dbt_meta
    with pytest.raises(ValueError, match="unknown layer"):
        dbt_meta.save("m1", {"layer": "gold"})


def test_undocumented_model_gets_a_block(proj):
    import dbt_meta
    (proj / "models" / "intermediate" / "m2.sql").write_text("select 2", encoding="utf-8")
    assert dbt_meta.describe("m2")["documented"] is False
    dbt_meta.save("m2", {"layer": "intermediate", "description": "Second.", "tags": ["x"]})
    out = dbt_meta.describe("m2")
    assert out["documented"] is True and out["tags"] == ["x"]


def test_forget_removes_orphaned_block(proj):
    import dbt_meta
    (proj / "models" / "staging" / "m1.sql").unlink()
    assert dbt_meta.forget("m1") is True
    assert "m1" not in (proj / "models" / "staging" / "_staging__models.yml").read_text()
    assert dbt_meta.forget("m1") is False


# ---- lake folders: models/<lake>/<layer>/[<sub>/]<name>.sql -------------- #

LAKE_SOURCES_YML = """\
version: 2
sources:
  - name: lake
    tables:
      - name: t1
"""

LAKE_MODELS_YML = """\
version: 2

# Lake staging -- this comment must survive a save too.

models:
  - name: l1
    description: Lake model.
"""


@pytest.fixture
def lake(proj):
    # The schema file carries the lake's name, not the layer's, and shares the
    # folder with a sources file that must not be mistaken for it.
    d = proj / "models" / "lk" / "staging"
    (d / "area").mkdir(parents=True)
    (d / "area" / "l1.sql").write_text("select 1", encoding="utf-8")
    (d / "_lk__sources.yml").write_text(LAKE_SOURCES_YML, encoding="utf-8")
    (d / "_lk__models.yml").write_text(LAKE_MODELS_YML, encoding="utf-8")
    return proj


def test_describe_finds_nested_lake_model(lake):
    import dbt_meta
    m = dbt_meta.describe("l1")
    assert (m["lake"], m["layer"]) == ("lk", "staging")
    assert m["path"] == "models/lk/staging/area/l1.sql"
    assert m["schema_file"] == "models/lk/staging/_lk__models.yml"
    assert m["description"] == "Lake model."


def test_lake_save_writes_the_lake_schema_file(lake):
    import dbt_meta
    dbt_meta.save("l1", {**dbt_meta.describe("l1"), "tags": ["edited"]})
    text = (lake / "models" / "lk" / "staging" / "_lk__models.yml").read_text()
    assert "this comment must survive a save too" in text and "edited" in text
    assert "edited" not in (lake / "models" / "lk" / "staging" / "_lk__sources.yml").read_text()
    assert not (lake / "models" / "staging" / "_staging__models.yml").read_text().count("l1")


def test_lake_move_stays_in_lake_and_subfolder(lake):
    import dbt_meta
    m = dbt_meta.describe("l1")
    assert m["moves"]["marts"]["path"] == "models/lk/marts/area/l1.sql"
    dbt_meta.save("l1", {**m, "layer": "marts"})
    assert (lake / "models" / "lk" / "marts" / "area" / "l1.sql").exists()
    assert not (lake / "models" / "marts" / "l1.sql").exists()
    assert "l1" not in (lake / "models" / "lk" / "staging" / "_lk__models.yml").read_text()
    moved = dbt_meta.describe("l1")
    assert moved["schema_file"] == "models/lk/marts/_marts__models.yml"
    assert moved["description"] == "Lake model."


def test_forget_and_overview_cover_lake_models(lake):
    import dbt_meta
    rows = {r["name"]: r for r in dbt_meta.overview()}
    assert rows["l1"]["lake"] == "lk" and rows["l1"]["described"] is True
    assert rows["m1"]["lake"] == ""
    (lake / "models" / "lk" / "staging" / "area" / "l1.sql").unlink()
    assert dbt_meta.forget("l1") is True
    assert "l1" not in (lake / "models" / "lk" / "staging" / "_lk__models.yml").read_text()
