"""Offset-based log tailing so the Monitor page transfers only new bytes."""
from __future__ import annotations

import pytest


@pytest.fixture
def logdir(tmp_path, monkeypatch):
    import config
    import workspace
    monkeypatch.setattr(config, "LOG_DIR", tmp_path)
    monkeypatch.setattr(workspace, "LOG_DIR", tmp_path)
    return tmp_path


def test_tail_from_zero_returns_whole_file(logdir):
    import workspace
    # bytes, not write_text: real logs are written raw (no \n -> \r\n on Windows)
    (logdir / "a.log").write_bytes(b"hello\nworld\n")
    out = workspace.tail_log_file("a.log", 0)
    assert out["chunk"] == "hello\nworld\n"
    assert out["offset"] == len(b"hello\nworld\n")


def test_tail_returns_only_new_bytes(logdir):
    import workspace
    p = logdir / "a.log"
    p.write_bytes(b"line1\n")
    first = workspace.tail_log_file("a.log", 0)
    with p.open("ab") as fh:
        fh.write(b"line2\n")
    out = workspace.tail_log_file("a.log", first["offset"])
    assert out["chunk"] == "line2\n"
    assert out["offset"] == p.stat().st_size


def test_tail_no_new_bytes_is_empty(logdir):
    import workspace
    p = logdir / "a.log"
    p.write_text("only\n", encoding="utf-8")
    size = p.stat().st_size
    out = workspace.tail_log_file("a.log", size)
    assert out["chunk"] == ""
    assert out["offset"] == size


def test_tail_from_zero_truncates_large_file(logdir):
    import workspace
    p = logdir / "big.log"
    p.write_bytes(b"X" * 100)
    out = workspace.tail_log_file("big.log", 0, max_bytes=10)
    assert out["truncated"] is True
    assert out["chunk"].startswith("...[truncated]...")
    assert out["offset"] == 100


def test_tail_rotated_file_restarts(logdir):
    import workspace
    p = logdir / "a.log"
    p.write_bytes(b"aaaa\n")
    # caller's offset is past the current (rotated/smaller) file
    out = workspace.tail_log_file("a.log", 9999)
    assert out["chunk"] == "aaaa\n"
    assert out["offset"] == p.stat().st_size


def test_tail_path_traversal_rejected(logdir):
    import workspace
    with pytest.raises(FileNotFoundError):
        workspace.tail_log_file("../secret.toml", 0)


def test_log_route_forwards_offset(monkeypatch):
    import app as gui_app
    seen = {}

    def fake(name, offset=0):
        seen.update(name=name, offset=offset)
        return {"name": name, "offset": 10, "chunk": "new", "truncated": False}

    monkeypatch.setattr(gui_app.workspace, "tail_log_file", fake)
    resp = gui_app.app.test_client().get("/api/logs/foo.log?offset=5")
    assert resp.status_code == 200
    assert seen["offset"] == 5
    assert resp.get_json()["chunk"] == "new"

