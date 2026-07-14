"""RunManager.has_live_run: running counts; detached only with a live PID."""
from __future__ import annotations

import threading


def _mgr_with(monkeypatch, runs, alive_pids=()):
    import pipeline_runner as pr

    mgr = pr.RunManager.__new__(pr.RunManager)  # skip __init__ disk I/O
    mgr._lock = threading.RLock()
    mgr._runs = {r["id"]: r for r in runs}
    mgr._procs = {}
    monkeypatch.setattr(pr, "_pid_alive", lambda pid: pid in alive_pids)
    return mgr


def test_running_is_live(monkeypatch):
    mgr = _mgr_with(monkeypatch, [{"id": "a", "status": "running", "pid": 1, "started_at": ""}])
    assert mgr.has_live_run()["id"] == "a"


def test_detached_dead_pid_is_not_live(monkeypatch):
    mgr = _mgr_with(monkeypatch, [{"id": "a", "status": "detached", "pid": 1, "started_at": ""}])
    assert mgr.has_live_run() is None


def test_detached_live_pid_is_live(monkeypatch):
    mgr = _mgr_with(monkeypatch, [{"id": "a", "status": "detached", "pid": 1, "started_at": ""}],
                    alive_pids={1})
    assert mgr.has_live_run()["id"] == "a"


def test_finished_is_not_live(monkeypatch):
    mgr = _mgr_with(monkeypatch, [{"id": "a", "status": "finished", "pid": 1, "started_at": ""}])
    assert mgr.has_live_run() is None
