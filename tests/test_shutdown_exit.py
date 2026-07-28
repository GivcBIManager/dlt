"""Process exit strategy around watchdog-abandoned commit threads.

A clean run must exit normally (``sys.exit`` -- atexit hooks, profilers and
coverage all run). But when a timed-out commit thread is still alive, normal
interpreter shutdown joins the executor workers wedged inside it forever
(concurrent.futures' exit hook joins every pool worker, daemon or not), so the
entry point must ``os._exit`` instead -- run 20260727-161529 hung 9h at 117%
CPU holding 68 GB exactly there.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

import oracle_to_iceberg
from etl import iceberg_load

REPO = Path(__file__).resolve().parents[1]


def test_clean_run_exits_via_sys_exit(monkeypatch):
    monkeypatch.setattr(iceberg_load, "_abandoned_commits", [])
    with pytest.raises(SystemExit) as exc:
        oracle_to_iceberg._shutdown_exit(0)
    assert exc.value.code == 0


def test_exit_code_is_preserved_on_clean_path(monkeypatch):
    monkeypatch.setattr(iceberg_load, "_abandoned_commits", [])
    with pytest.raises(SystemExit) as exc:
        oracle_to_iceberg._shutdown_exit(1)
    assert exc.value.code == 1


def test_hard_exits_when_an_abandoned_commit_wedges_an_executor():
    """Reproduce the real deadlock in a subprocess and prove we escape it.

    The wedged commit spawns a ThreadPoolExecutor whose worker never returns --
    the shape dlt's load step leaves behind when the watchdog abandons
    ``pipeline.run``. ``sys.exit`` would hang forever in the executor exit
    hook's join; ``_shutdown_exit`` must ``os._exit`` with the same code,
    promptly.
    """
    script = r"""
import threading
from concurrent.futures import ThreadPoolExecutor

import oracle_to_iceberg
from etl.iceberg_load import _run_with_timeout

pool_up = threading.Event()

def wedged_commit():
    pool = ThreadPoolExecutor(max_workers=1)
    pool.submit(threading.Event().wait)   # worker hangs forever
    pool_up.set()
    threading.Event().wait()              # the commit thread hangs too

try:
    _run_with_timeout(wedged_commit, timeout_s=0.2, label="wedge")
except TimeoutError:
    pass
assert pool_up.wait(10)
oracle_to_iceberg._shutdown_exit(7)
"""
    proc = subprocess.run([sys.executable, "-c", script], cwd=REPO,
                          capture_output=True, text=True, timeout=120)
    assert proc.returncode == 7, (proc.stdout, proc.stderr)
