"""Launch, track and stream pipeline runs.

Each run is a child process whose combined stdout/stderr is redirected to a log
file under ``run_logs/``. A small JSON registry survives restarts so the run
history is still visible after the GUI is bounced; processes that outlived the
GUI are detected by PID liveness.
"""

from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from config import LOG_DIR, REPO_ROOT, RUNS_REGISTRY, ensure_dirs

_MAX_HISTORY = 200


def _pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        if os.name == "nt":
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=5,
            )
            return str(pid) in out.stdout
        os.kill(pid, 0)
        return True
    except (OSError, subprocess.SubprocessError):
        return False


class RunManager:
    def __init__(self) -> None:
        ensure_dirs()
        self._lock = threading.RLock()
        self._runs: dict[str, dict[str, Any]] = {}
        self._procs: dict[str, subprocess.Popen] = {}
        self._load()

    # --- persistence ------------------------------------------------------ #
    def _load(self) -> None:
        if not RUNS_REGISTRY.exists():
            return
        try:
            data = json.loads(RUNS_REGISTRY.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return
        for run in data:
            # We no longer own these processes; reconcile their state from PID.
            if run.get("status") == "running":
                run["status"] = "detached" if _pid_alive(run.get("pid")) else "unknown"
            self._runs[run["id"]] = run

    def _save(self) -> None:
        runs = sorted(self._runs.values(), key=lambda r: r["started_at"], reverse=True)[:_MAX_HISTORY]
        tmp = RUNS_REGISTRY.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(runs, indent=2), encoding="utf-8")
        tmp.replace(RUNS_REGISTRY)

    # --- lifecycle -------------------------------------------------------- #
    def start(self, argv: list[str], label: str = "") -> dict[str, Any]:
        run_id = datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6]
        log_path = LOG_DIR / f"run-{run_id}.log"
        started = datetime.now().isoformat(timespec="seconds")

        header = (
            f"# OASIS run {run_id}\n"
            f"# label   : {label}\n"
            f"# command : {' '.join(shlex.quote(a) for a in argv)}\n"
            f"# started : {started}\n"
            f"# cwd     : {REPO_ROOT}\n"
            f"{'-' * 70}\n"
        )
        log_fh = open(log_path, "w", encoding="utf-8", buffering=1)
        log_fh.write(header)
        log_fh.flush()

        kwargs: dict[str, Any] = {}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True  # own process group -> killpg

        env = dict(os.environ)
        env.setdefault("PYTHONUNBUFFERED", "1")
        try:
            proc = subprocess.Popen(
                argv, cwd=str(REPO_ROOT), stdout=log_fh,
                stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, **kwargs,
            )
        except OSError as exc:
            log_fh.write(f"\n[runner] failed to start: {exc}\n")
            log_fh.close()
            raise

        run = {
            "id": run_id,
            "label": label,
            "command": " ".join(shlex.quote(a) for a in argv),
            "argv": argv,
            "log": log_path.name,
            "pid": proc.pid,
            "status": "running",
            "returncode": None,
            "started_at": started,
            "ended_at": None,
        }
        with self._lock:
            self._runs[run_id] = run
            self._procs[run_id] = proc
            self._save()
        threading.Thread(target=self._wait, args=(run_id, proc, log_fh), daemon=True).start()
        return run

    def _wait(self, run_id: str, proc: subprocess.Popen, log_fh) -> None:
        rc = proc.wait()
        try:
            log_fh.write(f"\n{'-' * 70}\n[runner] exited with code {rc}\n")
            log_fh.flush()
            log_fh.close()
        except OSError:
            pass
        with self._lock:
            run = self._runs.get(run_id)
            if run:
                run["status"] = "finished" if rc == 0 else "failed"
                run["returncode"] = rc
                run["ended_at"] = datetime.now().isoformat(timespec="seconds")
                self._save()
            self._procs.pop(run_id, None)

    def stop(self, run_id: str) -> bool:
        with self._lock:
            proc = self._procs.get(run_id)
            run = self._runs.get(run_id)
        if proc is None:
            # Possibly a detached run from a previous session.
            if run and _pid_alive(run.get("pid")):
                try:
                    os.kill(run["pid"], signal.SIGTERM)
                    return True
                except OSError:
                    return False
            return False
        try:
            if os.name == "nt":
                proc.send_signal(signal.CTRL_BREAK_EVENT)
                proc.terminate()
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (OSError, ProcessLookupError):
            try:
                proc.terminate()
            except OSError:
                return False
        with self._lock:
            if run:
                run["status"] = "stopped"
                run["ended_at"] = datetime.now().isoformat(timespec="seconds")
                self._save()
        return True

    # --- queries ---------------------------------------------------------- #
    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return sorted(self._runs.values(), key=lambda r: r["started_at"], reverse=True)

    def get(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._runs.get(run_id)

    def active_count(self) -> int:
        with self._lock:
            return sum(1 for r in self._runs.values() if r["status"] in ("running", "detached"))

    def tail(self, run_id: str, offset: int = 0) -> dict[str, Any]:
        """Return new log bytes since ``offset`` plus the run's current status."""
        run = self.get(run_id)
        if run is None:
            raise KeyError(run_id)
        path = LOG_DIR / run["log"]
        chunk, size = "", offset
        if path.exists():
            size = path.stat().st_size
            if offset < size:
                with path.open("rb") as fh:
                    fh.seek(offset)
                    chunk = fh.read().decode("utf-8", errors="replace")
        return {
            "offset": size,
            "chunk": chunk,
            "status": run["status"],
            "returncode": run.get("returncode"),
        }
