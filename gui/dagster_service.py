"""Supervise a local Dagster instance (webserver + daemon) for the GUI.

Launches one combined process — ``python -m dagster dev -m orchestrator.definitions``
— sharing an absolute DAGSTER_HOME with a generated dagster.yaml (run-queue
concurrency limit). Cross-platform process-group handling mirrors
pipeline_runner.py so the whole tree can be killed on Windows and POSIX.
"""
from __future__ import annotations

import os
import signal
import subprocess
import threading
from pathlib import Path
from typing import Any

import config

_DAGSTER_YAML = """\
run_queue:
  max_concurrent_runs: 1
telemetry:
  enabled: false
"""


class DagsterService:
    def __init__(self) -> None:
        self._proc: subprocess.Popen | None = None
        self._lock = threading.RLock()
        self._log_fh: "Any | None" = None

    # --- setup ------------------------------------------------------------ #
    def ensure_home(self) -> Path:
        home = config.DAGSTER_HOME
        home.mkdir(parents=True, exist_ok=True)
        yaml = home / "dagster.yaml"
        if not yaml.exists():
            yaml.write_text(_DAGSTER_YAML, encoding="utf-8")
        return home

    def launch_argv(self) -> list[str]:
        return [
            config.python_executable(), "-m", "dagster", "dev",
            "-m", "orchestrator.definitions",
            "-h", config.dagster_host(), "-p", str(config.dagster_port()),
        ]

    # --- lifecycle -------------------------------------------------------- #
    def is_running(self) -> bool:
        with self._lock:
            return self._proc is not None and self._proc.poll() is None

    def start(self) -> dict[str, Any]:
        with self._lock:
            if self.is_running():
                return self.status()
            self.ensure_home()
            config.LOG_DIR.mkdir(parents=True, exist_ok=True)
            env = dict(os.environ)
            env["DAGSTER_HOME"] = str(config.DAGSTER_HOME)
            env.setdefault("OASIS_DAGSTER_HOST", config.dagster_host())
            env.setdefault("OASIS_DAGSTER_PORT", str(config.dagster_port()))
            kwargs: dict[str, Any] = {}
            if os.name == "nt":
                kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                kwargs["start_new_session"] = True
            if self._log_fh is not None:
                try:
                    self._log_fh.close()
                except OSError:
                    pass
                self._log_fh = None
            log_path = config.LOG_DIR / "dagster.log"
            self._log_fh = open(log_path, "a", encoding="utf-8", buffering=1)
            self._proc = subprocess.Popen(
                self.launch_argv(), cwd=str(config.ORCHESTRATOR_DIR),
                stdout=self._log_fh, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL, env=env, **kwargs,
            )
        return self.status()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                self._proc = None
                return {"running": False}
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
                    pass
            self._proc = None
            if self._log_fh is not None:
                try:
                    self._log_fh.close()
                except OSError:
                    pass
                self._log_fh = None
        return {"running": False}

    def status(self) -> dict[str, Any]:
        with self._lock:
            running = self._proc is not None and self._proc.poll() is None
            pid = self._proc.pid if running else None
        return {
            "running": running,
            "pid": pid,
            "url": config.dagster_base_url(),
        }


service = DagsterService()
