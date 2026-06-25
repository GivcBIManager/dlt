"""Live progress + memory probe for a running pipeline.

Two jobs, both designed to cost ~nothing on the hot path:

* **Progress** -- a compact heartbeat (tables loaded, extract units done, rows,
  current/peak memory) printed every ``interval_s`` from a single daemon thread.
  The extraction/load code only ever bumps a couple of integer counters under a
  short lock; it never measures anything itself.

* **Memory probe** -- answers "what was the peak, and what was running when we hit
  it". The *peak* numbers are exact regardless of how often we sample, because
  they come from OS/Arrow high-water counters (Windows ``PeakWorkingSetSize`` and
  the pyarrow memory pool's ``max_memory``), not from the samples. Sampling (1s)
  only attributes each new high-water mark to whatever ``activity`` label is set,
  so the summary can say e.g. the peak landed during ``load:appointments`` (the
  whole-file ``pq.read_table`` + cast) rather than during ``extract`` (per-branch
  fetch buffers / the cursor fallback). That attribution is what tells us which of
  the suspected contributors actually dominates on real data.
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

import pyarrow as pa

log = logging.getLogger("etl.progress")

_SAMPLE_INTERVAL_S = 1.0  # peak attribution granularity (cheap O(1) reads)


# --------------------------------------------------------------------------- #
# Memory readers (no third-party deps; psutil used only if already installed)
# --------------------------------------------------------------------------- #
def _make_rss_reader() -> tuple[Callable[[], int], Callable[[], int]]:
    """Return ``(current_rss, peak_rss)`` byte readers for this process.

    ``peak_rss`` returns 0 when the platform exposes no OS-maintained high-water
    mark; the sampler then derives the peak from the running max of ``current``.
    """
    try:  # opportunistic: cross-platform and gives Windows peak_wset for free
        import psutil

        proc = psutil.Process()

        def cur() -> int:
            return int(proc.memory_info().rss)

        def peak() -> int:
            return int(getattr(proc.memory_info(), "peak_wset", 0) or 0)

        return cur, peak
    except Exception:  # noqa: BLE001 - psutil absent/unusable; fall through
        pass

    if sys.platform == "win32":
        try:
            return _windows_rss_reader()
        except Exception:  # noqa: BLE001 - ctypes/psapi unavailable
            pass

    try:  # POSIX: ru_maxrss is the process peak (KiB on Linux, bytes on macOS)
        import resource

        scale = 1 if sys.platform == "darwin" else 1024

        def peak() -> int:
            return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * scale

        return (lambda: 0), peak
    except Exception:  # noqa: BLE001
        return (lambda: 0), (lambda: 0)


def _windows_rss_reader() -> tuple[Callable[[], int], Callable[[], int]]:
    import ctypes
    from ctypes import wintypes

    class PMC(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    try:
        get_info = ctypes.WinDLL("psapi").GetProcessMemoryInfo
    except OSError:
        get_info = ctypes.windll.kernel32.K32GetProcessMemoryInfo
    get_info.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
    get_info.restype = wintypes.BOOL
    get_proc = ctypes.windll.kernel32.GetCurrentProcess
    get_proc.restype = wintypes.HANDLE

    def _read() -> tuple[int, int]:
        pmc = PMC()
        pmc.cb = ctypes.sizeof(PMC)
        if get_info(get_proc(), ctypes.byref(pmc), pmc.cb):
            return int(pmc.WorkingSetSize), int(pmc.PeakWorkingSetSize)
        return 0, 0

    return (lambda: _read()[0]), (lambda: _read()[1])


def _arrow_current() -> int:
    try:
        return int(pa.total_allocated_bytes())
    except Exception:  # noqa: BLE001
        return 0


def _arrow_peak() -> int:
    """Peak bytes ever held by the default pyarrow pool (-1 if untracked)."""
    try:
        return max(0, int(pa.default_memory_pool().max_memory()))
    except Exception:  # noqa: BLE001
        return 0


# --------------------------------------------------------------------------- #
# Formatting helpers
# --------------------------------------------------------------------------- #
def _mb(b: int) -> str:
    if b <= 0:
        return "n/a"
    if b >= (1 << 30):
        return f"{b / (1 << 30):.2f}GB"
    return f"{b / (1 << 20):.0f}MB"


def _elapsed(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 3600:d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


@dataclass
class MonitorReport:
    duration_s: float
    units_done: int
    units_total: int
    units_failed: int
    rows: int
    tables_loaded: int
    tables_total: int
    tables_failed: int
    rss_peak: int
    rss_peak_activity: str
    arrow_peak: int
    arrow_peak_activity: str

    @property
    def non_arrow_at_peak(self) -> int:
        return max(0, self.rss_peak - self.arrow_peak)

    def verdict(self) -> str:
        """Best-effort attribution of the peak to a suspected contributor."""
        if self.rss_peak <= 0:
            return "unknown (no memory readings available)"
        arrow_share = self.arrow_peak / self.rss_peak if self.rss_peak else 0.0
        act = self.arrow_peak_activity or self.rss_peak_activity
        if self.arrow_peak > 0 and arrow_share >= 0.6:
            if act.startswith("load:"):
                return (f"Arrow buffers during {act} -- the whole-file "
                        f"pq.read_table + cast in the load resource "
                        f"({arrow_share:.0%} of peak RSS is Arrow)")
            return (f"Arrow buffers during {act} -- native fetch batches "
                    f"({arrow_share:.0%} of peak RSS is Arrow)")
        if self.rss_peak_activity.startswith("extract"):
            return ("non-Arrow memory during extract -- consistent with the "
                    "cursor fallback buffering rows as Python objects")
        return (f"mixed: {_mb(self.arrow_peak)} Arrow + "
                f"{_mb(self.non_arrow_at_peak)} non-Arrow at peak")


# --------------------------------------------------------------------------- #
# Monitor
# --------------------------------------------------------------------------- #
class PipelineMonitor:
    """Background heartbeat + peak-memory attribution for one pipeline run.

    Call ``start()`` once, ``record_unit`` / ``record_table_loaded`` from the
    extraction/load callbacks, ``set_activity`` to label what is currently
    running, and ``stop()`` at the end to get (and log) a :class:`MonitorReport`.
    Every method is safe to call when ``enabled=False`` -- it just does nothing,
    so call sites need no conditionals.
    """

    def __init__(self, *, total_units: int, total_tables: int,
                 interval_s: float = 5.0, enabled: bool = True,
                 logger: Optional[logging.Logger] = None):
        self.total_units = total_units
        self.total_tables = total_tables
        self.interval_s = max(_SAMPLE_INTERVAL_S, float(interval_s))
        self.enabled = enabled
        self.log = logger or log

        self._lock = threading.Lock()
        self._units_done = 0
        self._units_failed = 0
        self._rows = 0
        self._tables_loaded = 0
        self._tables_failed = 0
        self._activity = "starting"            # plain str: assignment is atomic

        self._rss_cur, self._rss_peak_fn = _make_rss_reader()
        self._rss_peak = 0
        self._rss_peak_activity = ""
        self._arrow_peak = 0
        self._arrow_peak_activity = ""

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_t = 0.0

    # ----- lifecycle ---------------------------------------------------------
    def start(self) -> "PipelineMonitor":
        self._start_t = time.perf_counter()
        if self.enabled:
            self._thread = threading.Thread(
                target=self._run, name="progress", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> MonitorReport:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + 2.0)
        self._refresh_peaks()
        report = self._report()
        self._log_report(report)
        return report

    # ----- hot-path updates (cheap) -----------------------------------------
    def set_activity(self, label: str) -> None:
        self._activity = label

    def record_unit(self, rows: int, status: str) -> None:
        with self._lock:
            self._units_done += 1
            self._rows += max(0, int(rows or 0))
            if status != "SUCCESS":
                self._units_failed += 1

    def record_table_loaded(self, status: str) -> None:
        with self._lock:
            self._tables_loaded += 1
            if status != "SUCCESS":
                self._tables_failed += 1

    # ----- sampler -----------------------------------------------------------
    def _refresh_peaks(self) -> None:
        # OS/Arrow high-water marks are monotonic; whenever one ticks up, pin the
        # increase to whatever activity is running right now.
        act = self._activity
        rss = self._rss_peak_fn() or self._rss_cur()
        if rss > self._rss_peak:
            self._rss_peak, self._rss_peak_activity = rss, act
        arrow = _arrow_peak() or _arrow_current()
        if arrow > self._arrow_peak:
            self._arrow_peak, self._arrow_peak_activity = arrow, act

    def _run(self) -> None:
        next_report = 0.0
        while not self._stop.wait(_SAMPLE_INTERVAL_S):
            self._refresh_peaks()
            elapsed = time.perf_counter() - self._start_t
            if elapsed >= next_report:
                self.log.info(self._heartbeat(elapsed))
                next_report = elapsed + self.interval_s

    def _heartbeat(self, elapsed: float) -> str:
        with self._lock:
            ud, uf, rows = self._units_done, self._units_failed, self._rows
            tl = self._tables_loaded
        failed = f" {uf} failed" if uf else ""
        return (f"PROGRESS {_elapsed(elapsed)} | {self._activity} | "
                f"tables {tl}/{self.total_tables} | "
                f"extract {ud}/{self.total_units}{failed} | "
                f"rows={rows:,} | rss={_mb(self._rss_cur())}"
                f"(peak {_mb(self._rss_peak)}) arrow={_mb(_arrow_current())}")

    # ----- reporting ---------------------------------------------------------
    def _report(self) -> MonitorReport:
        with self._lock:
            return MonitorReport(
                duration_s=time.perf_counter() - self._start_t,
                units_done=self._units_done, units_total=self.total_units,
                units_failed=self._units_failed, rows=self._rows,
                tables_loaded=self._tables_loaded, tables_total=self.total_tables,
                tables_failed=self._tables_failed,
                rss_peak=self._rss_peak, rss_peak_activity=self._rss_peak_activity,
                arrow_peak=self._arrow_peak,
                arrow_peak_activity=self._arrow_peak_activity,
            )

    def _log_report(self, r: MonitorReport) -> None:
        self.log.info("MEMORY/PROGRESS SUMMARY")
        self.log.info("  duration          : %s", _elapsed(r.duration_s))
        self.log.info("  extract units     : %d/%d (%d failed), rows=%s",
                      r.units_done, r.units_total, r.units_failed, f"{r.rows:,}")
        self.log.info("  tables loaded     : %d/%d (%d failed)",
                      r.tables_loaded, r.tables_total, r.tables_failed)
        self.log.info("  peak process RSS  : %s  (while: %s)",
                      _mb(r.rss_peak), r.rss_peak_activity or "n/a")
        self.log.info("  peak Arrow alloc  : %s  (while: %s)",
                      _mb(r.arrow_peak), r.arrow_peak_activity or "n/a")
        self.log.info("  non-Arrow at peak : ~%s", _mb(r.non_arrow_at_peak))
        self.log.info("  likely dominant   : %s", r.verdict())
