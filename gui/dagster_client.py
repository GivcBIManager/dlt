"""Thin Dagster GraphQL client for status, reload, and schedule control.

Uses only stdlib urllib so it adds no dependency. Every call is best-effort:
on a connection error it returns an error dict (or [] for status) rather than
raising, so the GUI stays responsive when Dagster is down.
"""
from __future__ import annotations

import datetime as dt
import json
import urllib.error
import urllib.request
from typing import Any, Optional

import config
import flow_naming

_TIMEOUT = 6

# Code-location name = the `-m` target used by dagster_service.launch_argv
# (`dagster dev -m orchestrator.definitions`). Selector-based GraphQL calls need it.
_LOCATION = "orchestrator.definitions"
_REPOSITORY = "__repository__"


def graphql_url() -> str:
    return f"{config.dagster_base_url()}/graphql"


def run_link(run_id: str) -> str:
    return f"{config.dagster_base_url()}/runs/{run_id}"


def job_link(job_name: str) -> str:
    return f"{config.dagster_base_url()}/jobs/{job_name}"


def _query(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        graphql_url(), data=payload,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        return {"errors": [{"message": str(exc)}]}


def _first_error(res: dict[str, Any]) -> str:
    """Safely extract the first GraphQL error message (never raises)."""
    errs = res.get("errors") or []
    if errs and isinstance(errs[0], dict) and errs[0].get("message"):
        return errs[0]["message"]
    return "request failed"


def reload_location() -> dict[str, Any]:
    q = """
    mutation Reload {
      reloadWorkspace {
        __typename
        ... on Workspace { locationEntries { name } }
        ... on PythonError { message }
      }
    }"""
    res = _query(q)
    if "errors" in res:
        return {"ok": False, "error": _first_error(res)}
    node = res.get("data", {}).get("reloadWorkspace", {})
    ok = node.get("__typename") == "Workspace"
    return {"ok": ok, "error": None if ok else node.get("message", "reload failed")}


def start_schedule(name: str) -> dict[str, Any]:
    q = """
    mutation Start($sel: ScheduleSelector!) {
      startSchedule(scheduleSelector: $sel) {
        __typename
        ... on PythonError { message }
      }
    }"""
    sel = {"repositoryLocationName": _LOCATION, "repositoryName": _REPOSITORY, "scheduleName": name}
    res = _query(q, {"sel": sel})
    if "errors" in res:
        return {"ok": False, "error": _first_error(res)}
    node = res.get("data", {}).get("startSchedule", {})
    ok = node.get("__typename") == "ScheduleStateResult"
    return {"ok": ok, "error": None if ok else node.get("message", "start failed")}


def _schedule_id(name: str) -> str | None:
    q = """
    query Sid {
      repositoriesOrError {
        ... on RepositoryConnection { nodes { schedules { name id } } }
      }
    }"""
    res = _query(q)
    nodes = (res.get("data", {}).get("repositoriesOrError", {}) or {}).get("nodes")
    if not nodes:
        return None
    for repo in nodes:
        for s in repo.get("schedules", []):
            if s.get("name") == name and s.get("id"):
                return s["id"]
    return None


def stop_schedule(name: str) -> dict[str, Any]:
    sid = _schedule_id(name)
    if not sid:
        return {"ok": False, "error": f"schedule {name} not found"}
    q = """
    mutation Stop($id: String!) {
      stopRunningSchedule(id: $id) {
        __typename
        ... on PythonError { message }
      }
    }"""
    res = _query(q, {"id": sid})
    if "errors" in res:
        return {"ok": False, "error": _first_error(res)}
    node = res.get("data", {}).get("stopRunningSchedule", {})
    ok = node.get("__typename") == "ScheduleStateResult"
    return {"ok": ok, "error": None if ok else node.get("message", "stop failed")}


def launch_job(job_name: str) -> dict[str, Any]:
    q = """
    mutation Launch($p: ExecutionParams!) {
      launchRun(executionParams: $p) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on PipelineNotFoundError { message }
        ... on PythonError { message }
      }
    }"""
    params = {"selector": {"repositoryLocationName": _LOCATION,
                           "repositoryName": _REPOSITORY, "jobName": job_name}}
    res = _query(q, {"p": params})
    if "errors" in res:
        return {"ok": False, "error": _first_error(res)}
    node = res.get("data", {}).get("launchRun", {})
    if node.get("__typename") == "LaunchRunSuccess":
        return {"ok": True, "run_id": node["run"]["runId"]}
    return {"ok": False, "error": node.get("message", "launch failed")}


def _rows_from_repos(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for repo in nodes:
        sched_by_job = {s["name"]: s for s in repo.get("schedules", [])}
        for job in repo.get("jobs", []):
            if not job["name"].startswith("flow_"):
                continue
            runs = job.get("runs") or []
            last = runs[0] if runs else {}
            sched = sched_by_job.get(f"{job['name']}_schedule", {})
            out.append({
                "job": job["name"],
                "flow_id": flow_naming.flow_id_from_job(job["name"]),
                "schedule_state": (sched.get("scheduleState") or {}).get("status"),
                "last_run_status": last.get("status"),
                "last_run_id": last.get("runId"),
                "last_run_at": last.get("startTime"),
                "link": job_link(job["name"]),
                "run_link": run_link(last["runId"]) if last.get("runId") else None,
            })
    return out


def flow_status() -> list[dict[str, Any]]:
    """Per-job latest-run + schedule state. Empty list if Dagster unreachable."""
    q = """
    query Status {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            jobs { name runs(limit: 1) { runId status startTime } }
            schedules { name scheduleState { status } }
          }
        }
      }
    }"""
    res = _query(q)
    nodes = (res.get("data", {}).get("repositoriesOrError", {}) or {}).get("nodes")
    if not nodes:
        return []
    return _rows_from_repos(nodes)


def _runs_from_payload(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep flow-job runs and reshape to the GUI's row format."""
    out: list[dict[str, Any]] = []
    for r in results:
        job = r.get("jobName") or ""
        if not job.startswith("flow_"):
            continue
        rid = r.get("runId")
        out.append({
            "run_id": rid,
            "job": job,
            "flow_id": flow_naming.flow_id_from_job(job),
            "status": r.get("status"),
            "start_time": r.get("startTime"),
            "end_time": r.get("endTime"),
            "run_link": run_link(rid) if rid else None,
        })
    return out


def flow_runs(limit: int = 50) -> list[dict[str, Any]]:
    """Recent flow-job runs, newest first. Empty list if Dagster unreachable."""
    q = """
    query FlowRuns($limit: Int!) {
      runsOrError(limit: $limit) {
        ... on Runs { results { runId jobName status startTime endTime } }
      }
    }"""
    res = _query(q, {"limit": limit})
    results = (res.get("data", {}).get("runsOrError", {}) or {}).get("results")
    if not results:
        return []
    return _runs_from_payload(results)


def _fmt_event(ev: dict[str, Any]) -> Optional[str]:
    """One log line per MessageEvent; None for empty/non-message events."""
    msg = ev.get("message")
    if not msg:
        return None
    try:
        hhmmss = dt.datetime.fromtimestamp(
            float(ev.get("timestamp")) / 1000).strftime("%H:%M:%S")
    except (TypeError, ValueError, OSError, OverflowError):
        hhmmss = "--:--:--"
    return f"{hhmmss} | {ev.get('level') or '':<8} | {msg}"


def run_log_tail(run_id: str, cursor: Optional[str] = None) -> dict[str, Any]:
    """New log lines for a run since ``cursor`` -- the GraphQL analogue of the
    byte-offset file tail the Monitor page uses for run_logs files."""
    q = """
    query Tail($runId: ID!, $cursor: String) {
      logsForRun(runId: $runId, afterCursor: $cursor, limit: 1000) {
        __typename
        ... on EventConnection {
          events { __typename ... on MessageEvent { message timestamp level } }
          cursor
          hasMore
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
      runOrError(runId: $runId) { ... on Run { status } }
    }"""
    res = _query(q, {"runId": run_id, "cursor": cursor})
    err_shape = {"chunk": "", "cursor": cursor, "has_more": False, "status": None}
    if "errors" in res:
        return {**err_shape, "error": _first_error(res)}
    data = res.get("data", {})
    node = data.get("logsForRun", {}) or {}
    if node.get("__typename") != "EventConnection":
        return {**err_shape, "error": node.get("message", "log fetch failed")}
    lines = [ln for ev in node.get("events", []) if (ln := _fmt_event(ev))]
    return {
        "chunk": "\n".join(lines) + ("\n" if lines else ""),
        "cursor": node.get("cursor") or cursor,
        "has_more": bool(node.get("hasMore")),
        "status": (data.get("runOrError", {}) or {}).get("status"),
        "error": None,
    }
