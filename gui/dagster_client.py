"""Thin Dagster GraphQL client for status, reload, and schedule control.

Uses only stdlib urllib so it adds no dependency. Every call is best-effort:
on a connection error it returns an error dict (or [] for status) rather than
raising, so the GUI stays responsive when Dagster is down.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

import config

_TIMEOUT = 6


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


def reload_location() -> dict[str, Any]:
    # Reload all repository locations (single workspace here).
    q = """
    mutation Reload {
      reloadWorkspace {
        __typename
        ... on WorkspaceLocationStatusEntries { entries { name } }
      }
    }"""
    res = _query(q)
    ok = "errors" not in res
    return {"ok": ok, "error": None if ok else res["errors"][0]["message"]}


def _set_schedule(mutation: str, schedule_name: str) -> dict[str, Any]:
    q = f"""
    mutation Toggle($name: String!) {{
      {mutation}(scheduleSelector: {{
        repositoryLocationName: "orchestrator",
        repositoryName: "__repository__",
        scheduleName: $name
      }}) {{ __typename }}
    }}"""
    res = _query(q, {"name": schedule_name})
    ok = "errors" not in res
    return {"ok": ok, "error": None if ok else res["errors"][0]["message"]}


def start_schedule(name: str) -> dict[str, Any]:
    return _set_schedule("startSchedule", name)


def stop_schedule(name: str) -> dict[str, Any]:
    return _set_schedule("stopSchedule", name)


def launch_job(job_name: str) -> dict[str, Any]:
    q = """
    mutation Launch($job: String!) {
      launchRun(executionParams: {
        selector: {
          repositoryLocationName: "orchestrator",
          repositoryName: "__repository__",
          jobName: $job
        }, mode: "default"
      }) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
      }
    }"""
    res = _query(q, {"job": job_name})
    if "errors" in res:
        return {"ok": False, "error": res["errors"][0]["message"]}
    node = res.get("data", {}).get("launchRun", {})
    if node.get("__typename") == "LaunchRunSuccess":
        return {"ok": True, "run_id": node["run"]["runId"]}
    return {"ok": False, "error": node.get("message", "launch failed")}


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
    out: list[dict[str, Any]] = []
    for repo in nodes:
        sched_by_job = {s["name"]: s for s in repo.get("schedules", [])}
        for job in repo.get("jobs", []):
            runs = job.get("runs") or []
            last = runs[0] if runs else {}
            sched = sched_by_job.get(f"{job['name']}_schedule", {})
            out.append({
                "job": job["name"],
                "schedule_state": (sched.get("scheduleState") or {}).get("status"),
                "last_run_status": last.get("status"),
                "last_run_id": last.get("runId"),
                "last_run_at": last.get("startTime"),
                "link": job_link(job["name"]),
                "run_link": run_link(last["runId"]) if last.get("runId") else None,
            })
    return out
