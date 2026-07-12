"""Turn pipelines.json + flows.json into a single dg.Definitions.

Each flow → assets (one per node) + an asset job + a schedule + email sensors.
A flow that fails validation is skipped (logged) so one bad flow never breaks
the whole code location.
"""
from __future__ import annotations

import logging

import dagster as dg

from orchestrator import assets as asset_mod
from orchestrator import email as email_mod
from orchestrator import state

_log = logging.getLogger("orchestrator.build")


def _build_flow(flow: dict, pipelines: dict[str, dict]):
    prefix = state.flow_naming.asset_key_prefix(flow)
    group = state.flow_naming.group_name(flow)
    node_ids = {n["node_id"] for n in flow["nodes"]}
    flow_assets = []
    for node in flow["nodes"]:
        dep_keys = [asset_mod.asset_key(prefix, d) for d in node.get("deps", [])]
        for d in node.get("deps", []):
            if d not in node_ids:
                raise ValueError(f"flow {flow['id']}: unknown dep {d}")
        kind = node.get("kind", "pipeline")
        if kind == "dbt":
            dbt = node.get("dbt") or {}
            spec = {"script": "dbt", "dbt_command": dbt.get("dbt_command", "run"),
                    "select": dbt.get("select", "")}
            name = f"dbt {spec['dbt_command']} {spec['select']}".strip()
        elif kind == "command":
            cmd = (node.get("command") or "").strip()
            if not cmd:
                raise ValueError(f"flow {flow['id']}: command node {node['node_id']} is empty")
            spec = {"script": "custom", "custom": cmd}
            name = f"custom: {cmd[:40]}"
        else:
            pid = node["pipeline_id"]
            if pid not in pipelines:
                raise ValueError(f"flow {flow['id']}: unknown pipeline {pid}")
            spec = pipelines[pid]["spec"]
            name = pipelines[pid].get("name", node["node_id"])
        flow_assets.append(asset_mod.build_asset(
            prefix, group, node["node_id"], name, spec, dep_keys))

    node_keys = [asset_mod.asset_key(prefix, n["node_id"]) for n in flow["nodes"]]
    job = dg.define_asset_job(
        state.flow_naming.job_name(flow),
        selection=dg.AssetSelection.assets(*node_keys))

    enabled = flow.get("enabled", True)
    schedule = dg.ScheduleDefinition(
        name=state.flow_naming.schedule_name(flow),
        job=job,
        cron_schedule=flow["cron"],
        execution_timezone=flow.get("timezone", "UTC"),
        default_status=(dg.DefaultScheduleStatus.RUNNING if enabled
                        else dg.DefaultScheduleStatus.STOPPED),
    )

    email = flow.get("email", {})
    sensors = email_mod.build_email_sensors(
        state.flow_naming.base_name(flow), flow["name"], job,
        email.get("on_success", []), email.get("on_failure", []))

    return flow_assets, job, schedule, sensors


def build_all_defs() -> dg.Definitions:
    pipelines = state.read_pipelines()
    flows = state.read_flows()
    all_assets, all_jobs, all_schedules, all_sensors = [], [], [], []
    for flow in flows:
        try:
            a, j, s, sens = _build_flow(flow, pipelines)
        except Exception as exc:  # noqa: BLE001 - skip a bad flow, keep the rest
            _log.warning("Skipping flow %s: %s", flow.get("id"), exc)
            continue
        all_assets += a
        all_jobs.append(j)
        all_schedules.append(s)
        all_sensors += sens
    return dg.Definitions(
        assets=all_assets, jobs=all_jobs,
        schedules=all_schedules, sensors=all_sensors)
