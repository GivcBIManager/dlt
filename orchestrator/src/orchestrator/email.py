"""SMTP notification: load config, render bodies, send, and build the
success/failure run-status sensors that fire one email per flow run.
"""
from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from typing import Any

import dagster as dg

from orchestrator import state

try:
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover
    import tomli as _toml  # type: ignore

_REQUIRED = ("host", "port", "from")


def load_smtp() -> dict[str, Any] | None:
    p = state.secrets_path()
    if not p.exists():
        return None
    with p.open("rb") as fh:
        smtp = _toml.load(fh).get("smtp", {})
    if not all(smtp.get(k) for k in _REQUIRED):
        return None
    return {
        "host": smtp["host"], "port": int(smtp["port"]),
        "username": smtp.get("username"), "password": smtp.get("password"),
        "from": smtp["from"], "use_tls": bool(smtp.get("use_tls", True)),
    }


def run_url(run_id: str) -> str:
    host = os.environ.get("OASIS_DAGSTER_HOST", "127.0.0.1")
    port = os.environ.get("OASIS_DAGSTER_PORT", "3000")
    return f"http://{host}:{port}/runs/{run_id}"


def render_body(flow_name: str, status: str, run_id: str, started: str,
                ended: str, url: str, error: str | None = None) -> str:
    lines = [
        f"Flow:    {flow_name}",
        f"Status:  {status}",
        f"Run id:  {run_id}",
        f"Started: {started}",
        f"Ended:   {ended}",
        f"Dagster: {url}",
    ]
    if error:
        lines += ["", "Error:", error]
    return "\n".join(lines)


def send_email(smtp: dict[str, Any], recipients: list[str], subject: str,
               body: str) -> None:
    if not recipients:
        return
    msg = EmailMessage()
    msg["From"] = smtp["from"]
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(body)
    with smtplib.SMTP(smtp["host"], smtp["port"], timeout=30) as server:
        if smtp["use_tls"]:
            server.starttls()
        if smtp.get("username"):
            server.login(smtp["username"], smtp.get("password") or "")
        server.send_message(msg)


def _send_for_run(context, status: str, recipients: list[str], flow_name: str,
                  error: str | None = None) -> None:
    smtp = load_smtp()
    if not smtp or not recipients:
        if not smtp:
            context.log.warning("SMTP not configured; skipping %s email", status)
        return
    run = context.dagster_run
    url = run_url(run.run_id)
    subject = f"[OASIS] Flow {flow_name} {status} — run {run.run_id[:8]}"
    body = render_body(flow_name, status, run.run_id,
                       str(getattr(run, "start_time", "")),
                       str(getattr(run, "end_time", "")), url, error)
    send_email(smtp, recipients, subject, body)
    context.log.info("Sent %s email to %s", status, recipients)


def build_email_sensors(flow_id: str, flow_name: str, job: Any,
                        success_to: list[str], failure_to: list[str]) -> list:
    sensors: list[Any] = []

    if success_to:
        @dg.run_status_sensor(
            name=f"flow_{flow_id}_success_email",
            run_status=dg.DagsterRunStatus.SUCCESS,
            monitored_jobs=[job],
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _success(context: dg.RunStatusSensorContext) -> None:
            _send_for_run(context, "SUCCEEDED", success_to, flow_name)

        sensors.append(_success)

    if failure_to:
        @dg.run_failure_sensor(
            name=f"flow_{flow_id}_failure_email",
            monitored_jobs=[job],
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _failure(context: dg.RunFailureSensorContext) -> None:
            _send_for_run(context, "FAILED", failure_to, flow_name,
                          error=context.failure_event.message)

        sensors.append(_failure)

    return sensors
