"""SMTP notification: load config, render bodies, send, and build the
success/failure run-status sensors that fire one email per flow run.
"""
from __future__ import annotations

import os
import smtplib
import socket
from collections.abc import Sequence
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


# A wildcard bind ("listen on every interface") is not an address anyone can
# type into a browser, so links must name a concrete interface instead.
_WILDCARD_HOSTS = {"", "*", "0.0.0.0", "::", "[::]"}


def _routable(addr: str) -> bool:
    """True for an address a reader on another machine could actually use."""
    if not addr or addr in _WILDCARD_HOSTS:
        return False
    low = addr.lower()
    return not (low.startswith("127.") or low == "::1"
                or low.startswith("169.254.")  # IPv4 link-local (APIPA)
                or low.startswith("fe80:"))    # IPv6 link-local


def _route_address() -> str | None:
    """The local address the OS would use for off-box traffic.

    A UDP ``connect`` puts nothing on the wire; it only makes the kernel pick a
    source interface, which is the interface remote readers can reach us on.
    Preferred over the hostname lookup because a multi-homed box resolves its
    own name to addresses that may not be the externally reachable one.
    """
    for family, peer in ((socket.AF_INET, ("8.8.8.8", 53)),
                         (socket.AF_INET6, ("2001:4860:4860::8888", 53))):
        try:
            with socket.socket(family, socket.SOCK_DGRAM) as sock:
                sock.settimeout(0.2)
                sock.connect(peer)
                addr = sock.getsockname()[0]
        except OSError:
            continue
        if _routable(addr):
            return addr
    return None


def _hostname_addresses() -> list[str]:
    """Every address this host's own name resolves to (best effort)."""
    try:
        infos = socket.getaddrinfo(socket.gethostname(), None)
    except OSError:
        return []
    return [info[4][0] for info in infos]


def host_addresses() -> list[str]:
    """Addresses that reach this machine's Dagster UI, best candidate first.

    ``OASIS_DAGSTER_HOST`` is a *bind* host: in prod it inherits the GUI's
    ``0.0.0.0``, which is useless in an email. When it is a wildcard we
    substitute the machine's real interface addresses. ``OASIS_DAGSTER_PUBLIC_HOST``
    (comma-separated) overrides everything for deployments behind a DNS name or
    reverse proxy.
    """
    override = os.environ.get("OASIS_DAGSTER_PUBLIC_HOST", "")
    if override.strip():
        return [h.strip() for h in override.split(",") if h.strip()]

    host = os.environ.get("OASIS_DAGSTER_HOST", "127.0.0.1").strip()
    if host.lower() not in _WILDCARD_HOSTS:
        return [host]

    found: list[str] = []
    primary = _route_address()
    if primary:
        found.append(primary)
    for addr in _hostname_addresses():
        if _routable(addr) and addr not in found:
            found.append(addr)
    # An isolated box has no routable interface; loopback at least works there.
    return found or ["127.0.0.1"]


def _url_host(host: str) -> str:
    """Bracket a bare IPv6 literal so it is valid inside a URL."""
    return f"[{host}]" if ":" in host and not host.startswith("[") else host


def run_urls(run_id: str) -> list[str]:
    """One Dagster run link per address this machine is reachable at."""
    port = os.environ.get("OASIS_DAGSTER_PORT", "3000")
    return [f"http://{_url_host(h)}:{port}/runs/{run_id}"
            for h in host_addresses()]


def run_url(run_id: str) -> str:
    return run_urls(run_id)[0]


def render_body(flow_name: str, status: str, run_id: str, started: str,
                ended: str, url: str | Sequence[str],
                error: str | None = None) -> str:
    urls = [url] if isinstance(url, str) else list(url)
    lines = [
        f"Flow:    {flow_name}",
        f"Status:  {status}",
        f"Run id:  {run_id}",
        f"Started: {started}",
        f"Ended:   {ended}",
        f"Dagster: {urls[0] if urls else '(unavailable)'}",
    ]
    # A multi-homed host is reachable at several addresses; list them all so the
    # reader can pick whichever one their network can see.
    lines += [f"         {extra}" for extra in urls[1:]]
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
    urls = run_urls(run.run_id)
    subject = f"[OASIS] Flow {flow_name} {status} — run {run.run_id[:8]}"
    body = render_body(flow_name, status, run.run_id,
                       str(getattr(run, "start_time", "")),
                       str(getattr(run, "end_time", "")), urls, error)
    send_email(smtp, recipients, subject, body)
    context.log.info("Sent %s email to %s", status, recipients)


def build_email_sensors(base: str, flow_name: str, job: Any,
                        success_to: list[str], failure_to: list[str]) -> list:
    sensors: list[Any] = []

    if success_to:
        @dg.run_status_sensor(
            name=f"flow_{base}_success_email",
            run_status=dg.DagsterRunStatus.SUCCESS,
            monitored_jobs=[job],
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _success(context: dg.RunStatusSensorContext) -> None:
            _send_for_run(context, "SUCCEEDED", success_to, flow_name)

        sensors.append(_success)

    if failure_to:
        @dg.run_failure_sensor(
            name=f"flow_{base}_failure_email",
            monitored_jobs=[job],
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _failure(context: dg.RunFailureSensorContext) -> None:
            _send_for_run(context, "FAILED", failure_to, flow_name,
                          error=context.failure_event.message)

        sensors.append(_failure)

    return sensors
