"""OASIS pipeline control panel -- a small Flask GUI.

Manage / run / monitor the Oracle->Iceberg pipeline, browse the Iceberg lake,
edit ``tables.json``, and orchestrate Flows via Dagster. Launch with
``python gui/app.py`` (or via the setup scripts) and open http://127.0.0.1:8765.
"""

from __future__ import annotations

import functools
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

# Must be set before pyarrow is first imported (pulled in transitively by the
# module imports below): the mimalloc pool holds freed memory, the system pool
# returns it to the OS.
os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")

# Allow both ``python gui/app.py`` and ``python -m gui.app`` by ensuring this
# directory is importable for the flat module imports below.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from flask import (Flask, Response, jsonify, redirect, render_template, request,  # noqa: E402
                   session, url_for)

import clickhouse_config  # noqa: E402
import commands  # noqa: E402
import config  # noqa: E402
import connections  # noqa: E402
import dagster_client  # noqa: E402
import dbt_config  # noqa: E402
import dbt_project_store  # noqa: E402
import flows_store  # noqa: E402
import flow_naming  # noqa: E402
import iceberg_browser  # noqa: E402
import iceberg_maintenance  # noqa: E402
import pipelines_store  # noqa: E402
import security  # noqa: E402
import smtp_config  # noqa: E402
import tables_store  # noqa: E402
import workspace  # noqa: E402
from config import ensure_dirs  # noqa: E402
from dagster_service import service as dagster_service  # noqa: E402
from pipeline_runner import RunManager  # noqa: E402

app = Flask(__name__)
runner = RunManager()
ensure_dirs()
config.server_timezone()  # prime the cached tz detection at startup, not on page load
# Signed-session key persists across restarts so logins survive a server bounce.
app.secret_key = security.load_or_create_secret_key(config.STATE_DIR / "secret_key")
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def api(fn):
    """Wrap an API handler: turn exceptions into JSON error responses."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except (FileNotFoundError, KeyError) as exc:
            return jsonify({"error": f"not found: {exc}"}), 404
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:  # pragma: no cover - surface unexpected errors
            app.logger.exception("API error in %s", fn.__name__)
            return jsonify({"error": f"{type(exc).__name__}: {exc}"}), 500

    return wrapper


def _body() -> dict:
    return request.get_json(silent=True) or {}


def _dagster_public_base() -> str:
    """Dagster UI base URL for the browser: the GUI hostname the client used.

    The server-local ``config.dagster_base_url()`` (127.0.0.1) is meaningless
    on a remote client's machine, so links inherit the request's host instead.
    """
    hostname = urlsplit(f"//{request.host}").hostname or "127.0.0.1"
    if ":" in hostname:  # bare IPv6 literal needs brackets in a URL
        hostname = f"[{hostname}]"
    return f"http://{hostname}:{config.dagster_port()}"


def _dagster_status_public() -> dict:
    """Dagster service status with a browser-usable UI URL."""
    status = dagster_service.status()
    status["url"] = _dagster_public_base()
    return status


def _publicise_dagster_links(items: list[dict]) -> list[dict]:
    """Rewrite server-local Dagster links so they work from the client's machine."""
    local = config.dagster_base_url()
    public = _dagster_public_base()
    for item in items:
        for key in ("link", "run_link"):
            val = item.get(key)
            if val and val.startswith(local):
                item[key] = public + val[len(local):]
    return items


# --------------------------------------------------------------------------- #
# Security gate: session login for non-loopback clients, JSON-only mutations
# --------------------------------------------------------------------------- #
_AUTH_EXEMPT_PATHS = {"/healthz", "/login"}
# Login/logout are browser form posts; the session cookie is SameSite=Lax so a
# cross-site form cannot ride an existing session.
_CSRF_EXEMPT_PATHS = {"/login", "/logout"}
_MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


@app.before_request
def _auth_gate():
    path = request.path
    if path.startswith("/static/") or path in _AUTH_EXEMPT_PATHS:
        return None
    if not security.request_authorized(request.remote_addr, "user" in session):
        # Browsers land on the login page; API clients get a JSON 401.
        if request.method == "GET" and request.accept_mimetypes.accept_html:
            return redirect(url_for("page_login"))
        return jsonify({"error": "unauthorized"}), 401
    # CSRF: a cross-site <form> POST cannot set an application/json content-type
    # without triggering a CORS preflight, so require JSON on mutating requests.
    if request.method in _MUTATING_METHODS and path not in _CSRF_EXEMPT_PATHS:
        if not request.is_json:
            return jsonify({"error": "mutating requests must be application/json"}), 415
    return None


@app.route("/login", methods=["GET", "POST"])
def page_login():
    already_in = security.request_authorized(request.remote_addr, "user" in session)
    if request.method == "GET":
        if already_in:
            return redirect(url_for("page_dashboard"))
        return render_template("login.html", error=None)
    creds = security.gui_credentials()
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    if security.credentials_match(username, password, creds):
        session["user"] = username
        return redirect(url_for("page_dashboard"))
    return render_template("login.html", error="Invalid username or password"), 401


@app.post("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("page_login"))


# --------------------------------------------------------------------------- #
# Pages
# --------------------------------------------------------------------------- #
@app.route("/")
def page_dashboard():
    return render_template("dashboard.html", active="dashboard")


@app.route("/run")
def page_run():
    return render_template("run.html", active="run")


@app.route("/logs")
def page_logs():
    return render_template("logs.html", active="logs")


@app.route("/tables")
def page_tables():
    return render_template("tables.html", active="tables")


@app.route("/iceberg")
def page_iceberg():
    return render_template("iceberg.html", active="iceberg")


@app.route("/connections")
def page_connections():
    return render_template("connections.html", active="connections")


@app.route("/flows")
def page_flows():
    return render_template("flows.html", active="flows")


@app.route("/models")
def page_models():
    return render_template("dbt.html", active="models")


@app.route("/settings")
def page_settings():
    return render_template("settings.html", active="settings")


# --------------------------------------------------------------------------- #
# Workspace / dashboard API
# --------------------------------------------------------------------------- #
@app.get("/api/overview")
@api
def api_overview():
    return jsonify(
        {
            "control": workspace.control_summary(),
            "branches": workspace.list_branches(),
            "settings": workspace.etl_settings(),
            "active_runs": runner.active_count(),
        }
    )


@app.get("/api/branches")
@api
def api_branches():
    return jsonify(workspace.list_branches())


@app.get("/api/control")
@api
def api_control():
    return jsonify({"summary": workspace.control_summary(), "rows": workspace.control_rows()})


@app.get("/api/settings")
@api
def api_settings_get():
    return jsonify(workspace.etl_settings())


@app.put("/api/settings")
@api
def api_settings_put():
    return jsonify(workspace.update_etl_settings(_body()))


# --------------------------------------------------------------------------- #
# Connections API (Oracle branches in secrets.toml)
# --------------------------------------------------------------------------- #
@app.get("/api/connections")
@api
def api_conn_list():
    return jsonify(connections.list_connections())


@app.post("/api/connections")
@api
def api_conn_add():
    return jsonify(connections.add_connection(_body()))


@app.put("/api/connections/<key>")
@api
def api_conn_update(key):
    return jsonify(connections.update_connection(key, _body()))


@app.delete("/api/connections/<key>")
@api
def api_conn_delete(key):
    return jsonify({"deleted": connections.delete_connection(key)})


@app.post("/api/connections/<key>/test")
@api
def api_conn_test(key):
    return jsonify(connections.test_connection(key))


# --------------------------------------------------------------------------- #
# Run API
# --------------------------------------------------------------------------- #
@app.post("/api/command/preview")
@api
def api_preview():
    spec = _body()
    argv, label = commands.build_argv(spec)
    return jsonify({"preview": commands.preview(spec), "argv": argv, "label": label})


@app.post("/api/run")
@api
def api_run():
    spec = _body()
    argv, label = commands.build_argv(spec)
    run = runner.start(argv, label=label)
    return jsonify(run)


@app.get("/api/runs")
@api
def api_runs():
    return jsonify(runner.list())


@app.get("/api/runs/<run_id>/tail")
@api
def api_run_tail(run_id):
    offset = request.args.get("offset", 0, type=int)
    return jsonify(runner.tail(run_id, offset))


@app.post("/api/runs/<run_id>/stop")
@api
def api_run_stop(run_id):
    return jsonify({"stopped": runner.stop(run_id)})


# --------------------------------------------------------------------------- #
# Pipelines API
# --------------------------------------------------------------------------- #
@app.get("/api/pipelines")
@api
def api_pipelines_list():
    return jsonify(pipelines_store.load_pipelines())


@app.post("/api/pipelines")
@api
def api_pipelines_add():
    b = _body()
    p = pipelines_store.add_pipeline(b.get("name", ""), b.get("spec", {}))
    dagster_client.reload_location()
    return jsonify(p)


@app.put("/api/pipelines/<pid>")
@api
def api_pipelines_update(pid):
    p = pipelines_store.update_pipeline(pid, **_body())
    dagster_client.reload_location()
    return jsonify(p)


@app.delete("/api/pipelines/<pid>")
@api
def api_pipelines_delete(pid):
    refs = flows_store.referencing_flows(pid)
    if refs:
        names = ", ".join(f["name"] for f in refs)
        raise ValueError(f"Pipeline is used by flow(s): {names}")
    deleted = pipelines_store.delete_pipeline(pid)
    dagster_client.reload_location()
    return jsonify({"deleted": deleted})


# --------------------------------------------------------------------------- #
# Flows API
# --------------------------------------------------------------------------- #
@app.get("/api/flows")
@api
def api_flows_list():
    return jsonify({
        "flows": flows_store.load_flows(),
        "pipelines": pipelines_store.load_pipelines(),
        "dagster": _dagster_status_public(),
        "server_timezone": config.server_timezone(),
    })


@app.post("/api/flows")
@api
def api_flows_add():
    b = _body()
    f = flows_store.add_flow(b.get("name", ""), b.get("nodes", []),
                             b.get("cron", ""), b.get("timezone", "UTC"),
                             b.get("email", {}), b.get("enabled", True),
                             b.get("graph"))
    dagster_client.reload_location()
    return jsonify(f)


@app.put("/api/flows/<fid>")
@api
def api_flows_update(fid):
    f = flows_store.update_flow(fid, **_body())
    dagster_client.reload_location()
    return jsonify(f)


@app.delete("/api/flows/<fid>")
@api
def api_flows_delete(fid):
    deleted = flows_store.delete_flow(fid)
    dagster_client.reload_location()
    return jsonify({"deleted": deleted})


@app.post("/api/flows/<fid>/run")
@api
def api_flows_run(fid):
    flow = flows_store.get_flow(fid)
    if flow is None:
        raise KeyError(fid)
    return jsonify(dagster_client.launch_job(flow_naming.job_name(flow)))


@app.post("/api/flows/<fid>/toggle")
@api
def api_flows_toggle(fid):
    enabled = bool(_body().get("enabled", True))
    flow = flows_store.update_flow(fid, enabled=enabled)
    dagster_client.reload_location()
    sched = flow_naming.schedule_name(flow)
    res = dagster_client.start_schedule(sched) if enabled else dagster_client.stop_schedule(sched)
    return jsonify({"enabled": enabled, **res})


# --------------------------------------------------------------------------- #
# SMTP API
# --------------------------------------------------------------------------- #
@app.get("/api/smtp")
@api
def api_smtp_get():
    return jsonify(smtp_config.get_smtp())


@app.put("/api/smtp")
@api
def api_smtp_put():
    return jsonify(smtp_config.save_smtp(_body()))


@app.post("/api/smtp/test")
@api
def api_smtp_test():
    return jsonify(smtp_config.send_test(_body().get("to", "")))


# --------------------------------------------------------------------------- #
# Dagster API
# --------------------------------------------------------------------------- #
@app.get("/api/dagster/status")
@api
def api_dagster_status():
    return jsonify(_dagster_status_public())


@app.post("/api/dagster/start")
@api
def api_dagster_start():
    dagster_service.start()
    return jsonify(_dagster_status_public())


@app.post("/api/dagster/stop")
@api
def api_dagster_stop():
    return jsonify(dagster_service.stop())


@app.get("/api/dagster/flow-status")
@api
def api_dagster_flow_status():
    return jsonify(_publicise_dagster_links(dagster_client.flow_status()))


@app.get("/api/flow-runs")
@api
def api_flow_runs():
    limit = min(request.args.get("limit", 50, type=int), 200)
    runs = dagster_client.flow_runs(limit=limit)
    names = {f["id"]: f["name"] for f in flows_store.load_flows()}
    for r in runs:
        r["flow_name"] = names.get(r["flow_id"]) or r["job"]
    return jsonify(_publicise_dagster_links(runs))


@app.get("/api/flow-runs/<run_id>/log")
@api
def api_flow_run_log(run_id):
    cursor = request.args.get("cursor") or None
    return jsonify(dagster_client.run_log_tail(run_id, cursor))


# --------------------------------------------------------------------------- #
# Logs API
# --------------------------------------------------------------------------- #
@app.get("/api/logs")
@api
def api_logs():
    return jsonify(workspace.list_log_files())


@app.get("/api/logs/<path:name>")
@api
def api_log_one(name):
    # Offset-based tail: the Monitor page polls with the last offset so only new
    # bytes cross the wire. Without an offset param, fall back to a full read.
    offset = request.args.get("offset", type=int)
    if offset is None:
        return jsonify({"name": name, "content": workspace.read_log_file(name)})
    return jsonify(workspace.tail_log_file(name, offset))


@app.post("/api/logs/purge")
@api
def api_logs_purge():
    b = _body()
    return jsonify(workspace.purge_logs(before=b.get("before"), days=b.get("days")))


# --------------------------------------------------------------------------- #
# Tables API
# --------------------------------------------------------------------------- #
@app.get("/api/tables")
@api
def api_tables_get():
    doc = tables_store.load_raw()
    doc["_branches"] = workspace.branch_keys()
    return jsonify(doc)


@app.post("/api/tables/validate")
@api
def api_tables_validate():
    errs = tables_store.validate(_body())
    return jsonify({"valid": not errs, "errors": errs})


@app.put("/api/tables")
@api
def api_tables_put():
    result = tables_store.save_raw(_body())
    return jsonify({"saved": True, **result})


# --------------------------------------------------------------------------- #
# Iceberg API
# --------------------------------------------------------------------------- #
@app.get("/api/iceberg/tables")
@api
def api_ib_tables():
    return jsonify(iceberg_browser.list_tables())


@app.get("/api/iceberg/tables/<table>")
@api
def api_ib_overview(table):
    return jsonify(iceberg_browser.table_overview(table))


def _run_guard(action: str = "deleting staging tables"):
    """409 body when a pipeline run is alive, else None (mutation allowed)."""
    live = runner.has_live_run()
    if live:
        return jsonify({"error": (
            f"a pipeline run is active ({live['id']}: {live.get('label') or live.get('command', '')}); "
            f"{action} while a run is loading would corrupt the lake"
        )}), 409
    return None


@app.delete("/api/iceberg/tables/<table>")
@api
def api_ib_delete_table(table):
    blocked = _run_guard()
    if blocked:
        return blocked
    return jsonify(iceberg_browser.delete_table(table))


@app.delete("/api/iceberg/tables")
@api
def api_ib_delete_all():
    blocked = _run_guard()
    if blocked:
        return blocked
    include_system = bool(_body().get("include_system"))
    return jsonify(iceberg_browser.delete_all_tables(include_system=include_system))


@app.post("/api/iceberg/tables/<table>/expire-snapshots")
@api
def api_ib_expire_snapshots(table):
    blocked = _run_guard("expiring snapshots")
    if blocked:
        return blocked
    return jsonify(iceberg_maintenance.expire_snapshots(table))


@app.get("/api/iceberg/tables/<table>/sample")
@api
def api_ib_sample(table):
    limit = request.args.get("limit", 50, type=int)
    branch_id = request.args.get("branch_id", None, type=int)
    snapshot_id = request.args.get("snapshot_id", None, type=int)
    return jsonify(iceberg_browser.sample_rows(
        table, limit=min(limit, 1000), branch_id=branch_id, snapshot_id=snapshot_id,
        date_col=request.args.get("date_col") or None,
        date_from=request.args.get("date_from") or None,
        date_to=request.args.get("date_to") or None))


@app.get("/api/iceberg/tables/<table>/export")
@api
def api_ib_export(table):
    branch_id = request.args.get("branch_id", None, type=int)
    snapshot_id = request.args.get("snapshot_id", None, type=int)
    fname = f"{table}" + (f"_branch{branch_id}" if branch_id is not None else "") + ".csv"
    return Response(
        iceberg_browser.iter_csv(
            table, branch_id=branch_id, snapshot_id=snapshot_id,
            date_col=request.args.get("date_col") or None,
            date_from=request.args.get("date_from") or None,
            date_to=request.args.get("date_to") or None),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/iceberg/tables/<table>/branch-counts")
@api
def api_ib_branch_counts(table):
    return jsonify(iceberg_browser.branch_counts(table))


@app.get("/api/iceberg/tables/<table>/aggregate")
@api
def api_ib_aggregate(table):
    by = request.args.get("by", "branch")
    date_col = request.args.get("date_col") or None
    gran = request.args.get("gran", "day")
    return jsonify(iceberg_browser.aggregate(table, by=by, date_col=date_col, gran=gran))


@app.get("/api/iceberg/system/<table>")
@api
def api_ib_system(table):
    limit = request.args.get("limit", 200, type=int)
    return jsonify(iceberg_browser.read_system_table(table, limit=min(limit, 1000)))


@app.get("/api/iceberg/runs")
@api
def api_ib_runs():
    limit = request.args.get("limit", 100, type=int)
    return jsonify(iceberg_browser.read_run_summary(limit_runs=min(limit, 500)))


@app.get("/api/iceberg/runs/<run_id>")
@api
def api_ib_run_detail(run_id):
    return jsonify(iceberg_browser.read_run_detail(run_id))


# --------------------------------------------------------------------------- #
# dbt API
# --------------------------------------------------------------------------- #
@app.get("/api/dbt/models")
@api
def api_dbt_models():
    return jsonify({"models": dbt_project_store.list_models()})


@app.get("/api/dbt/tests")
@api
def api_dbt_tests():
    return jsonify({"tests": dbt_project_store.list_tests()})


@app.get("/api/dbt/file")
@api
def api_dbt_file_get():
    path = request.args.get("path", "")
    return jsonify({"path": path, "content": dbt_project_store.read_file(path)})


@app.put("/api/dbt/file")
@api
def api_dbt_file_put():
    b = _body()
    return jsonify(dbt_project_store.write_file(b.get("path", ""), b.get("content", "")))


@app.post("/api/dbt/file")
@api
def api_dbt_file_create():
    b = _body()
    return jsonify(dbt_project_store.create_from_template(
        b.get("name", ""), b.get("kind", "model"),
        b.get("materialization", "table"), b.get("content")))


@app.get("/api/dbt/template")
@api
def api_dbt_template():
    return jsonify({"content": dbt_project_store.template_for(
        request.args.get("kind", "model"), request.args.get("name", ""),
        request.args.get("materialization", "table"))})


@app.delete("/api/dbt/file")
@api
def api_dbt_file_delete():
    return jsonify({"deleted": dbt_project_store.delete_file(request.args.get("path", ""))})


@app.get("/api/dbt/config")
@api
def api_dbt_config_get():
    return jsonify({"clickhouse": clickhouse_config.get_clickhouse(),
                    "dbt": workspace.dbt_settings()})


@app.put("/api/dbt/config")
@api
def api_dbt_config_put():
    b = _body()
    out = {}
    if b.get("clickhouse") is not None:
        out["clickhouse"] = clickhouse_config.save_clickhouse(b["clickhouse"])
    if b.get("dbt") is not None:
        out["dbt"] = workspace.update_dbt_settings(b["dbt"])
    # Regenerate profiles.yml from the new config (best effort).
    try:
        dbt_config.write_profiles()
    except ValueError:
        pass
    return jsonify(out)


@app.post("/api/dbt/test-connection")
@api
def api_dbt_test_connection():
    return jsonify(clickhouse_config.test_connection())


@app.post("/api/dbt/run")
@api
def api_dbt_run():
    b = _body()
    spec = {"script": "dbt", "dbt_command": b.get("dbt_command", "run"),
            "select": b.get("select", ""), "full_refresh": bool(b.get("full_refresh")),
            "extra": b.get("extra", "")}
    argv, label = commands.build_argv(spec)
    dbt_config.write_profiles()  # ensure the profile is current before running
    return jsonify(runner.start(argv, label=label))


@app.get("/healthz")
def healthz():
    return "ok"


def main() -> None:
    host = os.environ.get("OASIS_GUI_HOST", "127.0.0.1")
    port = int(os.environ.get("OASIS_GUI_PORT", "8765"))
    # Fail closed: refuse to expose the panel on a public interface unless
    # login credentials are configured (it can launch processes and edit config).
    security.check_bind(host, security.gui_credentials())
    # The Werkzeug debugger is an RCE vector; never enable it on a public bind.
    debug = os.environ.get("OASIS_GUI_DEBUG", "0") == "1"
    if debug and not security.debugger_allowed(host):
        print(f"[warn] debugger disabled: refusing debug mode on public bind {host!r}")
        debug = False
    if not security.is_loopback(host):
        print("[info] login required for non-loopback clients")
    print(f"HNH ETLPipeline Manager -> http://{host}:{port}  (Ctrl+C to stop)")
    if os.environ.get("OASIS_DAGSTER_AUTOSTART", "1") == "1":
        try:
            dagster_service.start()
            print(f"Dagster UI -> {config.dagster_base_url()}")
            if not security.is_loopback(config.dagster_host()):
                print(f"[warn] Dagster UI has no authentication and is listening on "
                      f"{config.dagster_host()}:{config.dagster_port()}")
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] could not start Dagster: {exc}")
    app.run(host=host, port=port, debug=debug, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
