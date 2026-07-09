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

# Allow both ``python gui/app.py`` and ``python -m gui.app`` by ensuring this
# directory is importable for the flat module imports below.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from flask import Flask, Response, jsonify, render_template, request  # noqa: E402

import clickhouse_config  # noqa: E402
import commands  # noqa: E402
import config  # noqa: E402
import connections  # noqa: E402
import dagster_client  # noqa: E402
import dbt_config  # noqa: E402
import dbt_project_store  # noqa: E402
import flows_store  # noqa: E402
import iceberg_browser  # noqa: E402
import pipelines_store  # noqa: E402
import smtp_config  # noqa: E402
import tables_store  # noqa: E402
import workspace  # noqa: E402
from config import ensure_dirs  # noqa: E402
from dagster_service import service as dagster_service  # noqa: E402
from pipeline_runner import RunManager  # noqa: E402

app = Flask(__name__)
runner = RunManager()
ensure_dirs()


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
        "dagster": dagster_service.status(),
    })


@app.post("/api/flows")
@api
def api_flows_add():
    b = _body()
    f = flows_store.add_flow(b.get("name", ""), b.get("nodes", []),
                             b.get("cron", ""), b.get("timezone", "UTC"),
                             b.get("email", {}), b.get("enabled", True))
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
    return jsonify(dagster_client.launch_job(f"flow_{fid}"))


@app.post("/api/flows/<fid>/toggle")
@api
def api_flows_toggle(fid):
    enabled = bool(_body().get("enabled", True))
    flows_store.update_flow(fid, enabled=enabled)
    dagster_client.reload_location()
    fn = f"flow_{fid}_schedule"
    res = dagster_client.start_schedule(fn) if enabled else dagster_client.stop_schedule(fn)
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
    return jsonify(dagster_service.status())


@app.post("/api/dagster/start")
@api
def api_dagster_start():
    return jsonify(dagster_service.start())


@app.post("/api/dagster/stop")
@api
def api_dagster_stop():
    return jsonify(dagster_service.stop())


@app.get("/api/dagster/flow-status")
@api
def api_dagster_flow_status():
    return jsonify(dagster_client.flow_status())


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
    return jsonify({"name": name, "content": workspace.read_log_file(name)})


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


@app.get("/api/iceberg/tables/<table>/sample")
@api
def api_ib_sample(table):
    limit = request.args.get("limit", 50, type=int)
    branch_id = request.args.get("branch_id", None, type=int)
    snapshot_id = request.args.get("snapshot_id", None, type=int)
    return jsonify(iceberg_browser.sample_rows(
        table, limit=min(limit, 500), branch_id=branch_id, snapshot_id=snapshot_id,
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
    return jsonify(iceberg_browser.read_system_table(table, limit=min(limit, 2000)))


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
        b.get("name", ""), b.get("kind", "model"), b.get("materialization", "table")))


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
    debug = os.environ.get("OASIS_GUI_DEBUG", "0") == "1"
    print(f"HNH ETLPipeline Manager -> http://{host}:{port}  (Ctrl+C to stop)")
    if os.environ.get("OASIS_DAGSTER_AUTOSTART", "1") == "1":
        try:
            dagster_service.start()
            print(f"Dagster UI -> {config.dagster_base_url()}")
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] could not start Dagster: {exc}")
    app.run(host=host, port=port, debug=debug, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
