"""OASIS pipeline control panel -- a small Flask GUI.

Manage / run / schedule the Oracle->Iceberg pipeline, monitor the workspace
(run logs, control state, DQ results), edit ``tables.json``, and browse the
Iceberg lake metadata. Launch with ``python gui/app.py`` (or via the setup
scripts) and open http://127.0.0.1:8765.
"""

from __future__ import annotations

import functools
import os
import sys
from pathlib import Path

# Allow both ``python gui/app.py`` and ``python -m gui.app`` by ensuring this
# directory is importable for the flat module imports below.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from flask import Flask, jsonify, render_template, request  # noqa: E402

import commands  # noqa: E402
import cron_manager  # noqa: E402
import iceberg_browser  # noqa: E402
import tables_store  # noqa: E402
import workspace  # noqa: E402
from config import ensure_dirs  # noqa: E402
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


@app.route("/schedule")
def page_schedule():
    return render_template("schedule.html", active="schedule")


@app.route("/logs")
def page_logs():
    return render_template("logs.html", active="logs")


@app.route("/tables")
def page_tables():
    return render_template("tables.html", active="tables")


@app.route("/iceberg")
def page_iceberg():
    return render_template("iceberg.html", active="iceberg")


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
            "cron": cron_manager.status()["available"],
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
    return jsonify(iceberg_browser.sample_rows(table, limit=min(limit, 500), branch_id=branch_id))


@app.get("/api/iceberg/tables/<table>/branch-counts")
@api
def api_ib_branch_counts(table):
    return jsonify(iceberg_browser.branch_counts(table))


@app.get("/api/iceberg/system/<table>")
@api
def api_ib_system(table):
    limit = request.args.get("limit", 200, type=int)
    return jsonify(iceberg_browser.read_system_table(table, limit=min(limit, 2000)))


# --------------------------------------------------------------------------- #
# Schedule API
# --------------------------------------------------------------------------- #
@app.get("/api/schedules")
@api
def api_sched_list():
    return jsonify(
        {
            "jobs": cron_manager.list_jobs(),
            "status": cron_manager.status(),
            "presets": cron_manager.PRESETS,
            "branches": workspace.branch_keys(),
        }
    )


@app.post("/api/schedules")
@api
def api_sched_add():
    b = _body()
    job = cron_manager.add_job(b.get("name", ""), b.get("expr", ""), b.get("spec", {}))
    return jsonify(job)


@app.put("/api/schedules/<job_id>")
@api
def api_sched_update(job_id):
    return jsonify(cron_manager.update_job(job_id, **_body()))


@app.delete("/api/schedules/<job_id>")
@api
def api_sched_delete(job_id):
    return jsonify({"deleted": cron_manager.delete_job(job_id)})


@app.post("/api/schedules/install")
@api
def api_sched_install():
    return jsonify(cron_manager.install())


@app.post("/api/schedules/uninstall")
@api
def api_sched_uninstall():
    return jsonify(cron_manager.uninstall())


@app.get("/healthz")
def healthz():
    return "ok"


def main() -> None:
    host = os.environ.get("OASIS_GUI_HOST", "127.0.0.1")
    port = int(os.environ.get("OASIS_GUI_PORT", "8765"))
    debug = os.environ.get("OASIS_GUI_DEBUG", "0") == "1"
    print(f"HNH ETLPipeline Manager -> http://{host}:{port}  (Ctrl+C to stop)")
    app.run(host=host, port=port, debug=debug, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
