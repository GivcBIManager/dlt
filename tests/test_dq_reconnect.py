"""A dropped Oracle connection costs DQ one retry, not the rest of the branch.

Each branch checks its tables one after another on a single connection. When the
database (or a DBA) drops it -- alrabwah restarted mid-run on 2026-09-30, khamis had
its session killed on 2026-09-29 -- every remaining table used to fail instantly with
"not connected", turning one incident into 38 (or 16) ERROR units and a failed run.
Now a unit that errors on a dead connection gets one reconnect and one retry.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import oracledb

from etl import dq_check, oracle_extract
from etl.config import CATEGORY_MASTER, BranchConfig, Settings, TableDef
from etl.dq_check import DqResult

BRANCH = BranchConfig(key="alrabwah", name="a", id=1, host="h", port=1521,
                      username="u", password="p", database="d")


def _tdef(name: str) -> TableDef:
    return TableDef(table=f"OASIS.{name.upper()}", unique_key="ID", cdc_column="AMEND_LAST_DATE",
                    where_date_column=None, where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_MASTER)


TABLES = [_tdef(n) for n in ("t1", "t2", "t3", "t4")]


class _Conn:
    def __init__(self):
        self.alive = True

    def ping(self):
        if not self.alive:
            raise RuntimeError("DPY-1001: not connected to database")

    def close(self):
        pass


def _run(monkeypatch, check, connect):
    monkeypatch.setattr(dq_check, "open_lake_table", lambda root, name: None)
    monkeypatch.setattr(dq_check, "dataset_root", lambda settings: Path("."))
    monkeypatch.setattr(oracle_extract, "ensure_oracle_client", lambda settings: None)
    monkeypatch.setattr(oracledb, "connect", connect)
    monkeypatch.setattr(dq_check, "check_unit", check)
    results = dq_check.run_dq(TABLES, [BRANCH], Settings(progress_enabled=False), {},
                              dt.date(2026, 9, 1), None)
    return {r.table: r for r in results}


def _res(tdef, status="OK", error=None):
    return DqResult(table=tdef.dataset_table_name, source_table=tdef.table,
                    branch=BRANCH.key, status=status, error=error)


def test_dropped_connection_is_reconnected_and_the_unit_retried(monkeypatch):
    conns, calls = [], []

    def connect(**kw):
        conns.append(_Conn())
        return conns[-1]

    def check(tdef, branch, settings, st, entry, since, until, do_hash, conn=None, self_test=False):
        calls.append(tdef.dataset_table_name)
        if tdef.dataset_table_name == "t2" and conn is conns[0]:
            conn.alive = False          # the database dropped us mid-unit
            return _res(tdef, "ERROR", "DPY-4011: the database or network closed the connection")
        return _res(tdef)

    out = _run(monkeypatch, check, connect)
    assert {t: r.status for t, r in out.items()} == {"t1": "OK", "t2": "OK", "t3": "OK", "t4": "OK"}
    assert len(conns) == 2                       # one reconnect
    assert calls == ["t1", "t2", "t2", "t3", "t4"]  # only the dropped unit is retried


def test_failed_reconnect_errors_the_rest_without_hammering_the_database(monkeypatch):
    conns, calls = [], []

    def connect(**kw):
        if conns:                                # the database is still down
            raise RuntimeError("ORA-12518: TNS:listener could not hand off client connection")
        conns.append(_Conn())
        return conns[-1]

    def check(tdef, branch, settings, st, entry, since, until, do_hash, conn=None, self_test=False):
        calls.append(tdef.dataset_table_name)
        if tdef.dataset_table_name == "t2":
            conn.alive = False
            return _res(tdef, "ERROR", "ORA-03135: connection lost contact")
        return _res(tdef)

    out = _run(monkeypatch, check, connect)
    assert {t: r.status for t, r in out.items()} == {"t1": "OK", "t2": "ERROR", "t3": "ERROR", "t4": "ERROR"}
    assert calls == ["t1", "t2"]                 # t3/t4 are not attempted on a dead branch
    assert "reconnect failed" in out["t3"].error and "ORA-12518" in out["t3"].error


def test_an_error_on_a_healthy_connection_is_not_retried(monkeypatch):
    conns, calls = [], []

    def connect(**kw):
        conns.append(_Conn())
        return conns[-1]

    def check(tdef, branch, settings, st, entry, since, until, do_hash, conn=None, self_test=False):
        calls.append(tdef.dataset_table_name)
        if tdef.dataset_table_name == "t2":
            return _res(tdef, "ERROR", "ORA-00942: table or view does not exist")
        return _res(tdef)

    out = _run(monkeypatch, check, connect)
    assert out["t2"].status == "ERROR" and out["t3"].status == "OK"
    assert len(conns) == 1 and calls == ["t1", "t2", "t3", "t4"]


def test_a_unit_is_retried_only_once(monkeypatch):
    conns, calls = [], []

    def connect(**kw):
        conns.append(_Conn())
        return conns[-1]

    def check(tdef, branch, settings, st, entry, since, until, do_hash, conn=None, self_test=False):
        calls.append(tdef.dataset_table_name)
        if tdef.dataset_table_name == "t2":
            conn.alive = False           # drops every time (e.g. a query the DBA keeps killing)
            return _res(tdef, "ERROR", "ORA-00028: your session has been killed")
        return _res(tdef)

    out = _run(monkeypatch, check, connect)
    assert out["t2"].status == "ERROR"
    assert calls.count("t2") == 2                # original + one retry, never a loop
    assert out["t3"].status == "OK" and out["t4"].status == "OK"   # later units get a fresh connection
