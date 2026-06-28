"""Asset factory: one subprocess-running asset per flow node.

The asset runs exactly the argv the Run page would (via state.build_argv),
streams child output into the Dagster run log, and fails the asset on a
non-zero exit. Dependencies are declared with ``deps=`` so Dagster runs nodes
in topological order and only starts a downstream node after upstreams succeed.
"""
from __future__ import annotations

import subprocess
import time
from typing import Any

import dagster as dg
from dagster import MaterializeResult, MetadataValue, Failure, AssetKey

from orchestrator import state


def asset_key(flow_id: str, node_id: str) -> AssetKey:
    return AssetKey([f"flow_{flow_id}", node_id])


def build_asset(flow_id: str, node_id: str, name: str, spec: dict[str, Any],
                dep_keys: list[AssetKey]) -> dg.AssetsDefinition:
    key = asset_key(flow_id, node_id)

    @dg.asset(key=key, deps=dep_keys, group_name=f"flow_{flow_id}",
              description=name, compute_kind="subprocess")
    def _asset(context) -> MaterializeResult:
        argv, label = state.build_argv(spec)
        context.log.info("Running: %s", " ".join(argv))
        start = time.time()
        proc = subprocess.Popen(
            argv, cwd=str(state.REPO_ROOT),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            context.log.info(line.rstrip())
        rc = proc.wait()
        duration = round(time.time() - start, 1)
        if rc != 0:
            raise Failure(description=f"{label}: command exited with code {rc}")
        return MaterializeResult(metadata={
            "exit_code": rc,
            "duration_s": duration,
            "command": MetadataValue.text(" ".join(argv)),
        })

    return _asset
