import sys

import dagster as dg


def test_asset_key_shape():
    from orchestrator import assets
    assert assets.asset_key("f1", "n1") == dg.AssetKey(["flow_f1", "n1"])


def test_asset_runs_command_and_succeeds(monkeypatch):
    from orchestrator import assets, state
    # A trivial spec whose build_argv yields a fast, zero-exit command.
    monkeypatch.setattr(state, "build_argv",
                        lambda spec: ([sys.executable, "-c", "print('hi')"], "noop"))
    a = assets.build_asset("f1", "n1", "noop", {"script": "x"}, [])
    result = dg.materialize([a])
    assert result.success


def test_asset_raises_on_nonzero(monkeypatch):
    from orchestrator import assets, state
    monkeypatch.setattr(state, "build_argv",
                        lambda spec: ([sys.executable, "-c", "import sys; sys.exit(3)"], "boom"))
    a = assets.build_asset("f1", "n2", "boom", {"script": "x"}, [])
    result = dg.materialize([a], raise_on_error=False)
    assert not result.success
