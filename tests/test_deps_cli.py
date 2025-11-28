from __future__ import annotations

import argparse
from pathlib import Path

import pytest
import nk.cli as cli
import nk.deps as deps


def test_install_dependencies_uses_env_override(monkeypatch, tmp_path):
    script_path = tmp_path / "install.sh"
    script_path.write_text("#!/usr/bin/env bash\n")

    calls: dict[str, object] = {}

    def _fake_run(cmd, check, cwd):
        calls["cmd"] = cmd
        calls["cwd"] = cwd

        class _Result:
            returncode = 3

        return _Result()

    monkeypatch.setattr(deps.subprocess, "run", _fake_run)
    monkeypatch.setenv("NK_INSTALL_SCRIPT", str(script_path))

    exit_code = deps.install_dependencies()

    assert exit_code == 3
    assert calls["cmd"] == ["bash", str(script_path)]
    assert calls["cwd"] == str(script_path.parent)


def test_install_dependencies_missing_script_raises(monkeypatch, tmp_path):
    missing = tmp_path / "install.sh"
    monkeypatch.setenv("NK_INSTALL_SCRIPT", str(missing))

    with pytest.raises(deps.DependencyInstallError) as excinfo:
        deps.install_dependencies()
    assert str(missing) in str(excinfo.value)


def test_run_deps_defaults_to_check(monkeypatch, capsys):
    statuses = [
        deps.DependencyStatus(
            name="One",
            available=True,
            path=Path("/tmp/one"),
            version="1.0",
            detail=None,
        ),
        deps.DependencyStatus(
            name="Two",
            available=False,
            path=None,
            version=None,
            detail="missing",
        ),
    ]
    monkeypatch.setattr(cli, "dependency_statuses", lambda: statuses)

    exit_code = cli._run_deps(argparse.Namespace(command=None))
    output = capsys.readouterr().out

    assert "One: OK" in output
    assert "Two: MISSING" in output
    assert exit_code == 1


def test_run_deps_install_forwards_script(monkeypatch, tmp_path):
    script = tmp_path / "custom.sh"

    calls: dict[str, object] = {}

    def _fake_install(script_path=None):
        calls["script_path"] = script_path
        return 0

    monkeypatch.setattr(cli, "install_dependencies", _fake_install)

    exit_code = cli._run_deps(argparse.Namespace(command="install", script=script))

    assert exit_code == 0
    assert calls["script_path"] == script
