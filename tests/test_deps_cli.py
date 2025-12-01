from __future__ import annotations

import argparse
import json
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


def test_uninstall_dependencies_removes_only_managed(tmp_path):
    opt_root = tmp_path / "opt"
    unidic_root = opt_root / "unidic-root"
    unidic_root.mkdir(parents=True)
    unidic_path = unidic_root / "unidic"
    unidic_path.mkdir()
    (unidic_path / "dicrc").write_text("")
    unidic_link = unidic_root / "current"
    unidic_link.symlink_to(unidic_path)

    voicevox_root = opt_root / "voicevox-root"
    voicevox_root.mkdir()
    voicevox_path = voicevox_root / "voicevox"
    voicevox_path.mkdir()

    manifest = {
        "components": {
            "unidic": {
                "installed_by_nk": True,
                "path": str(unidic_path),
                "symlink": str(unidic_link),
                "root_path": str(unidic_root),
                "root_created_by_nk": True,
            },
            "voicevox": {
                "installed_by_nk": True,
                "path": str(voicevox_path),
                "root_path": str(voicevox_root),
                "root_created_by_nk": True,
            },
            "opt_root": {
                "path": str(opt_root),
                "root_created_by_nk": True,
            },
        }
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    results = deps.uninstall_dependencies(
        manifest_path=manifest_path, allow_outside_home=True
    )
    statuses = {r.name: r.status for r in results}

    assert statuses["unidic"] == "removed"
    assert statuses["unidic-symlink"] == "removed"
    assert statuses["unidic-root"] == "removed"
    assert statuses["voicevox"] == "removed"
    assert statuses["voicevox-root"] == "removed"
    assert statuses["opt-root"] == "removed"
    assert not unidic_path.exists()
    assert not unidic_link.exists()
    assert not unidic_root.exists()
    assert not voicevox_path.exists()
    assert not voicevox_root.exists()
    assert not opt_root.exists()


def test_uninstall_dependencies_requires_manifest(tmp_path):
    missing = tmp_path / "missing.json"
    with pytest.raises(deps.DependencyUninstallError):
        deps.uninstall_dependencies(manifest_path=missing, allow_outside_home=True)


def test_uninstall_dependencies_skips_non_nk_paths(tmp_path):
    voicevox_root = tmp_path / "voicevox-root"
    voicevox_root.mkdir()
    voicevox_path = voicevox_root / "voicevox"
    voicevox_path.mkdir()

    manifest = {
        "components": {
            "voicevox": {
                "installed_by_nk": False,
                "path": str(voicevox_path),
                "root_path": str(voicevox_root),
                "root_created_by_nk": False,
            },
            "opt_root": {
                "path": str(tmp_path / "opt"),
                "root_created_by_nk": False,
            },
        }
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    results = deps.uninstall_dependencies(
        manifest_path=manifest_path, allow_outside_home=True
    )
    statuses = {r.name: r.status for r in results}

    assert statuses["voicevox"] == "skipped"
    assert voicevox_path.exists()
    assert voicevox_root.exists()


def test_run_deps_uninstall_forwards_manifest(monkeypatch, capsys, tmp_path):
    manifest = tmp_path / "manifest.json"
    calls: dict[str, object] = {}

    fake_results = [
        deps.UninstallResult(name="unidic", status="removed", detail="ok"),
        deps.UninstallResult(name="voicevox", status="unsafe", detail="nope"),
    ]

    def _fake_uninstall(manifest_path=None):
        calls["manifest_path"] = manifest_path
        return fake_results

    monkeypatch.setattr(cli, "uninstall_dependencies", _fake_uninstall)

    exit_code = cli._run_deps(argparse.Namespace(command="uninstall", manifest=manifest))
    output = capsys.readouterr().out

    assert calls["manifest_path"] == manifest
    assert "unidic: removed" in output
    assert "voicevox: unsafe" in output
    assert exit_code == 1
