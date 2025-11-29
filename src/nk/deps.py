from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping

try:
    import importlib.resources as resources
except ImportError:  # pragma: no cover - py<3.9 fallback
    import importlib_resources as resources  # type: ignore

UNIDIC_VERSION = "3.1.1"
UNIDIC_ARCHIVE_URL = (
    "https://clrd.ninjal.ac.jp/unidic_archive/cwj/3.1.1/unidic-cwj-3.1.1-full.zip"
)


@dataclass(slots=True)
class DependencyStatus:
    name: str
    available: bool
    path: Path | None
    version: str | None
    detail: str | None = None


class DependencyInstallError(RuntimeError):
    """Raised when nk cannot invoke the install helper script."""


class DependencyUninstallError(RuntimeError):
    """Raised when nk cannot safely uninstall managed dependencies."""


_STATE_DIR_ENV = "NK_STATE_DIR"
_MANIFEST_FILENAME = "deps-manifest.json"


def _state_dir() -> Path:
    env_dir = os.environ.get(_STATE_DIR_ENV)
    if env_dir:
        return Path(env_dir).expanduser()
    return Path.home() / ".local" / "share" / "nk"


def _manifest_path(manifest_path: Path | None = None) -> Path:
    if manifest_path:
        return manifest_path
    return _state_dir() / _MANIFEST_FILENAME


@dataclass(slots=True)
class UninstallResult:
    name: str
    status: str
    detail: str


def _candidate_unidic_paths() -> Iterable[Path]:
    env_root = os.environ.get("NK_UNIDIC_ROOT")
    if env_root:
        root = Path(env_root).expanduser()
        yield root / UNIDIC_VERSION
        yield root / "current"

    default_root = Path.home() / "opt" / "unidic"
    yield default_root / UNIDIC_VERSION
    yield default_root / "current"

    managed_root = Path(sys.prefix) / "share" / "nk" / "unidic"
    yield managed_root / UNIDIC_VERSION
    yield managed_root / "current"


def get_unidic_dicdir() -> Path | None:
    env_dir = os.environ.get("NK_UNIDIC_DIR")
    if env_dir:
        candidate = Path(env_dir).expanduser()
        if (candidate / "dicrc").exists():
            return candidate

    for candidate in _candidate_unidic_paths():
        if (candidate / "dicrc").exists():
            return candidate

    try:
        import unidic  # type: ignore
    except ImportError:
        return None
    dicdir = Path(getattr(unidic, "DICDIR", ""))
    if dicdir and (dicdir / "dicrc").exists():
        return dicdir
    return None


def describe_unidic() -> DependencyStatus:
    dicdir = get_unidic_dicdir()
    available = dicdir is not None
    version = None
    detail = None
    if dicdir:
        version = UNIDIC_VERSION if UNIDIC_VERSION in dicdir.as_posix() else None
        if version is None:
            detail = "Custom UniDic path in use; version unknown."
    else:
        detail = "Install via install.sh or set NK_UNIDIC_DIR/NK_UNIDIC_ROOT."
    return DependencyStatus(
        name="UniDic",
        available=available,
        path=dicdir,
        version=version,
        detail=detail,
    )


def _read_voicevox_version(root: Path) -> str | None:
    version_file = root / ".nk-voicevox-version"
    if version_file.is_file():
        text = version_file.read_text(encoding="utf-8").strip()
        return text or None
    return None


def _discover_voicevox_runtime() -> Path | None:
    try:
        from .tts import discover_voicevox_runtime
    except Exception:
        return None
    return discover_voicevox_runtime("http://127.0.0.1:50021")


def describe_voicevox() -> DependencyStatus:
    runtime = _discover_voicevox_runtime()
    available = runtime is not None
    version = None
    detail = None
    install_dir = runtime.parent if runtime else None
    if install_dir:
        version = _read_voicevox_version(install_dir)
        if version is None:
            detail = "Version marker (.nk-voicevox-version) not found."
    else:
        detail = "VoiceVox runtime not detected. Run install.sh or set VOICEVOX_RUNTIME."
    return DependencyStatus(
        name="VoiceVox",
        available=available,
        path=install_dir,
        version=version,
        detail=detail,
    )


def describe_ffmpeg() -> DependencyStatus:
    ffmpeg_path = shutil.which("ffmpeg")
    available = ffmpeg_path is not None
    version = None
    detail = None
    if ffmpeg_path:
        try:
            result = subprocess.run(
                [ffmpeg_path, "-version"],
                capture_output=True,
                text=True,
                check=False,
            )
            first_line = (result.stdout or "").splitlines()
            if first_line:
                version = first_line[0].strip()
        except Exception:
            detail = "Failed to query ffmpeg version."
    else:
        detail = "ffmpeg command not found in PATH."
    return DependencyStatus(
        name="ffmpeg",
        available=available,
        path=Path(ffmpeg_path) if ffmpeg_path else None,
        version=version,
        detail=detail,
    )


def dependency_statuses() -> List[DependencyStatus]:
    return [
        describe_unidic(),
        describe_voicevox(),
        describe_ffmpeg(),
    ]


def _resolve_install_script(script_path: Path | None = None) -> Path:
    env_path = os.environ.get("NK_INSTALL_SCRIPT")
    if env_path and not script_path:
        return Path(env_path).expanduser()
    if script_path:
        return script_path

    module_path = Path(__file__).resolve()
    search_roots = [module_path.parent, *module_path.parents]
    for base in search_roots:
        candidate = base / "install.sh"
        if candidate.is_file():
            return candidate

    default_path = (
        module_path.parents[2] / "install.sh"
        if len(module_path.parents) >= 3
        else module_path.parent / "install.sh"
    )
    try:
        package_files = resources.files(__package__ or "nk")
        candidate = package_files.joinpath("install.sh")
        with resources.as_file(candidate) as extracted:
            if extracted.is_file():
                return extracted
    except Exception:
        pass
    raise DependencyInstallError(
        f"install.sh not found at {default_path}. "
        "Reinstall nk from PyPI or set NK_INSTALL_SCRIPT/--script to point at a local copy."
    )


def install_dependencies(*, script_path: Path | None = None) -> int:
    """
    Invoke the project install.sh helper and return its exit code.

    The optional NK_INSTALL_SCRIPT env var or script_path argument can override the
    location for testing or custom layouts.
    """
    install_script = _resolve_install_script(script_path).expanduser().resolve()
    if not install_script.is_file():
        raise DependencyInstallError(
            f"install.sh not found at {install_script}. "
            "Reinstall nk from PyPI or set NK_INSTALL_SCRIPT/--script to point at a local copy."
        )

    try:
        result = subprocess.run(
            ["bash", str(install_script)],
            check=False,
            cwd=str(install_script.parent),
        )
    except FileNotFoundError as exc:
        raise DependencyInstallError("bash is required to run install.sh.") from exc
    except PermissionError as exc:
        raise DependencyInstallError(
            f"Permission denied executing install.sh at {install_script}"
        ) from exc
    return int(result.returncode)


def _load_install_manifest(manifest_path: Path | None = None) -> dict:
    manifest_file = _manifest_path(manifest_path).expanduser()
    if not manifest_file.is_file():
        raise DependencyUninstallError(
            f"Install manifest not found at {manifest_file}. Run `nk deps install` first."
        )
    try:
        with manifest_file.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as exc:
        raise DependencyUninstallError(
            f"Install manifest at {manifest_file} is not valid JSON."
        ) from exc


def _path_is_under_home(path: Path, *, allow_outside_home: bool) -> bool:
    if allow_outside_home:
        return True
    home = Path.home().resolve()
    try:
        return path.is_relative_to(home)
    except AttributeError:  # pragma: no cover - python <3.9 fallback
        return str(path).startswith(str(home))
    except RuntimeError:
        return False


def _remove_path_if_safe(
    name: str, path: Path, *, allow_outside_home: bool
) -> UninstallResult:
    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = candidate.absolute()
    is_link = candidate.is_symlink()
    safety_target = candidate if is_link else candidate.resolve()
    if not _path_is_under_home(safety_target, allow_outside_home=allow_outside_home):
        return UninstallResult(
            name=name,
            status="unsafe",
            detail=f"Refusing to remove outside home: {safety_target}",
        )

    exists = candidate.exists() or candidate.is_symlink()
    if not exists:
        return UninstallResult(
            name=name,
            status="missing",
            detail=str(candidate),
        )
    try:
        if candidate.is_dir() and not is_link:
            shutil.rmtree(candidate)
        else:
            candidate.unlink()
    except Exception as exc:
        return UninstallResult(
            name=name,
            status="error",
            detail=f"{candidate}: {exc}",
        )
    return UninstallResult(name=name, status="removed", detail=str(candidate))


def _remove_root_if_created(
    name: str,
    data: Mapping[str, object],
    *,
    allow_outside_home: bool,
) -> list[UninstallResult]:
    root_path = data.get("root_path") or data.get("path")
    created_flag = data.get("root_created_by_nk")
    if created_flag is None:
        created_flag = data.get("created_by_nk")
    created_by_nk = bool(created_flag)
    if not created_by_nk or not root_path:
        return []
    root = Path(str(root_path)).expanduser()
    if not root.is_absolute():
        root = root.absolute()
    if not _path_is_under_home(root, allow_outside_home=allow_outside_home):
        return [
            UninstallResult(
                name=f"{name}-root",
                status="unsafe",
                detail=f"Refusing to remove outside home: {root}",
            )
        ]
    if not root.exists():
        return [
            UninstallResult(
                name=f"{name}-root",
                status="missing",
                detail=str(root),
            )
        ]
    try:
        contents = list(root.iterdir())
    except Exception as exc:
        return [
            UninstallResult(
                name=f"{name}-root",
                status="error",
                detail=f"{root}: {exc}",
            )
        ]
    if contents:
        return [
            UninstallResult(
                name=f"{name}-root",
                status="nonempty",
                detail=str(root),
            )
        ]
    try:
        root.rmdir()
    except Exception as exc:
        return [
            UninstallResult(
                name=f"{name}-root",
                status="error",
                detail=f"{root}: {exc}",
            )
        ]
    return [
        UninstallResult(
            name=f"{name}-root",
            status="removed",
            detail=str(root),
        )
    ]


def _uninstall_component(
    name: str,
    data: Mapping[str, object] | None,
    *,
    allow_outside_home: bool,
) -> list[UninstallResult]:
    if not isinstance(data, Mapping):
        return [
            UninstallResult(
                name=name,
                status="error",
                detail="Invalid manifest entry.",
            )
        ]
    installed_by_nk = bool(data.get("installed_by_nk"))
    if not installed_by_nk:
        return [
            UninstallResult(
                name=name,
                status="skipped",
                detail="Not installed by nk; leaving untouched.",
            )
        ]
    raw_path = data.get("path")
    if not raw_path:
        return [
            UninstallResult(
                name=name,
                status="skipped",
                detail="No path recorded in manifest.",
            )
        ]

    results = [
        _remove_path_if_safe(
            name,
            Path(str(raw_path)),
            allow_outside_home=allow_outside_home,
        )
    ]
    symlink_path = data.get("symlink")
    if symlink_path:
        results.append(
            _remove_path_if_safe(
                f"{name}-symlink",
                Path(str(symlink_path)),
                allow_outside_home=allow_outside_home,
            )
        )
    results.extend(
        _remove_root_if_created(
            name,
            data,
            allow_outside_home=allow_outside_home,
        )
    )
    return results


def uninstall_dependencies(
    *,
    manifest_path: Path | None = None,
    allow_outside_home: bool = False,
) -> list[UninstallResult]:
    """
    Remove nk-managed dependencies that were installed via install.sh.

    Only entries recorded as installed_by_nk in the install manifest are removed.
    """
    manifest = _load_install_manifest(manifest_path)
    components: Mapping[str, object] = {}
    if isinstance(manifest, Mapping):
        maybe_components = manifest.get("components", {})
        if isinstance(maybe_components, Mapping):
            components = maybe_components
    results: list[UninstallResult] = []
    for key in ("unidic", "voicevox"):
        results.extend(
            _uninstall_component(
                key,
                components.get(key),
                allow_outside_home=allow_outside_home,
            )
        )
    opt_root_data = components.get("opt_root")
    if isinstance(opt_root_data, Mapping):
        results.extend(
            _remove_root_if_created(
                "opt",
                opt_root_data,
                allow_outside_home=allow_outside_home,
            )
        )
    return results
