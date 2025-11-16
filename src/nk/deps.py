from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

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
