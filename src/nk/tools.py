from __future__ import annotations

import os
import shutil
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

DEFAULT_UNIDIC_URL = "https://clrd.ninjal.ac.jp/unidic_archive/cwj/3.1.1/unidic-cwj-3.1.1-full.zip"
UNIDIC_VERSION = "3.1.1"


class UniDicInstallError(RuntimeError):
    pass


@dataclass(slots=True)
class UniDicStatus:
    version: str | None
    path: Path | None
    managed: bool


def _venv_share_root() -> Path:
    prefix = Path(sys.prefix)
    return prefix / "share" / "nk" / "unidic"


def _current_marker(root: Path) -> Path:
    return root / "current"


def _version_dir(root: Path, version: str) -> Path:
    return root / version


def _write_marker(marker: Path, target: Path) -> None:
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(str(target), encoding="utf-8")


def _read_marker(marker: Path) -> Path | None:
    try:
        target = Path(marker.read_text(encoding="utf-8").strip())
    except FileNotFoundError:
        return None
    if target.exists():
        return target
    return None


def ensure_unidic_installed(
    *,
    url: str | None = DEFAULT_UNIDIC_URL,
    zip_path: str | None = None,
    force: bool = False,
) -> UniDicStatus:
    root = _venv_share_root()
    marker = _current_marker(root)
    target_dir = _version_dir(root, UNIDIC_VERSION)

    if target_dir.exists() and not force:
        _write_marker(marker, target_dir)
        return UniDicStatus(version=UNIDIC_VERSION, path=target_dir, managed=True)

    archive_path = Path(zip_path) if zip_path else None
    if archive_path is not None and not archive_path.is_file():
        raise UniDicInstallError(f"Archive not found: {archive_path}")

    if archive_path is None:
        if url is None:
            raise UniDicInstallError("No download URL provided for UniDic installation.")
        archive_path = _download_unidic_archive(url, root)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        _extract_archive(archive_path, tmp_path)
        dic_root = _locate_dic_root(tmp_path)
        if dic_root is None:
            raise UniDicInstallError("Failed to locate dicrc inside the UniDic archive.")
        if target_dir.exists():
            shutil.rmtree(target_dir)
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(dic_root), str(target_dir))

    _write_marker(marker, target_dir)
    return UniDicStatus(version=UNIDIC_VERSION, path=target_dir, managed=True)


def _download_unidic_archive(url: str, root: Path) -> Path:
    downloads = root / "downloads"
    downloads.mkdir(parents=True, exist_ok=True)
    filename = url.rstrip("/").split("/")[-1] or "unidic.zip"
    archive_path = downloads / filename
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise UniDicInstallError(f"Failed to download UniDic archive: {exc}") from exc

    total = response.headers.get("Content-Length")
    total_bytes = int(total) if total and total.isdigit() else None
    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TimeRemainingColumn(),
        transient=True,
    )
    with archive_path.open("wb") as handle, progress:
        task = progress.add_task("Downloading UniDic 3.1.1", total=total_bytes)
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            handle.write(chunk)
            progress.advance(task, len(chunk))
    return archive_path


def _extract_archive(archive_path: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive_path) as zf:
        members = zf.infolist()
        total = len(members) or None
        progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            transient=True,
        )
        with progress:
            task = progress.add_task("Extracting UniDic 3.1.1", total=total)
            for member in members:
                zf.extract(member, destination)
                progress.advance(task, 1)


def _locate_dic_root(base: Path) -> Path | None:
    for path in _iter_dirs(base):
        if (path / "dicrc").is_file():
            return path
    return None


def _iter_dirs(base: Path) -> Iterable[Path]:
    stack = [base]
    while stack:
        current = stack.pop()
        if current.is_dir():
            yield current
            for child in current.iterdir():
                if child.is_dir():
                    stack.append(child)


def resolve_managed_unidic() -> UniDicStatus:
    marker = _current_marker(_venv_share_root())
    target = _read_marker(marker)
    if target is None:
        return UniDicStatus(version=None, path=None, managed=False)
    if not (target / "dicrc").exists():
        return UniDicStatus(version=None, path=None, managed=True)
    return UniDicStatus(version=UNIDIC_VERSION, path=target, managed=True)


def get_unidic_dicdir() -> Path | None:
    env_dir = os.environ.get("NK_UNIDIC_DIR")
    if env_dir:
        candidate = Path(env_dir).expanduser()
        if (candidate / "dicrc").exists():
            return candidate
    managed = resolve_managed_unidic()
    if managed.path and (managed.path / "dicrc").exists():
        return managed.path
    try:
        import unidic  # type: ignore
    except ImportError:
        return None
    dicdir = Path(getattr(unidic, "DICDIR", ""))
    if dicdir and (dicdir / "dicrc").exists():
        return dicdir
    return None
