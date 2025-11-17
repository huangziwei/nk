from __future__ import annotations

import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import threading
from typing import Mapping
from uuid import uuid4

from .book_io import write_book_package
from .core import epub_to_chapter_texts, get_epub_cover
from .nlp import NLPBackend, NLPBackendUnavailableError

_INVALID_BOOK_CHARS = set('<>:"/\\|?*')


def _normalize_upload_filename(filename: str | None) -> str:
    if isinstance(filename, str):
        candidate = Path(filename).name.strip()
    else:
        candidate = ""
    if not candidate:
        candidate = "upload.epub"
    if not candidate.lower().endswith(".epub"):
        candidate = f"{candidate}.epub"
    return candidate


def _derive_book_dir_name(filename: str) -> str:
    base = Path(filename or "book").name
    stem = Path(base).with_suffix("").name or "book"
    normalized = stem.strip()
    if not normalized:
        normalized = "book"
    cleaned_chars: list[str] = []
    for ch in normalized:
        if ch in _INVALID_BOOK_CHARS:
            cleaned_chars.append("_")
        elif ord(ch) < 32:
            continue
        else:
            cleaned_chars.append(ch)
    cleaned = "".join(cleaned_chars).strip(" .")
    if not cleaned:
        cleaned = "book"
    return cleaned[:120]


def _relative_path_or_name(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError:
        return path.name or path.as_posix()


def _format_chapter_progress_label(
    book_label: str,
    index: int | None,
    total: int | None,
    title: str | None,
) -> str:
    parts: list[str] = [book_label]
    if isinstance(index, int) and index > 0:
        if isinstance(total, int) and total > 0:
            parts.append(f"{index}/{total}")
        else:
            parts.append(str(index))
    if title:
        clean = str(title).strip()
        if clean:
            parts.append(clean)
    return " · ".join(parts)


class UploadJob:
    def __init__(self, root: Path, filename: str | None) -> None:
        self.root = root
        self.id = uuid4().hex
        self.filename = _normalize_upload_filename(filename)
        self.book_label = self.filename
        self.output_dir = root / _derive_book_dir_name(self.filename)
        self.target_rel = _relative_path_or_name(root, self.output_dir)
        self.status = "pending"
        self.message: str | None = "Waiting to start"
        self.error: str | None = None
        self.progress_index: int | None = None
        self.progress_total: int | None = None
        self.progress_label: str | None = None
        self.progress_event: str | None = None
        self.book_dir_rel: str | None = None
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = self.created_at
        self.temp_dir = Path(tempfile.mkdtemp(prefix="nk-upload-"))
        self.temp_path = self.temp_dir / self.filename
        self.lock = threading.Lock()

    def _touch(self) -> None:
        self.updated_at = datetime.now(timezone.utc)

    def set_status(self, status: str, message: str | None = None) -> None:
        with self.lock:
            self.status = status
            if message is not None:
                self.message = message
            self._touch()

    def set_error(self, message: str) -> None:
        with self.lock:
            self.status = "error"
            self.error = message
            self.message = message
            self._touch()

    def update_progress(
        self,
        index: int | None,
        total: int | None,
        label: str | None,
        event: str | None,
    ) -> None:
        with self.lock:
            self.progress_index = index
            self.progress_total = total
            self.progress_label = label
            self.progress_event = event
            if label:
                self.message = label
            self._touch()

    def mark_success(self) -> None:
        with self.lock:
            self.status = "success"
            self.book_dir_rel = _relative_path_or_name(self.root, self.output_dir)
            if self.book_dir_rel:
                self.message = f"Ready: {self.book_dir_rel}"
            else:
                self.message = "Upload complete"
            self.progress_event = "complete"
            self.progress_label = None
            self._touch()

    def to_payload(self) -> dict[str, object]:
        with self.lock:
            progress_payload: dict[str, object] | None = None
            if any(
                value is not None
                for value in (
                    self.progress_index,
                    self.progress_total,
                    self.progress_label,
                    self.progress_event,
                )
            ):
                progress_payload = {
                    "index": self.progress_index,
                    "total": self.progress_total,
                    "label": self.progress_label,
                    "event": self.progress_event,
                }
            return {
                "id": self.id,
                "filename": self.filename,
                "status": self.status,
                "message": self.message,
                "error": self.error,
                "progress": progress_payload,
                "book_dir": self.book_dir_rel,
                "target_name": self.target_rel,
                "created": self.created_at.isoformat(),
                "updated": self.updated_at.isoformat(),
            }

    def cleanup(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)


class UploadManager:
    def __init__(self, root: Path, max_workers: int = 1) -> None:
        self.root = root
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="nk-upload")
        self.jobs: dict[str, UploadJob] = {}

    def enqueue(self, job: UploadJob) -> UploadJob:
        with self.lock:
            self.jobs[job.id] = job
        self.executor.submit(self._run_job, job)
        return job

    def list_jobs(self) -> list[dict[str, object]]:
        with self.lock:
            snapshot = list(self.jobs.values())
        snapshot.sort(key=lambda job: job.updated_at, reverse=True)
        return [job.to_payload() for job in snapshot]

    def shutdown(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=False)

    def _run_job(self, job: UploadJob) -> None:
        job.set_status("running", "Preparing upload…")
        try:
            backend = NLPBackend()
        except NLPBackendUnavailableError as exc:
            job.set_error(str(exc))
            job.cleanup()
            return

        def _progress_callback(event: Mapping[str, object]) -> None:
            try:
                total = event.get("total")
                if not isinstance(total, int) or total <= 0:
                    total = None
                index = event.get("index")
                if not isinstance(index, int):
                    index = None
                title_value = event.get("title") or event.get("title_hint")
                if not isinstance(title_value, str) or not title_value.strip():
                    source = event.get("source")
                    if isinstance(source, Path):
                        title_value = source.stem
                    elif isinstance(source, str):
                        title_value = Path(source).stem
                    else:
                        title_value = ""
                description = _format_chapter_progress_label(
                    job.book_label,
                    index,
                    total,
                    title_value,
                )
                event_type = event.get("event")
                event_label = event_type if isinstance(event_type, str) else None
                job.update_progress(index, total, description, event_label)
            except Exception:
                return

        try:
            job.set_status("running", "Chapterizing…")
            chapters, ruby_evidence = epub_to_chapter_texts(
                str(job.temp_path),
                nlp=backend,
                progress=_progress_callback,
            )
            job.set_status("running", "Writing chapters…")
            cover = get_epub_cover(str(job.temp_path))
            write_book_package(
                job.output_dir,
                chapters,
                source_epub=job.temp_path,
                cover_image=cover,
                ruby_evidence=ruby_evidence,
            )
            job.mark_success()
        except Exception as exc:
            job.set_error(f"{exc.__class__.__name__}: {exc}")
        finally:
            job.cleanup()


__all__ = ["UploadJob", "UploadManager"]
