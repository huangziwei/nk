from __future__ import annotations

import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from uuid import uuid4

from .book_io import write_book_package
from .core import epub_to_chapter_texts, get_epub_cover
from .nlp import NLPBackend, NLPBackendUnavailableError
from .refine import OverrideRule, load_override_config, load_refine_config, refine_book

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
    def __init__(
        self,
        root: Path,
        filename: str | None,
        *,
        source_path: Path | None = None,
        output_dir: Path | None = None,
        force: bool = False,
    ) -> None:
        self.root = root
        self.id = uuid4().hex
        self.filename = _normalize_upload_filename(filename)
        self.book_label = self.filename
        self.force = force
        self._owns_temp = source_path is None
        target_path: Path | None = None
        if output_dir is not None:
            target_path = output_dir
        if target_path is None:
            if source_path is None:
                target_path = root / _derive_book_dir_name(self.filename)
            else:
                target_path = source_path.with_suffix("").with_name(
                    _derive_book_dir_name(source_path.stem)
                )
        try:
            target_rel = target_path.resolve().relative_to(root)
            target_path = root / target_rel
        except ValueError:
            target_path = root / _derive_book_dir_name(target_path.name)
        self.output_dir = target_path
        if source_path is not None:
            self.book_label = source_path.name
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
        self.timed_out = False
        if source_path is None:
            self.temp_dir = Path(tempfile.mkdtemp(prefix="nk-upload-"))
            self.temp_path = self.temp_dir / self.filename
        else:
            self.temp_dir = None
            self.temp_path = source_path
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
            if self.timed_out:
                return
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
            if self.timed_out:
                return
            self.progress_index = index
            self.progress_total = total
            self.progress_label = label
            self.progress_event = event
            if label:
                self.message = label
            self._touch()

    def mark_success(self) -> None:
        with self.lock:
            if self.timed_out:
                return
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
        if self._owns_temp and self.temp_dir is not None:
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def mark_timeout(self, message: str) -> None:
        with self.lock:
            if self.status != "running":
                return
            self.timed_out = True
            self.status = "error"
            self.error = message
            self.message = message
            self.progress_event = "timeout"
            self._touch()


class UploadManager:
    def __init__(self, root: Path, max_workers: int = 2) -> None:
        self.root = root
        self.lock = threading.Lock()
        workers = max_workers
        env_workers = os.getenv("NK_UPLOAD_WORKERS")
        if env_workers:
            try:
                parsed = int(env_workers)
                if parsed > 0:
                    workers = parsed
            except ValueError:
                workers = max_workers
        workers = max(1, min(workers, 8))
        self.executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="nk-upload")
        self.jobs: dict[str, UploadJob] = {}
        self.watchdog_interval = 10  # seconds
        self.timeout_seconds = 600  # mark running jobs stale after 10 minutes without updates
        self._stop_event = threading.Event()
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, name="nk-upload-watchdog", daemon=True
        )
        self._watchdog_thread.start()

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
        self._stop_event.set()
        if self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=1)

    def _watchdog_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                now = datetime.now(timezone.utc)
                with self.lock:
                    jobs = list(self.jobs.values())
                for job in jobs:
                    with job.lock:
                        status = job.status
                        delta = (now - job.updated_at).total_seconds()
                    if status != "running":
                        continue
                    if delta >= self.timeout_seconds:
                        job.mark_timeout(
                            "Upload stuck: no progress for 10 minutes; marked failed."
                        )
            except Exception:
                pass
            time.sleep(self.watchdog_interval)

    def _run_job(self, job: UploadJob) -> None:
        job.set_status("running", "Preparing upload…")
        try:
            backend = NLPBackend()
        except NLPBackendUnavailableError as exc:
            job.set_error(str(exc))
            job.cleanup()
            return

        chapter_total_hint: int | None = None
        total_steps: int | None = None
        completed_steps = 0

        def _progress_callback(event: Mapping[str, object]) -> None:
            nonlocal chapter_total_hint, total_steps
            try:
                total = event.get("total")
                if not isinstance(total, int) or total <= 0:
                    total = None
                index = event.get("index")
                if not isinstance(index, int):
                    index = None
                if isinstance(total, int) and total > 0:
                    chapter_total_hint = total
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
                job.update_progress(index, total_steps or total, description, event_label)
            except Exception:
                return

        try:
            job.set_status("running", "Chapterizing…")
            chapters, ruby_evidence = epub_to_chapter_texts(
                str(job.temp_path),
                nlp=backend,
                progress=_progress_callback,
            )
            base_total = len(chapters) if chapters else (chapter_total_hint or 0)
            total_steps = (base_total or 0) + 1  # writing step
            completed_steps = base_total
            job.update_progress(
                completed_steps,
                total_steps,
                f"{job.book_label} · writing chapters…",
                "writing",
            )
            job.set_status("running", "Writing chapters…")
            cover = get_epub_cover(str(job.temp_path))
            write_book_package(
                job.output_dir,
                chapters,
                source_epub=job.temp_path,
                cover_image=cover,
                ruby_evidence=ruby_evidence,
                apply_overrides=False,
            )
            completed_steps = base_total + 1
            job.update_progress(
                completed_steps,
                total_steps,
                f"{job.book_label} · writing chapters…",
                "writing",
            )

            overrides: list[OverrideRule] = []
            removals = []
            try:
                overrides, removals = load_refine_config(job.output_dir)
            except ValueError as exc:
                job.set_status("running", f"Overrides skipped: {exc}")
                overrides = []
                removals = []

            if overrides or removals:
                override_total = base_total or len(overrides)
                total_steps = base_total + 1 + override_total
                override_completed = 0

                def _override_label(event: Mapping[str, object]) -> str:
                    desc = f"{job.book_label} · applying overrides…"
                    idx_val = event.get("index")
                    total_val = event.get("total")
                    chapter_name = ""
                    path_val = event.get("path")
                    if isinstance(path_val, Path):
                        chapter_name = path_val.name
                    elif isinstance(path_val, str):
                        chapter_name = Path(path_val).name
                    prefix = ""
                    if isinstance(idx_val, int):
                        prefix = str(idx_val)
                        if isinstance(total_val, int) and total_val > 0:
                            prefix = f"{idx_val}/{total_val}"
                    suffix_parts = []
                    if prefix:
                        suffix_parts.append(prefix)
                    if chapter_name:
                        suffix_parts.append(chapter_name)
                    if suffix_parts:
                        desc = f"{desc} · {' '.join(suffix_parts)}"
                    return desc

                def _refine_progress_handler(event: Mapping[str, object]) -> None:
                    nonlocal override_total, total_steps, override_completed, completed_steps
                    event_type = event.get("event")
                    if event_type == "book_start":
                        total_chapters = event.get("total_chapters")
                        if isinstance(total_chapters, int) and total_chapters > 0:
                            override_total = total_chapters
                            total_steps = base_total + 1 + override_total
                            job.update_progress(
                                completed_steps,
                                total_steps,
                                f"{job.book_label} · applying overrides…",
                                "apply_overrides",
                            )
                    elif event_type == "chapter_start":
                        job.update_progress(
                            completed_steps,
                            total_steps,
                            _override_label(event),
                            "apply_overrides",
                        )
                    elif event_type == "chapter_done":
                        override_completed += 1
                        completed_steps = base_total + 1 + override_completed
                        job.update_progress(
                            completed_steps,
                            total_steps,
                            _override_label(event),
                            "apply_overrides",
                        )

                job.set_status("running", "Applying overrides…")
                try:
                    refine_book(
                        job.output_dir,
                        overrides if overrides else None,
                        removals=removals,
                        progress=_refine_progress_handler,
                    )
                except ValueError as exc:
                    job.set_status("running", f"Overrides skipped: {exc}")

            job.mark_success()
        except Exception as exc:
            job.set_error(f"{exc.__class__.__name__}: {exc}")
        finally:
            job.cleanup()


__all__ = ["UploadJob", "UploadManager"]
