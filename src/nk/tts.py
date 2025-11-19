from __future__ import annotations

import contextlib
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping
import threading
from urllib.parse import urlparse

import requests

from .book_io import (
    LoadedBookMetadata,
    PARTIAL_TEXT_SUFFIX,
    ensure_cover_is_square,
    load_book_metadata,
    load_token_metadata,
)
from .pitch import PitchToken
from .tokens import tokens_to_pitch_tokens

_DEBUG_LOG = False
_VOICEVOX_ENGINE_DEFAULT_KEYS = {
    "speed": "speedScale",
    "pitch": "pitchScale",
    "intonation": "intonationScale",
}


def set_debug_logging(enabled: bool) -> None:
    global _DEBUG_LOG
    _DEBUG_LOG = enabled


def _debug_log(message: str) -> None:
    if _DEBUG_LOG:
        print(f"[nk tts debug] {message}")


class VoiceVoxError(RuntimeError):
    """Raised when the VoiceVox engine returns an unexpected response."""


class VoiceVoxUnavailableError(ConnectionError):
    """Raised when the VoiceVox engine is unreachable."""


class VoiceVoxRuntimeError(RuntimeError):
    """Raised when the VoiceVox runtime cannot be started or becomes unhealthy."""


class FFmpegError(RuntimeError):
    """Raised when ffmpeg fails to render the requested MP3."""


@dataclass
class TTSTarget:
    source: Path
    output: Path
    book_title: str | None = None
    book_author: str | None = None
    chapter_title: str | None = None
    original_title: str | None = None
    track_number: int | None = None
    track_total: int | None = None
    cover_image: Path | None = None


@dataclass
class _ChunkSpan:
    text: str
    start: int
    end: int

def _read_original_title_from_file(chapter_path: Path) -> str | None:
    original_path = chapter_path.with_suffix(".original.txt")
    try:
        with original_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if stripped:
                    return stripped
    except FileNotFoundError:
        return None
    except OSError:
        return None
    return None


def _book_title_from_metadata(book_dir: Path, metadata: LoadedBookMetadata | None) -> str:
    if metadata and metadata.title:
        return metadata.title
    return book_dir.name


def _cover_path_for_book(book_dir: Path, metadata: LoadedBookMetadata | None) -> Path | None:
    if metadata and metadata.cover_path and metadata.cover_path.exists():
        ensure_cover_is_square(metadata.cover_path)
        return metadata.cover_path
    for ext in (".jpg", ".jpeg", ".png"):
        candidate = book_dir / f"cover{ext}"
        if candidate.exists():
            ensure_cover_is_square(candidate)
            return candidate
    return None


def _parse_track_number_from_name(stem: str) -> int | None:
    match = re.match(r"^(\d+)", stem)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _is_original_text_file(path: Path) -> bool:
    return path.name.endswith(".original.txt")


def _is_partial_text_file(path: Path) -> bool:
    return path.name.endswith(PARTIAL_TEXT_SUFFIX)


def _base_txt_name(path: Path) -> str:
    name = path.name
    if _is_partial_text_file(path):
        base = name[: -len(PARTIAL_TEXT_SUFFIX)]
        return f"{base}.txt"
    return name


def _canonical_chapter_path(path: Path) -> Path:
    if not _is_partial_text_file(path):
        return path
    base = _base_txt_name(path)
    candidate = path.with_name(base)
    return candidate if candidate.exists() else path


def _collect_text_variants(directory: Path) -> tuple[list[Path], list[Path]]:
    canonical: list[Path] = []
    partial: list[Path] = []
    for candidate in directory.iterdir():
        if not candidate.is_file() or candidate.suffix.lower() != ".txt":
            continue
        if _is_original_text_file(candidate):
            continue
        if _is_partial_text_file(candidate):
            partial.append(candidate)
        else:
            canonical.append(candidate)
    canonical.sort()
    partial.sort()
    return canonical, partial


def resolve_text_targets(
    input_path: Path,
    output_dir: Path | None = None,
    *,
    text_variant: str = "auto",
) -> list[TTSTarget]:
    """
    Determine which .txt files should be synthesized and their destination MP3 paths.
    """
    path = input_path
    if not path.exists():
        raise FileNotFoundError(f"Input path not found: {path}")
    variant = (text_variant or "auto").strip().lower()
    if variant not in {"auto", "full", "partial"}:
        raise ValueError("text_variant must be one of: auto, full, partial")

    targets: list[TTSTarget] = []
    if path.is_dir():
        canonical_files, partial_files = _collect_text_variants(path)
        if variant == "partial":
            text_files = partial_files
            if not text_files:
                raise FileNotFoundError(f"No {PARTIAL_TEXT_SUFFIX} files found in directory: {path}")
        elif variant == "auto":
            text_files = partial_files or canonical_files
        else:
            text_files = canonical_files
        if not text_files:
            raise FileNotFoundError(f"No .txt files found in directory: {path}")
        base_output = output_dir or path
        metadata = load_book_metadata(path)
        book_title = _book_title_from_metadata(path, metadata)
        book_author = metadata.author if metadata else None
        cover_path = _cover_path_for_book(path, metadata)
        track_total = len(text_files)
        for idx, txt in enumerate(text_files):
            base_name = _base_txt_name(txt)
            base_stem = Path(base_name).stem
            output = base_output / (base_stem + ".mp3")
            chapter_meta = metadata.chapters.get(base_name) if metadata else None
            canonical_txt = _canonical_chapter_path(txt)
            original_title = (
                chapter_meta.original_title if chapter_meta and chapter_meta.original_title else None
            )
            if not original_title:
                original_title = _read_original_title_from_file(canonical_txt)
            track_number = (
                chapter_meta.index
                if chapter_meta and chapter_meta.index is not None
                else _parse_track_number_from_name(txt.stem)
            )
            if track_number is None:
                track_number = idx + 1
            targets.append(
                TTSTarget(
                    source=txt,
                    output=output,
                    book_title=book_title,
                    book_author=book_author,
                    chapter_title=chapter_meta.title if chapter_meta else None,
                    original_title=original_title,
                    track_number=track_number,
                    track_total=track_total,
                    cover_image=cover_path,
                )
            )
    else:
        if path.suffix.lower() != ".txt" or _is_original_text_file(path):
            raise ValueError("TTS input must be a .txt file or a directory of .txt files.")
        actual_path = path
        if variant in {"partial", "auto"} and not _is_partial_text_file(path):
            candidate = path.with_name(f"{path.stem}{PARTIAL_TEXT_SUFFIX}")
            if candidate.exists():
                actual_path = candidate
            elif variant == "partial":
                raise FileNotFoundError(f"Partial text not found for {path.name}")
        base_output = output_dir or path.parent
        base_name = _base_txt_name(actual_path)
        base_stem = Path(base_name).stem
        output = base_output / (base_stem + ".mp3")
        book_dir = actual_path.parent
        metadata = load_book_metadata(book_dir) if book_dir.exists() else None
        book_title = _book_title_from_metadata(book_dir, metadata)
        book_author = metadata.author if metadata else None
        cover_path = _cover_path_for_book(book_dir, metadata)
        chapter_meta = metadata.chapters.get(base_name) if metadata else None
        original_title = (
            chapter_meta.original_title if chapter_meta and chapter_meta.original_title else None
        )
        if not original_title:
            canonical_txt = _canonical_chapter_path(actual_path)
            original_title = _read_original_title_from_file(canonical_txt)
        track_total = (
            len(metadata.chapters) if metadata and metadata.chapters else None
        )
        track_number = (
            chapter_meta.index
            if chapter_meta and chapter_meta.index is not None
            else _parse_track_number_from_name(base_stem)
        )
        if track_number is None:
            track_number = 1
        targets.append(
            TTSTarget(
                source=actual_path,
                output=output,
                book_title=book_title,
                book_author=book_author,
                chapter_title=chapter_meta.title if chapter_meta else None,
                original_title=original_title,
                track_number=track_number,
                track_total=track_total or 1,
                cover_image=cover_path,
            )
        )
    return targets


def _normalize_base_url(base_url: str) -> str:
    trimmed = base_url.strip()
    if not trimmed:
        raise ValueError("VoiceVox base URL cannot be empty.")
    if "://" not in trimmed:
        trimmed = f"http://{trimmed}"
    return trimmed.rstrip("/")


def _prepare_voicevox_endpoint(base_url: str) -> tuple[str, str, int]:
    normalized = _normalize_base_url(base_url)
    parsed = urlparse(normalized)
    if not parsed.hostname:
        raise ValueError(f"Invalid VoiceVox base URL: {base_url}")
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        raise ValueError(f"Unsupported VoiceVox URL scheme: {parsed.scheme}")
    host = parsed.hostname
    if parsed.port is None:
        port = 50021
        parsed = parsed._replace(netloc=f"{host}:{port}")
        normalized = parsed.geturl()
    else:
        port = parsed.port
    return normalized.rstrip("/"), host, port


def _voicevox_health_url(normalized_base_url: str) -> str:
    return f"{normalized_base_url}/version"


def _voicevox_is_ready(
    normalized_base_url: str,
    *,
    request_timeout: float = 1.0,
) -> bool:
    try:
        resp = requests.get(
            _voicevox_health_url(normalized_base_url),
            timeout=request_timeout,
        )
    except requests.RequestException:
        return False
    return resp.status_code == 200


def _resolve_runtime_executable(runtime_path: Path) -> Path:
    expanded = runtime_path.expanduser()
    if expanded.is_dir():
        for candidate in ("run", "run.exe"):
            executable = expanded / candidate
            if executable.is_file():
                return executable
        raise FileNotFoundError(
            f"No VoiceVox runtime executable found in directory: {expanded}"
        )
    if expanded.is_file():
        return expanded
    raise FileNotFoundError(f"VoiceVox runtime executable not found: {expanded}")


_LOCAL_HOSTS = {"127.0.0.1", "localhost", "0.0.0.0", "::1"}


_ACCENT_CACHE_SENTINEL = object()


class _VoiceVoxAccentCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[tuple[str, str], int | None] = {}

    def get(self, key: tuple[str, str]) -> object:
        with self._lock:
            return self._data.get(key, _ACCENT_CACHE_SENTINEL)

    def set(self, key: tuple[str, str], value: int | None) -> None:
        with self._lock:
            self._data[key] = value

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_VOICEVOX_ACCENT_CACHE = _VoiceVoxAccentCache()


def _allocate_local_port(host: str) -> int:
    """
    Reserve an ephemeral TCP port bound to the given host and return it.
    """
    errors: list[BaseException] = []
    candidates = [host]
    if host == "localhost":
        candidates.append("127.0.0.1")
    for candidate in candidates:
        try:
            addr_info = socket.getaddrinfo(
                candidate,
                0,
                family=socket.AF_UNSPEC,
                type=socket.SOCK_STREAM,
            )
        except OSError as exc:
            errors.append(exc)
            continue
        for family, socktype, proto, _, sockaddr in addr_info:
            try:
                with contextlib.closing(socket.socket(family, socktype, proto)) as sock:
                    sock.bind(sockaddr)
                    return sock.getsockname()[1]
            except OSError as exc:
                errors.append(exc)
                continue
    if errors:
        raise VoiceVoxRuntimeError(
            f"Unable to allocate a free port for VoiceVox on host '{host}': {errors[-1]}"
        ) from errors[-1]
    raise VoiceVoxRuntimeError(
        f"Unable to allocate a free port for VoiceVox on host '{host}'."
    )


def ensure_dedicated_voicevox_url(base_url: str) -> tuple[str, bool]:
    """
    Ensure the returned VoiceVox base URL is free so we can launch a dedicated runtime.
    Returns (url, changed_flag).
    """
    normalized_base_url, host, port = _prepare_voicevox_endpoint(base_url)
    if host not in _LOCAL_HOSTS:
        return normalized_base_url, False
    if not _voicevox_is_ready(normalized_base_url):
        return normalized_base_url, False
    new_port = _allocate_local_port(host)
    if new_port == port:
        return normalized_base_url, False
    parsed = urlparse(normalized_base_url)
    updated = parsed._replace(netloc=f"{host}:{new_port}")
    new_url = updated.geturl().rstrip("/")
    _debug_log(
        f"Existing VoiceVox runtime detected at {normalized_base_url}; "
        f"using dedicated runtime on port {new_port} ({new_url})"
    )
    return new_url, True


def discover_voicevox_runtime(
    base_url: str,
    *,
    env: dict[str, str] | None = None,
) -> Path | None:
    """
    Attempt to locate a VoiceVox runtime executable for the given endpoint.
    Returns None when no candidate is found or when the endpoint is non-local.
    """
    if env is None:
        env = os.environ  # pragma: no cover

    try:
        _, host, _ = _prepare_voicevox_endpoint(base_url)
    except ValueError:
        return None

    if host not in _LOCAL_HOSTS:
        return None

    def _candidates() -> Iterator[Path]:
        env_vars = [
            "NK_VOICEVOX_RUNTIME",
            "VOICEVOX_RUNTIME",
            "VOICEVOX_DIR",
            "VOICEVOX_PATH",
        ]
        for var in env_vars:
            value = env.get(var)
            if value:
                yield Path(value)

        home = Path.home()
        default_roots = [
            home / "opt" / "voicevox",
            home / "Applications" / "VOICEVOX" / "app",
        ]
        for root in default_roots:
            yield root
            yield root / "macos-x64"
            yield root / "linux-x64"
            yield root / "linux-cpu-x64"
            yield root / "linux-gpu-x64"
            yield root / "win-x64"
            yield root / "run"
        yield home / "opt" / "voicevox-engine"

    for candidate in _candidates():
        try:
            return _resolve_runtime_executable(candidate)
        except FileNotFoundError:
            continue
    return None


def _emit_progress(
    progress: Callable[[dict[str, object]], None] | None,
    event: str,
    **payload: object,
) -> None:
    if progress is None:
        return
    data = {"event": event}
    data.update(payload)
    progress(data)


def _effective_jobs(requested: int, total: int) -> int:
    if total <= 1:
        return 1
    jobs = requested
    if jobs <= 0:
        cpu = os.cpu_count() or 1
        jobs = max(1, cpu // 2)
        jobs = min(jobs, 4)
        if jobs <= 0:
            jobs = 1
    jobs = max(1, jobs)
    return min(jobs, total)


_CACHE_SANITIZE_RE = re.compile(r"[^0-9A-Za-z._-]+")

_MAX_CHARS_PER_CHUNK = 360
_SENTENCE_BREAKS = (
    "\n",
    "。", "！", "？", "!", "?", "…", "‼", "⁉", "⁈", "｡",
)
_CLAUSE_BREAKS = (
    "、", "，", "､", ",", ";", "；", ":", "：", "・", "—", "─",
)


def _slugify_cache_component(text: str) -> str:
    slug = _CACHE_SANITIZE_RE.sub("_", text)
    slug = slug.strip("._-")
    return slug[:64]


def _target_cache_dir(cache_base: Path | None, target: TTSTarget) -> Path:
    """
    Determine the cache directory for a TTS target, preferring any existing cache
    layout (legacy or current) so interrupted runs can resume seamlessly.
    """
    base_candidate = (
        cache_base if cache_base is not None else target.output.parent / ".nk-tts-cache"
    )
    base_path = Path(base_candidate)
    if not base_path.is_absolute():
        base_path = (Path.cwd() / base_path).resolve()

    stem_slug = _slugify_cache_component(target.output.stem)

    try:
        canonical_source = target.source.resolve(strict=False)
    except OSError:
        canonical_source = target.source.absolute()
    canonical_key = canonical_source.as_posix()

    def _compose_name(slug: str, fingerprint: str) -> str:
        return f"{slug}-{fingerprint}" if slug else fingerprint

    def _score(path: Path) -> tuple[int, int, int]:
        complete = (path / ".complete").is_file()
        progress = (path / ".progress").is_file()
        chunk_count = sum(1 for _ in path.glob("*.wav"))
        return (1 if complete else 0, 1 if progress else 0, chunk_count)

    candidate_keys: list[str] = [canonical_key]

    text_source_str = str(target.source)
    if text_source_str not in candidate_keys:
        candidate_keys.append(text_source_str)

    def _append_relative(base: Path) -> None:
        try:
            rel = canonical_source.relative_to(base)
        except ValueError:
            return
        relative_key = rel.as_posix()
        if relative_key and relative_key not in candidate_keys:
            candidate_keys.append(relative_key)

    _append_relative(Path.cwd())
    _append_relative(target.output.parent)
    if target.output.parent.parent != target.output.parent:
        _append_relative(target.output.parent.parent)

    candidate_dirs: list[Path] = []
    priority: dict[Path, int] = {}
    for index, key in enumerate(candidate_keys):
        fingerprint = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        candidate = base_path / _compose_name(stem_slug, fingerprint)
        if candidate in priority:
            continue
        candidate_dirs.append(candidate)
        priority[candidate] = -index

    existing = [path for path in candidate_dirs if path.exists()]
    if existing:
        return max(existing, key=lambda path: (_score(path), priority[path]))

    if stem_slug:
        prefixed_dirs = [
            path
            for path in base_path.glob(f"{stem_slug}-*")
            if path.is_dir()
        ]
        if prefixed_dirs:
            return max(prefixed_dirs, key=_score)

    if candidate_dirs:
        return candidate_dirs[0]

    fingerprint = hashlib.sha1(canonical_key.encode("utf-8")).hexdigest()[:10]
    return base_path / _compose_name(stem_slug, fingerprint)


def _chunk_cache_path(
    cache_dir: Path,
    index: int,
    chunk_text: str,
    pitch_signature: str | None = None,
) -> Path:
    hasher = hashlib.sha1()
    hasher.update(chunk_text.encode("utf-8"))
    if pitch_signature:
        hasher.update(b"||")
        hasher.update(pitch_signature.encode("utf-8"))
    digest = hasher.hexdigest()[:10]
    return cache_dir / f"{index:05d}_{digest}.wav"


def _ffmpeg_escape_path(path: Path) -> str:
    escaped = path.as_posix().replace("'", "'\\''")
    return f"'{escaped}'"


def _synthesize_target_with_client(
    target: TTSTarget,
    client: VoiceVoxClient,
    *,
    index: int,
    total: int,
    ffmpeg_path: str,
    overwrite: bool,
    progress: Callable[[dict[str, object]], None] | None,
    cache_base: Path | None,
    keep_cache: bool,
    cancel_event: threading.Event | None = None,
) -> Path | None:
    if cancel_event and cancel_event.is_set():
        raise KeyboardInterrupt

    cache_dir = _target_cache_dir(cache_base, target)
    marker_path = cache_dir / ".complete"

    if target.output.exists() and not overwrite:
        if cache_dir.exists() and not marker_path.exists():
            target.output.unlink(missing_ok=True)
        else:
            _emit_progress(
                progress,
                "target_skipped",
                index=index,
                total=total,
                source=target.source,
                output=target.output,
                reason="exists",
            )
            if cache_dir.exists() and not keep_cache:
                shutil.rmtree(cache_dir, ignore_errors=True)
            return target.output

    text = target.source.read_text(encoding="utf-8").strip()
    if not text:
        _emit_progress(
            progress,
            "target_skipped",
            index=index,
            total=total,
            source=target.source,
            reason="empty",
        )
        return None

    text_hash = hashlib.sha1(text.encode("utf-8")).hexdigest()
    token_metadata = load_token_metadata(target.source)
    if token_metadata and token_metadata.text_sha1 and token_metadata.text_sha1 != text_hash:
        _debug_log(
            f"Token metadata SHA mismatch (expected {token_metadata.text_sha1}, got {text_hash}); ignoring overrides"
        )
        token_metadata = None
    chapter_tokens = token_metadata.tokens if token_metadata else []
    pitch_tokens = tokens_to_pitch_tokens(chapter_tokens) if chapter_tokens else []
    if pitch_tokens:
        _enrich_pitch_tokens_with_voicevox(pitch_tokens, client)

    raw_chunk_entries = _split_text_on_breaks_with_spans(text)
    chunk_entries: list[tuple[_ChunkSpan, str]] = []
    for entry in raw_chunk_entries:
        chunk_text = entry.text
        if not chunk_text:
            continue
        chunk_entries.append((entry, chunk_text))
    if not chunk_entries:
        _emit_progress(
            progress,
            "target_skipped",
            index=index,
            total=total,
            source=target.source,
            reason="no_chunks",
        )
        return None

    chunk_count = len(chunk_entries)
    _emit_progress(
        progress,
        "target_start",
        index=index,
        total=total,
        source=target.source,
        output=target.output,
        chunk_count=chunk_count,
    )

    cache_dir.mkdir(parents=True, exist_ok=True)
    marker_path.unlink(missing_ok=True)
    chunk_files: list[Path] = []

    for chunk_index, (chunk_entry, chunk_text) in enumerate(chunk_entries, start=1):
        if cancel_event and cancel_event.is_set():
            raise KeyboardInterrupt
        _emit_progress(
            progress,
            "chunk_start",
            index=index,
            total=total,
            chunk_index=chunk_index,
            chunk_count=chunk_count,
            source=target.source,
        )
        local_pitch_tokens = _slice_pitch_tokens_for_chunk(
            pitch_tokens,
            chunk_entry.start,
            chunk_entry.end,
        )
        pitch_signature = _pitch_signature(local_pitch_tokens)
        if pitch_signature:
            _debug_log(f"Chunk {chunk_index}: pitch signature {pitch_signature}")
        chunk_path = _chunk_cache_path(cache_dir, chunk_index, chunk_text, pitch_signature)
        if not chunk_path.exists():

            def _modifier(
                payload: dict[str, object],
                tokens=local_pitch_tokens,
                voice_client=client,
                chunk_idx=chunk_index,
            ) -> None:
                if not tokens:
                    return
                changed = _apply_pitch_overrides(payload, tokens)
                if changed and hasattr(voice_client, "recalculate_mora_pitch"):
                    _debug_log(
                        f"Chunk {chunk_idx}: overrides applied (tokens={len(tokens)}); recalculating mora pitch"
                    )
                    try:
                        updated_phrases = voice_client.recalculate_mora_pitch(
                            payload.get("accent_phrases") or []
                        )
                    except Exception:
                        _debug_log(f"Chunk {chunk_idx}: mora pitch recalculation failed")
                        return
                    if isinstance(updated_phrases, list):
                        payload["accent_phrases"] = updated_phrases

            modify_query = _modifier if local_pitch_tokens else None
            wav_bytes = client.synthesize_wav(chunk_text, modify_query=modify_query)
            chunk_path.write_bytes(wav_bytes)

        chunk_files.append(chunk_path)

    book_title = target.book_title or target.output.parent.name or "nk"
    track_total = target.track_total or total or None
    track_number_int = target.track_number
    if track_number_int is None:
        track_number_int = _parse_track_number_from_name(target.source.stem)
    if track_number_int is None:
        track_number_int = index
    chapter_label = target.original_title or target.chapter_title
    if not chapter_label:
        chapter_label = target.source.stem.replace("_", " ").strip()
    track_title = chapter_label or book_title
    artist_name = target.book_author or book_title
    album_title = book_title
    metadata: dict[str, str] = {
        "title": track_title,
        "artist": artist_name,
        "album": album_title,
        "album_artist": artist_name,
    }
    if track_number_int is not None:
        metadata["track"] = str(track_number_int)
    if track_total:
        metadata["tracktotal"] = str(track_total)

    if cancel_event and cancel_event.is_set():
        raise KeyboardInterrupt

    _merge_wavs_to_mp3(
        chunk_files,
        target.output,
        ffmpeg_path=ffmpeg_path,
        overwrite=overwrite,
        metadata=metadata,
        cover_path=target.cover_image,
    )
    if cache_dir.exists():
        if keep_cache:
            marker_path.write_text(str(chunk_count), encoding="utf-8")
        else:
            shutil.rmtree(cache_dir, ignore_errors=True)

    _emit_progress(
        progress,
        "target_done",
        index=index,
        total=total,
        source=target.source,
        output=target.output,
        chunk_count=chunk_count,
    )
    return target.output


def _wait_for_voicevox_ready(
    process: subprocess.Popen[bytes],
    normalized_base_url: str,
    readiness_timeout: float,
    poll_interval: float,
) -> None:
    deadline = time.monotonic() + max(readiness_timeout, 0.0)
    interval = max(poll_interval, 0.1)
    while True:
        if _voicevox_is_ready(normalized_base_url):
            return
        if process.poll() is not None:
            raise VoiceVoxRuntimeError(
                "VoiceVox runtime exited before becoming ready."
            )
        if time.monotonic() >= deadline:
            break
        time.sleep(interval)
    raise VoiceVoxRuntimeError(
        f"Timed out waiting for VoiceVox runtime to become ready at {normalized_base_url}"
    )


@contextlib.contextmanager
def managed_voicevox_runtime(
    runtime_path: Path | str | None,
    base_url: str,
    *,
    readiness_timeout: float = 30.0,
    poll_interval: float = 0.5,
    extra_env: Mapping[str, str] | None = None,
    cpu_threads: int | None = None,
) -> Iterator[subprocess.Popen[bytes] | None]:
    """
    Context manager that launches a VoiceVox runtime if requested and stops it on exit.
    """
    if not runtime_path:
        yield None
        return

    normalized_base_url, host, port = _prepare_voicevox_endpoint(base_url)
    runtime_executable = _resolve_runtime_executable(Path(runtime_path))

    if _voicevox_is_ready(normalized_base_url):
        yield None
        return

    cmd = [str(runtime_executable), "--host", host, "--port", str(port)]
    if cpu_threads and cpu_threads > 0:
        cmd.extend(["--cpu_num_threads", str(cpu_threads)])
    proc_env = os.environ.copy()
    if extra_env:
        for key, value in extra_env.items():
            if value is None:
                continue
            proc_env[str(key)] = str(value)
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            cmd,
            cwd=str(runtime_executable.parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=proc_env,
        )
    except OSError as exc:
        raise VoiceVoxRuntimeError(
            f"Failed to launch VoiceVox runtime: {exc}"
        ) from exc

    try:
        _wait_for_voicevox_ready(
            process,
            normalized_base_url,
            readiness_timeout,
            poll_interval,
        )
        yield process
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()


def _extract_voicevox_engine_defaults(payload: Mapping[str, object]) -> dict[str, float]:
    defaults: dict[str, float] = {}
    for logical_key, payload_key in _VOICEVOX_ENGINE_DEFAULT_KEYS.items():
        value = payload.get(payload_key)
        if isinstance(value, (int, float)):
            defaults[logical_key] = float(value)
    return defaults


class VoiceVoxClient:
    """
    Thin wrapper around the VoiceVox HTTP API.
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:50021",
        speaker_id: int = 2,
        timeout: float = 30.0,
        *,
        post_phoneme_length: float | None = None,
        speed_scale: float | None = None,
        pitch_scale: float | None = None,
        intonation_scale: float | None = None,
        engine_defaults_callback: Callable[[dict[str, float]], None] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.speaker_id = speaker_id
        self.timeout = timeout
        self.post_phoneme_length = post_phoneme_length
        self.speed_scale = speed_scale
        self.pitch_scale = pitch_scale
        self.intonation_scale = intonation_scale
        self._session = requests.Session()
        self._engine_defaults_callback = engine_defaults_callback
        self._engine_defaults_reported = False
        self._last_engine_defaults: dict[str, float] | None = None

    def build_audio_query(self, text: str) -> dict:
        try:
            query_resp = self._session.post(
                f"{self.base_url}/audio_query",
                params={"text": text, "speaker": self.speaker_id},
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise VoiceVoxUnavailableError(
                f"Failed to contact VoiceVox engine at {self.base_url}"
            ) from exc

        if query_resp.status_code != 200:
            raise VoiceVoxError(
                f"/audio_query failed with status {query_resp.status_code}: {query_resp.text}"
            )

        try:
            query_payload = query_resp.json()
        except json.JSONDecodeError as exc:
            raise VoiceVoxError("VoiceVox returned invalid JSON for /audio_query") from exc

        engine_defaults = _extract_voicevox_engine_defaults(query_payload)
        if engine_defaults:
            self._last_engine_defaults = engine_defaults
            if self._engine_defaults_callback and not self._engine_defaults_reported:
                try:
                    self._engine_defaults_callback(engine_defaults.copy())
                    self._engine_defaults_reported = True
                except Exception as exc:  # pragma: no cover - defensive
                    _debug_log(f"engine_defaults_callback failed: {exc}")

        if self.post_phoneme_length is not None and self.post_phoneme_length >= 0:
            payload_value = float(query_payload.get("postPhonemeLength", 0.0))
            query_payload["postPhonemeLength"] = max(
                payload_value,
                float(self.post_phoneme_length),
            )
        if self.speed_scale is not None:
            query_payload["speedScale"] = float(self.speed_scale)
        if self.pitch_scale is not None:
            query_payload["pitchScale"] = float(self.pitch_scale)
        if self.intonation_scale is not None:
            query_payload["intonationScale"] = float(self.intonation_scale)
        return query_payload

    def last_engine_defaults(self) -> dict[str, float] | None:
        if self._last_engine_defaults is None:
            return None
        return self._last_engine_defaults.copy()

    def synthesize_from_query(self, query_payload: dict) -> bytes:
        try:
            synth_resp = self._session.post(
                f"{self.base_url}/synthesis",
                params={"speaker": self.speaker_id},
                json=query_payload,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise VoiceVoxUnavailableError(
                f"Failed to contact VoiceVox engine during synthesis at {self.base_url}"
            ) from exc

        if synth_resp.status_code != 200:
            raise VoiceVoxError(
                f"/synthesis failed with status {synth_resp.status_code}: {synth_resp.text}"
            )

        return synth_resp.content

    def synthesize_wav(
        self,
        text: str,
        *,
        modify_query: Callable[[dict[str, object]], None] | None = None,
    ) -> bytes:
        """
        Generate WAV audio bytes for the provided text via VoiceVox.
        """
        query_payload = self.build_audio_query(text)
        if modify_query is not None:
            modify_query(query_payload)
        return self.synthesize_from_query(query_payload)

    def recalculate_mora_pitch(self, accent_phrases: list[dict[str, object]]) -> list[dict[str, object]]:
        """
        Ask VoiceVox to recompute mora pitch values for the provided accent phrases.
        """
        try:
            resp = self._session.post(
                f"{self.base_url}/mora_pitch",
                params={"speaker": self.speaker_id},
                json=accent_phrases,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise VoiceVoxUnavailableError(
                f"Failed to recalculate mora pitch at {self.base_url}"
            ) from exc
        if resp.status_code != 200:
            raise VoiceVoxError(
                f"/mora_pitch failed with status {resp.status_code}: {resp.text}"
            )
        try:
            updated = resp.json()
        except json.JSONDecodeError as exc:
            raise VoiceVoxError("VoiceVox returned invalid JSON for /mora_pitch") from exc
        if isinstance(updated, list):
            return updated
        return accent_phrases

    def close(self) -> None:
        self._session.close()


def wav_bytes_to_mp3(
    wav_bytes: bytes,
    output_path: Path,
    *,
    ffmpeg_path: str = "ffmpeg",
    overwrite: bool = False,
) -> None:
    """
    Convert WAV bytes to an MP3 file using ffmpeg.
    """
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {output_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        tmp.write(wav_bytes)
        tmp_path = Path(tmp.name)
    try:
        cmd = [
            ffmpeg_path,
            "-y",
            "-i",
            str(tmp_path),
            "-codec:a",
            "libmp3lame",
            "-qscale:a",
            "2",
            str(output_path),
        ]
        try:
            subprocess.run(
                cmd,
                check=True,
                capture_output=True,
            )
        except FileNotFoundError as exc:
            raise FFmpegError(
                f"ffmpeg executable not found: {ffmpeg_path}"
            ) from exc
        except subprocess.CalledProcessError as exc:
            stderr = exc.stderr.decode("utf-8", errors="ignore") if exc.stderr else ""
            raise FFmpegError(f"ffmpeg failed: {stderr.strip()}") from exc
    finally:
        tmp_path.unlink(missing_ok=True)


def _split_text_on_breaks(text: str) -> list[str]:
    """
    Split text into chunks using blank-line separated blocks.
    Empty lines are treated as delimiters; consecutive blanks collapse.
    """
    return [chunk.text for chunk in _split_text_on_breaks_with_spans(text)]


def _split_text_on_breaks_with_spans(text: str) -> list[_ChunkSpan]:
    chunks: list[_ChunkSpan] = []
    current: list[tuple[str, int, int]] = []

    def flush() -> None:
        if not current:
            return
        first_line, first_start, _ = current[0]
        last_line, _, last_end = current[-1]
        start = first_start + _leading_trim_index(first_line)
        end = last_end - _trailing_trim_count(last_line)
        current[:] = []
        if start >= end:
            return
        chunk_text = text[start:end]
        sub_chunks = _split_chunk_with_spans(chunk_text, start)
        chunks.extend(sub_chunks)

    for line, start, end in _iter_lines_with_positions(text):
        if line.strip():
            current.append((line, start, end))
        else:
            flush()
    flush()
    return chunks


def _split_chunk_if_needed(chunk: str) -> list[str]:
    if not chunk:
        return []
    if len(chunk) <= _MAX_CHARS_PER_CHUNK:
        return [chunk]
    segments: list[str] = []
    remaining = chunk
    while len(remaining) > _MAX_CHARS_PER_CHUNK:
        cut = _preferred_chunk_cut_index(remaining, _MAX_CHARS_PER_CHUNK)
        head = remaining[:cut].rstrip()
        if head:
            segments.append(head)
        remaining = remaining[cut:].lstrip()
        if not remaining:
            break
    if remaining:
        tail = remaining.strip()
        if tail:
            segments.append(tail)
    return segments


def _split_chunk_with_spans(chunk_text: str, base_start: int) -> list[_ChunkSpan]:
    segments = _split_chunk_if_needed(chunk_text)
    if not segments:
        return []
    spans: list[_ChunkSpan] = []
    cursor = 0
    for segment in segments:
        if not segment:
            continue
        idx = chunk_text.find(segment, cursor)
        if idx == -1:
            idx = chunk_text.find(segment)
            if idx == -1:
                continue
        start = base_start + idx
        end = start + len(segment)
        spans.append(_ChunkSpan(text=segment, start=start, end=end))
        cursor = idx + len(segment)
        while cursor < len(chunk_text) and chunk_text[cursor].isspace():
            cursor += 1
    return spans


def _slice_pitch_tokens_for_chunk(
    tokens: list[PitchToken],
    chunk_start: int,
    chunk_end: int,
) -> list[PitchToken]:
    if not tokens or chunk_start >= chunk_end:
        return []
    chunk_tokens: list[PitchToken] = []
    for token in tokens:
        if token.end <= chunk_start:
            continue
        if token.start >= chunk_end:
            break
        local_start = max(token.start, chunk_start) - chunk_start
        local_end = min(token.end, chunk_end) - chunk_start
        if local_end <= local_start:
            continue
        chunk_tokens.append(token.with_offsets(local_start, local_end))
    return chunk_tokens


def _apply_pitch_overrides(query_payload: dict[str, object], chunk_tokens: list[PitchToken]) -> bool:
    if not chunk_tokens:
        return False
    phrases = query_payload.get("accent_phrases")
    if not isinstance(phrases, list):
        return False
    token_idx = 0
    token_count = len(chunk_tokens)
    cursor = 0
    changed = False
    for phrase in phrases:
        moras = phrase.get("moras")
        if not isinstance(moras, list) or not moras:
            continue
        phrase_text = "".join(str(mora.get("text") or "") for mora in moras)
        phrase_len = len(phrase_text)
        if phrase_len == 0:
            continue
        phrase_start = cursor
        phrase_end = phrase_start + phrase_len
        cursor = phrase_end
        pause = phrase.get("pause_mora")
        if isinstance(pause, dict):
            pause_text = pause.get("text")
            if isinstance(pause_text, str):
                cursor += len(pause_text)
        while token_idx < token_count and chunk_tokens[token_idx].end <= phrase_start:
            token_idx += 1
        relevant: list[PitchToken] = []
        idx = token_idx
        while idx < token_count:
            token = chunk_tokens[idx]
            if token.start >= phrase_end:
                break
            if token.end > phrase_start:
                relevant.append(token)
            idx += 1
        if not relevant:
            continue
        accent_type = _select_accent_type(relevant)
        if accent_type is None:
            continue
        accent_index = _accent_index_from_type(accent_type, len(moras))
        if accent_index is None:
            continue
        if phrase.get("accent") != accent_index:
            phrase["accent"] = accent_index
            changed = True
    return changed


_CONTENT_POS_PREFIXES = (
    "名詞",
    "動詞",
    "形容詞",
    "副詞",
    "連体詞",
    "感動詞",
    "接頭辞",
    "接頭詞",
)


def _select_accent_type(tokens: list[PitchToken]) -> int | None:
    for token in tokens:
        if token.accent_type is None:
            continue
        if _is_content_pos(token.pos):
            return token.accent_type
    for token in tokens:
        if token.accent_type is not None:
            return token.accent_type
    return None


def _accent_index_from_type(accent_type: int | None, mora_count: int) -> int | None:
    if accent_type is None or mora_count <= 0:
        return None
    if accent_type <= 0:
        return mora_count
    return max(1, min(accent_type, mora_count))


def _is_content_pos(pos: str | None) -> bool:
    if not pos:
        return False
    return any(pos.startswith(prefix) for prefix in _CONTENT_POS_PREFIXES)


def _pitch_signature(tokens: list[PitchToken]) -> str | None:
    if not tokens:
        return None
    parts = []
    for token in tokens:
        accent = token.accent_type if token.accent_type is not None else "-"
        reading = token.reading
        parts.append(f"{token.start}:{token.end}:{reading}:{accent}")
    return "|".join(parts)


_KANA_IGNORE_CHARS = {"'", "’", "`", "´", "/", "／", "、", "，", ",", ".", "．", "・"}


def _normalize_kana(text: str | None) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text.strip())
    result: list[str] = []
    for char in normalized:
        if char in _KANA_IGNORE_CHARS or char.isspace():
            continue
        code = ord(char)
        if 0x3041 <= code <= 0x3096:
            char = chr(code + 0x60)
        elif 0x30A1 <= code <= 0x30FF:
            pass
        elif char == "ー":
            pass
        else:
            continue
        result.append(char)
    return "".join(result)


def _contains_kanji(text: str | None) -> bool:
    if not text:
        return False
    for char in text:
        code = ord(char)
        if (
            0x4E00 <= code <= 0x9FFF
            or 0x3400 <= code <= 0x4DBF
            or 0xF900 <= code <= 0xFAFF
            or 0x20000 <= code <= 0x2A6DF
            or 0x2A700 <= code <= 0x2B73F
            or 0x2B740 <= code <= 0x2B81F
            or 0x2B820 <= code <= 0x2CEAF
        ):
            return True
    return False


def _voicevox_accent_type_from_phrases(accent_phrases: object) -> int | None:
    if not isinstance(accent_phrases, list) or not accent_phrases:
        return None
    if len(accent_phrases) != 1:
        return None
    phrase = accent_phrases[0]
    if not isinstance(phrase, dict):
        return None
    moras = phrase.get("moras")
    if not isinstance(moras, list) or not moras:
        return None
    accent_val = phrase.get("accent")
    if not isinstance(accent_val, int):
        return None
    mora_count = len(moras)
    if mora_count <= 0:
        return None
    if accent_val <= 0 or accent_val >= mora_count:
        return 0
    return accent_val


def _fetch_voicevox_accent_from_surface(
    surface: str,
    reading: str,
    normalized_reading: str,
    client: VoiceVoxClient,
) -> int | None:
    try:
        query_payload = client.build_audio_query(surface)
    except (VoiceVoxUnavailableError, VoiceVoxError):
        return None
    kana = query_payload.get("kana")
    if not isinstance(kana, str):
        return None
    normalized_kana = _normalize_kana(kana)
    if not normalized_kana or normalized_kana != normalized_reading:
        return None
    accent_type = _voicevox_accent_type_from_phrases(query_payload.get("accent_phrases"))
    return accent_type


def _lookup_voicevox_accent(
    surface: str,
    reading: str,
    normalized_reading: str,
    client: VoiceVoxClient,
) -> int | None:
    cache_key = (surface, normalized_reading)
    cached = _VOICEVOX_ACCENT_CACHE.get(cache_key)
    if cached is not _ACCENT_CACHE_SENTINEL:
        return cached  # type: ignore[return-value]
    accent_type = _fetch_voicevox_accent_from_surface(
        surface,
        reading,
        normalized_reading,
        client,
    )
    _VOICEVOX_ACCENT_CACHE.set(cache_key, accent_type)
    return accent_type


def _enrich_pitch_tokens_with_voicevox(
    tokens: list[PitchToken],
    client: VoiceVoxClient,
) -> None:
    if not tokens:
        return
    if not hasattr(client, "build_audio_query"):
        return
    grouped: dict[tuple[str, str], list[PitchToken]] = defaultdict(list)
    readings: dict[tuple[str, str], str] = {}
    for token in tokens:
        if not token.surface or not token.reading:
            continue
        if not token.sources or "unidic" not in token.sources:
            continue
        if not _contains_kanji(token.surface):
            continue
        normalized_reading = _normalize_kana(token.reading)
        if not normalized_reading:
            continue
        key = (token.surface, normalized_reading)
        grouped[key].append(token)
        readings.setdefault(key, token.reading)

    for key, token_group in grouped.items():
        surface, normalized_reading = key
        reading = readings[key]
        accent_type = _lookup_voicevox_accent(surface, reading, normalized_reading, client)
        if accent_type is None:
            continue
        for token in token_group:
            if token.accent_type == accent_type:
                continue
            _debug_log(
                f"VoiceVox accent override for '{surface}': {token.accent_type} -> {accent_type}"
            )
            token.accent_type = accent_type


def _reset_voicevox_accent_cache_for_tests() -> None:
    _VOICEVOX_ACCENT_CACHE.clear()


def _iter_lines_with_positions(text: str) -> list[tuple[str, int, int]]:
    lines: list[tuple[str, int, int]] = []
    cursor = 0
    for raw in text.splitlines(keepends=True):
        line = raw.rstrip("\r\n")
        line_start = cursor
        line_end = line_start + len(line)
        lines.append((line, line_start, line_end))
        cursor += len(raw)
    if not text.endswith(("\n", "\r")) and text:
        # splitlines with keepends already adds final line without newline,
        # so this branch is only reached when the input is empty.
        pass
    return lines


def _leading_trim_index(line: str) -> int:
    idx = 0
    while idx < len(line) and line[idx].isspace():
        idx += 1
    return idx


def _trailing_trim_count(line: str) -> int:
    idx = len(line)
    while idx > 0 and line[idx - 1].isspace():
        idx -= 1
    return len(line) - idx


def _preferred_chunk_cut_index(text: str, limit: int) -> int:
    def _best_index(separators: tuple[str, ...]) -> tuple[int, int] | None:
        best: tuple[int, int] | None = None
        for sep in separators:
            idx = text.rfind(sep, 0, limit)
            if idx > 0:
                end = idx + len(sep)
                if best is None or end > best[0] + best[1]:
                    best = (idx, len(sep))
        return best

    for candidates in (_SENTENCE_BREAKS, _CLAUSE_BREAKS):
        match = _best_index(candidates)
        if match is not None:
            return match[0] + match[1]
    return max(1, limit)


def _merge_wavs_to_mp3(
    wav_paths: list[Path],
    output_path: Path,
    *,
    ffmpeg_path: str,
    overwrite: bool,
    metadata: dict[str, str] | None = None,
    cover_path: Path | None = None,
) -> None:
    """
    Merge multiple WAV files into a single MP3 using the ffmpeg concat demuxer.
    """
    if not wav_paths:
        raise ValueError("No WAV files provided for merge.")
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as list_file:
        for wav_path in wav_paths:
            absolute = wav_path.resolve()
            list_file.write(f"file {_ffmpeg_escape_path(absolute)}\n")
        concat_list = Path(list_file.name)
    try:
        cover_input = cover_path if cover_path and cover_path.exists() else None
        cmd = [
            ffmpeg_path,
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_list),
        ]
        if cover_input is not None:
            cmd.extend(["-i", str(cover_input)])
        cmd.extend(["-map", "0:a:0"])
        if cover_input is not None:
            cmd.extend(
                [
                    "-map",
                    "1:v:0",
                    "-c:v",
                    "copy",
                    "-disposition:v",
                    "attached_pic",
                    "-metadata:s:v",
                    "title=Cover",
                    "-metadata:s:v",
                    "comment=Cover (front)",
                ]
            )
        cmd.extend(
            [
                "-codec:a",
                "libmp3lame",
                "-qscale:a",
                "2",
            ]
        )
        if metadata:
            for key, value in metadata.items():
                if not value:
                    continue
                cmd.extend(["-metadata", f"{key}={value}"])
        if cover_input is not None:
            cmd.extend(["-id3v2_version", "3"])
        cmd.append(str(output_path))
        try:
            subprocess.run(
                cmd,
                check=True,
                capture_output=True,
            )
        except FileNotFoundError as exc:
            raise FFmpegError(
                f"ffmpeg executable not found: {ffmpeg_path}"
            ) from exc
        except subprocess.CalledProcessError as exc:
            stderr = exc.stderr.decode("utf-8", errors="ignore") if exc.stderr else ""
            raise FFmpegError(f"ffmpeg failed: {stderr.strip()}") from exc
    finally:
        concat_list.unlink(missing_ok=True)


def synthesize_texts_to_mp3(
    targets: Iterable[TTSTarget],
    *,
    speaker_id: int = 2,
    base_url: str = "http://127.0.0.1:50021",
    ffmpeg_path: str = "ffmpeg",
    overwrite: bool = False,
    timeout: float = 30.0,
    post_phoneme_length: float | None = None,
    speed_scale: float | None = None,
    pitch_scale: float | None = None,
    intonation_scale: float | None = None,
    jobs: int = 1,
    cache_dir: Path | None = None,
    keep_cache: bool = False,
    progress: Callable[[dict[str, object]], None] | None = None,
    cancel_event: threading.Event | None = None,
    engine_defaults_callback: Callable[[dict[str, float]], None] | None = None,
) -> list[Path]:
    """
    Synthesize each target text file into an MP3 and return the generated paths.
    """
    target_list = list(targets)
    total_targets = len(target_list)
    if not target_list:
        return []

    effective_jobs = _effective_jobs(jobs, total_targets)
    cache_base = Path(cache_dir).expanduser() if cache_dir is not None else None
    generated: list[Path | None]

    if effective_jobs == 1:
        client = VoiceVoxClient(
            base_url=base_url,
            speaker_id=speaker_id,
            timeout=timeout,
            post_phoneme_length=post_phoneme_length,
            speed_scale=speed_scale,
            pitch_scale=pitch_scale,
            intonation_scale=intonation_scale,
            engine_defaults_callback=engine_defaults_callback,
        )
        try:
            results: list[Path] = []
            for index, target in enumerate(target_list, start=1):
                if cancel_event and cancel_event.is_set():
                    break
                try:
                    produced = _synthesize_target_with_client(
                        target,
                        client,
                        index=index,
                        total=total_targets,
                        ffmpeg_path=ffmpeg_path,
                        overwrite=overwrite,
                        progress=progress,
                        cache_base=cache_base,
                        keep_cache=keep_cache,
                        cancel_event=cancel_event,
                    )
                except KeyboardInterrupt:
                    if cancel_event:
                        cancel_event.set()
                    raise
                if produced is not None:
                    results.append(produced)
        finally:
            client.close()
        return results

    generated = [None] * total_targets

    def _worker(payload: tuple[int, TTSTarget]) -> tuple[int, Path | None]:
        idx, target = payload
        if cancel_event and cancel_event.is_set():
            return idx, None
        client = VoiceVoxClient(
            base_url=base_url,
            speaker_id=speaker_id,
            timeout=timeout,
            post_phoneme_length=post_phoneme_length,
            speed_scale=speed_scale,
            pitch_scale=pitch_scale,
            intonation_scale=intonation_scale,
            engine_defaults_callback=engine_defaults_callback,
        )
        try:
            if cancel_event and cancel_event.is_set():
                return idx, None
            try:
                produced = _synthesize_target_with_client(
                    target,
                    client,
                    index=idx + 1,
                    total=total_targets,
                    ffmpeg_path=ffmpeg_path,
                    overwrite=overwrite,
                    progress=progress,
                    cache_base=cache_base,
                    keep_cache=keep_cache,
                    cancel_event=cancel_event,
                )
            except KeyboardInterrupt:
                if cancel_event:
                    cancel_event.set()
                raise
            return idx, produced
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=effective_jobs) as executor:
        futures = [executor.submit(_worker, (idx, target)) for idx, target in enumerate(target_list)]
        for future in futures:
            if cancel_event and cancel_event.is_set():
                break
            try:
                order, produced = future.result()
            except KeyboardInterrupt:
                if cancel_event:
                    cancel_event.set()
                for f in futures:
                    f.cancel()
                raise
            if produced is not None:
                generated[order] = produced
        if cancel_event and cancel_event.is_set():
            for f in futures:
                if not f.done():
                    f.cancel()

    return [path for path in generated if path is not None]
