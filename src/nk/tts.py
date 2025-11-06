from __future__ import annotations

import contextlib
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator
import threading
from urllib.parse import urlparse

import requests

try:
    import simpleaudio as _simpleaudio
except ImportError:  # pragma: no cover - optional dependency
    _simpleaudio = None


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


def resolve_text_targets(
    input_path: Path,
    output_dir: Path | None = None,
) -> list[TTSTarget]:
    """
    Determine which .txt files should be synthesized and their destination MP3 paths.
    """
    path = input_path
    if not path.exists():
        raise FileNotFoundError(f"Input path not found: {path}")

    targets: list[TTSTarget] = []
    if path.is_dir():
        text_files = sorted(
            p for p in path.iterdir() if p.is_file() and p.suffix.lower() == ".txt"
        )
        if not text_files:
            raise FileNotFoundError(f"No .txt files found in directory: {path}")
        base_output = output_dir or path
        for txt in text_files:
            output = base_output / (txt.stem + ".mp3")
            targets.append(TTSTarget(source=txt, output=output))
    else:
        if path.suffix.lower() != ".txt":
            raise ValueError("TTS input must be a .txt file or a directory of .txt files.")
        base_output = output_dir or path.parent
        output = base_output / (path.stem + ".mp3")
        targets.append(TTSTarget(source=path, output=output))
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


def _slugify_cache_component(text: str) -> str:
    slug = _CACHE_SANITIZE_RE.sub("_", text)
    slug = slug.strip("._-")
    return slug[:64]


def _target_cache_dir(cache_base: Path | None, target: TTSTarget) -> Path:
    base_candidate = (
        cache_base if cache_base is not None else target.output.parent / ".nk-tts-cache"
    )
    base_path = Path(base_candidate)
    if not base_path.is_absolute():
        base_path = (Path.cwd() / base_path).resolve()
    stem_slug = _slugify_cache_component(target.output.stem)
    source_fingerprint = hashlib.sha1(str(target.source).encode("utf-8")).hexdigest()[:10]
    name = f"{stem_slug}-{source_fingerprint}" if stem_slug else source_fingerprint
    return base_path / name


def _chunk_cache_path(cache_dir: Path, index: int, chunk_text: str) -> Path:
    digest = hashlib.sha1(chunk_text.encode("utf-8")).hexdigest()[:10]
    return cache_dir / f"{index:05d}_{digest}.wav"


def _ffmpeg_escape_path(path: Path) -> str:
    escaped = path.as_posix().replace("'", "'\\''")
    return f"'{escaped}'"


def _play_chunk_simpleaudio(chunk_path: Path) -> None:
    if _simpleaudio is None:
        raise RuntimeError(
            "simpleaudio is required for live playback. Install with `pip install simpleaudio`."
        )
    wave_obj = _simpleaudio.WaveObject.from_wave_file(str(chunk_path))
    return wave_obj.play()


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
    live_playback: bool = False,
    playback_callback: Callable[[Path], None] | None = None,
    live_prebuffer: int = 2,
) -> Path | None:
    if cancel_event and cancel_event.is_set():
        raise KeyboardInterrupt

    cache_dir = _target_cache_dir(cache_base, target)
    marker_path = cache_dir / ".complete"
    progress_path = cache_dir / ".progress"

    if target.output.exists() and not overwrite and not live_playback:
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
                live=live_playback,
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
            live=live_playback,
        )
        return None

    chunks = _split_text_on_breaks(text)
    if not chunks:
        _emit_progress(
            progress,
            "target_skipped",
            index=index,
            total=total,
            source=target.source,
            reason="no_chunks",
            live=live_playback,
        )
        return None

    chunk_count = len(chunks)
    _emit_progress(
        progress,
        "target_start",
        index=index,
        total=total,
        source=target.source,
        output=target.output,
        chunk_count=chunk_count,
        live=live_playback,
    )

    cache_dir.mkdir(parents=True, exist_ok=True)
    marker_path.unlink(missing_ok=True)
    last_played = 0
    if live_playback and progress_path.exists():
        try:
            last_played = int(progress_path.read_text(encoding="utf-8").strip() or "0")
        except ValueError:
            last_played = 0

    prebuffer_threshold = max(1, min(live_prebuffer, chunk_count)) if live_playback else 0
    playback_started = last_played >= prebuffer_threshold if live_playback else False
    chunk_paths: dict[int, Path] = {}
    chunk_files: list[Path] = []
    last_play_object = None

    for chunk_index, chunk in enumerate(chunks, start=1):
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
            live=live_playback,
        )
        chunk_path = _chunk_cache_path(cache_dir, chunk_index, chunk)
        if not chunk_path.exists():
            wav_bytes = client.synthesize_wav(chunk)
            chunk_path.write_bytes(wav_bytes)

        chunk_files.append(chunk_path)
        if live_playback:
            if chunk_index <= last_played:
                chunk_paths.pop(chunk_index, None)
                continue
            chunk_paths[chunk_index] = chunk_path
            if not playback_started and chunk_index >= prebuffer_threshold:
                playback_started = True
            if playback_started:
                while (last_played + 1) in chunk_paths:
                    next_index = last_played + 1
                    next_path = chunk_paths.pop(next_index)
                    if last_play_object is not None and hasattr(last_play_object, "wait_done"):
                        last_play_object.wait_done()
                    last_play_object = playback_callback(next_path) if playback_callback else None
                    last_played = next_index
                    progress_path.write_text(str(last_played), encoding="utf-8")

    if live_playback:
        if chunk_paths:
            for next_index in sorted(chunk_paths):
                if next_index <= last_played:
                    continue
                next_path = chunk_paths[next_index]
                if last_play_object is not None:
                    last_play_object.wait_done()
                last_play_object = playback_callback(next_path)
                last_played = next_index
                progress_path.write_text(str(last_played), encoding="utf-8")
        if last_play_object is not None and hasattr(last_play_object, "wait_done"):
            last_play_object.wait_done()

    book_title = target.output.parent.name
    chapter_id = target.source.stem
    track_number: str | None = None
    first_segment = chapter_id.split("_", 1)[0]
    if first_segment.isdigit():
        track_number = str(int(first_segment))

    display_number = track_number.zfill(3) if track_number is not None else f"{index:03d}"
    metadata: dict[str, str] = {
        "title": f"{display_number} {book_title}",
        "artist": book_title,
        "album": book_title,
    }
    if track_number is not None:
        metadata["track"] = track_number
    if total:
        metadata["tracktotal"] = str(total)

    if cancel_event and cancel_event.is_set():
        raise KeyboardInterrupt

    _merge_wavs_to_mp3(
        chunk_files,
        target.output,
        ffmpeg_path=ffmpeg_path,
        overwrite=overwrite or live_playback,
        metadata=metadata,
    )
    progress_path.unlink(missing_ok=True)
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
        live=live_playback,
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
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            cmd,
            cwd=str(runtime_executable.parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
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
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.speaker_id = speaker_id
        self.timeout = timeout
        self.post_phoneme_length = post_phoneme_length
        self._session = requests.Session()

    def synthesize_wav(self, text: str) -> bytes:
        """
        Generate WAV audio bytes for the provided text via VoiceVox.
        """
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

        if self.post_phoneme_length is not None and self.post_phoneme_length >= 0:
            payload_value = float(query_payload.get("postPhonemeLength", 0.0))
            query_payload["postPhonemeLength"] = max(
                payload_value,
                float(self.post_phoneme_length),
            )

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
    chunks: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            if current:
                chunk = "\n".join(current).strip()
                if chunk:
                    chunks.append(chunk)
                current = []
            continue
        current.append(line)
    if current:
        chunk = "\n".join(current).strip()
        if chunk:
            chunks.append(chunk)
    return chunks


def _merge_wavs_to_mp3(
    wav_paths: list[Path],
    output_path: Path,
    *,
    ffmpeg_path: str,
    overwrite: bool,
    metadata: dict[str, str] | None = None,
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
        cmd = [
            ffmpeg_path,
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_list),
            "-codec:a",
            "libmp3lame",
            "-qscale:a",
            "2",
        ]
        if metadata:
            for key, value in metadata.items():
                if not value:
                    continue
                cmd.extend(["-metadata", f"{key}={value}"])
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
    jobs: int = 1,
    cache_dir: Path | None = None,
    keep_cache: bool = False,
    live_playback: bool = False,
    playback_callback: Callable[[Path], None] | None = None,
    live_prebuffer: int = 2,
    progress: Callable[[dict[str, object]], None] | None = None,
    cancel_event: threading.Event | None = None,
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
    if live_playback:
        if playback_callback is None:
            raise ValueError("playback_callback must be provided when live_playback=True.")
        effective_jobs = 1
        live_prebuffer = max(1, live_prebuffer)
    generated: list[Path | None]

    if effective_jobs == 1:
        client = VoiceVoxClient(
            base_url=base_url,
            speaker_id=speaker_id,
            timeout=timeout,
            post_phoneme_length=post_phoneme_length,
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
                        live_playback=live_playback,
                        playback_callback=playback_callback,
                        live_prebuffer=live_prebuffer,
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
                    live_playback=live_playback,
                    playback_callback=playback_callback,
                    live_prebuffer=live_prebuffer,
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
