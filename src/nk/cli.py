from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import tomllib
import webbrowser
from importlib import metadata
from multiprocessing import Process
from pathlib import Path
from typing import Mapping

import uvicorn
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from .book_io import (
    BOOK_METADATA_FILENAME,
    M4B_MANIFEST_FILENAME,
    load_book_metadata,
    regenerate_m4b_manifest,
    update_book_tts_defaults,
    write_book_package,
)
from .cast import cast_manifests
from .core import (
    _apply_mapping_with_pattern,
    _build_mapping_pattern,
    _load_corpus_reading_accumulators,
    _select_reading_mapping,
    epub_to_chapter_texts,
    get_epub_cover,
)
from .deps import (
    DependencyInstallError,
    DependencyUninstallError,
    dependency_statuses,
    install_dependencies,
    uninstall_dependencies,
)
from .logging_utils import build_uvicorn_log_config
from .nlp import NLPBackend, NLPBackendUnavailableError
from .player import PlayerConfig, create_app
from .reader import create_reader_app
from .refine import OverrideRule, load_override_config, load_refine_config, refine_book, refine_chapter
from .tts import (
    FFmpegError,
    TTSTarget,
    VoiceVoxClient,
    VoiceVoxError,
    VoiceVoxRuntimeError,
    VoiceVoxUnavailableError,
    discover_voicevox_runtime,
    ensure_dedicated_voicevox_url,
    managed_voicevox_runtime,
    resolve_text_targets,
    set_debug_logging,
    synthesize_texts_to_mp3,
    wav_bytes_to_mp3,
    VoiceProfile,
    _voice_profile_from_defaults,
)
from .voice_samples import (
    build_sample_text,
    format_voice_sample_filename,
    voice_samples_from_payload,
)
from .voice_defaults import (
    DEFAULT_INTONATION_SCALE,
    DEFAULT_PITCH_SCALE,
    DEFAULT_SPEAKER_ID,
    DEFAULT_SPEED_SCALE,
)

_READER_RELOAD_ENV = "NK_READER_RELOAD_ROOT"
_PLAYER_RELOAD_ENV = "NK_PLAYER_RELOAD_CONFIG"
_OPEN_AUTO = "__NK_OPEN_AUTO__"
_SUBCOMMAND_HELP = """Subcommands:
  nk tts ...      Synthesize MP3s from chapterized text
  nk read ...     Launch the reader web UI
  nk play ...     Launch the audio player
  nk dav ...      Serve a WebDAV endpoint for books
  nk convert ...  Convert arbitrary text to kana
  nk cast ...     Annotate manifests with speaker/voice suggestions
  nk deps ...     Check or install runtime dependencies
  nk samples ...  Generate VoiceVox voice samples
  nk refine ...   Apply pitch overrides to chapterized text
"""



def _package_source_dir() -> Path:
    return Path(__file__).resolve().parent


def _reload_watch_dirs() -> list[str]:
    return [str(_package_source_dir())]


def _set_reader_reload_config(root: Path) -> None:
    payload = {
        "root": str(root),
    }
    os.environ[_READER_RELOAD_ENV] = json.dumps(payload)


def _reader_reload_app():
    raw_value = os.environ.get(_READER_RELOAD_ENV)
    if not raw_value:
        raise RuntimeError(
            "nk reader reload context missing. Start the server via `nk read --reload`."
        )
    root_value = raw_value
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict) and payload.get("root"):
        root_value = payload["root"]
    return create_reader_app(Path(root_value))


def _serialize_player_reload_config(
    config: PlayerConfig,
    *,
    reader_url: str | None = None,
) -> str:
    payload = {
        "root": str(config.root),
        "speaker": config.speaker,
        "engine_url": config.engine_url,
        "engine_runtime": str(config.engine_runtime) if config.engine_runtime else None,
        "engine_wait": config.engine_wait,
        "engine_threads": config.engine_threads,
        "ffmpeg_path": config.ffmpeg_path,
        "pause": config.pause,
        "speed_scale": config.speed_scale,
        "pitch_scale": config.pitch_scale,
        "intonation_scale": config.intonation_scale,
        "cache_dir": str(config.cache_dir) if config.cache_dir else None,
        "keep_cache": config.keep_cache,
        "reader_url": reader_url,
    }
    return json.dumps(payload)


def _set_player_reload_config(
    config: PlayerConfig,
    *,
    reader_url: str | None = None,
) -> None:
    os.environ[_PLAYER_RELOAD_ENV] = _serialize_player_reload_config(
        config,
        reader_url=reader_url,
    )


def _player_reload_app():
    raw_config = os.environ.get(_PLAYER_RELOAD_ENV)
    if not raw_config:
        raise RuntimeError(
            "nk player reload context missing. Start the server via `nk play --reload`."
        )
    try:
        data = json.loads(raw_config)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Invalid nk player reload configuration.") from exc

    def _to_path(value: str | None) -> Path | None:
        if not value:
            return None
        return Path(value)

    engine_threads_value = data.get("engine_threads")
    engine_threads = (
        int(engine_threads_value) if engine_threads_value is not None else None
    )
    config = PlayerConfig(
        root=Path(data["root"]),
        speaker=int(data["speaker"]),
        engine_url=data["engine_url"],
        engine_runtime=_to_path(data.get("engine_runtime")),
        engine_wait=float(data["engine_wait"]),
        engine_threads=engine_threads,
        ffmpeg_path=data["ffmpeg_path"],
        pause=float(data["pause"]),
        speed_scale=data.get("speed_scale"),
        pitch_scale=data.get("pitch_scale"),
        intonation_scale=data.get("intonation_scale"),
        cache_dir=_to_path(data.get("cache_dir")),
        keep_cache=bool(data.get("keep_cache", True)),
    )
    reader_url = data.get("reader_url")
    return create_app(config, reader_url=reader_url)


def _reader_process_entry(
    host: str, port: int, log_config: dict[str, object] | None
) -> None:
    uvicorn.run(
        "nk.cli:_reader_reload_app",
        host=host,
        port=port,
        log_level="info",
        log_config=log_config,
        factory=True,
    )


def _start_reader_process(
    root: Path,
    host: str,
    port: int,
    log_config: dict[str, object] | None,
) -> Process:
    _set_reader_reload_config(root)
    process = Process(
        target=_reader_process_entry,
        args=(host, port, log_config),
        daemon=True,
    )
    process.start()
    # Give the subprocess a moment to bind its socket or fail fast.
    time.sleep(0.35)
    if process.exitcode is not None:
        raise RuntimeError("nk reader failed to start (see logs above for details).")
    return process


def _stop_reader_process(process: Process | None) -> None:
    if not process:
        return
    if process.is_alive():
        process.terminate()
    process.join(timeout=5)


def _read_local_version() -> str | None:
    try:
        pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    except IndexError:  # pragma: no cover - defensive
        return None
    try:
        with pyproject_path.open("rb") as fh:
            data = tomllib.load(fh)
    except (FileNotFoundError, tomllib.TOMLDecodeError):
        return None
    return data.get("project", {}).get("version")


try:
    __version__ = metadata.version("nk")
except metadata.PackageNotFoundError:
    __version__ = _read_local_version() or "0.0.0+unknown"


def _add_version_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"nk {__version__}",
    )


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="EPUB → TXT with ruby propagation and base removal. Use `nk tts` for speech.",
        epilog=_SUBCOMMAND_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_version_flag(ap)
    ap.add_argument(
        "input_path",
        help="Path to input .epub or a directory containing .epub files",
    )
    ap.add_argument(
        "--transform",
        choices=("partial", "full"),
        default="partial",
        help=(
            "Choose which transform to write into .txt files: 'partial' keeps safe kanji (default), "
            "'full' converts everything to kana."
        ),
    )
    return ap


def build_convert_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Convert arbitrary text to kana using the NLP backend.",
    )
    _add_version_flag(ap)
    ap.add_argument(
        "text",
        nargs="+",
        help="Japanese text to convert. Wrap the phrase in quotes if it contains spaces.",
    )
    return ap


def _apply_dictionary_mapping(
    text: str,
    mapping: dict[str, str],
    context_rules: dict[str, object],
) -> str:
    if not mapping:
        return text
    pattern = _build_mapping_pattern(mapping)
    if pattern is None:
        return text
    return _apply_mapping_with_pattern(
        text,
        mapping,
        pattern,
        source_labels=None,
        context_rules=context_rules,
    )


def build_tts_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Synthesize MP3s from .txt files using a running VoiceVox engine.",
    )
    _add_version_flag(ap)
    ap.add_argument(
        "input_path",
        nargs="?",
        help="Path to a .txt file or directory containing .txt files.",
    )
    ap.add_argument(
        "--output-dir",
        help="Optional output directory for generated .mp3 files (defaults to input location).",
    )
    ap.add_argument(
        "--speaker",
        type=int,
        default=None,
        help="VoiceVox speaker ID to use (defaults to the saved per-book value or 2).",
    )
    ap.add_argument(
        "--speed",
        type=float,
        help=f"Override VoiceVox speedScale (default: {DEFAULT_SPEED_SCALE}).",
    )
    ap.add_argument(
        "--pitch",
        type=float,
        help=f"Override VoiceVox pitchScale (default: {DEFAULT_PITCH_SCALE}).",
    )
    ap.add_argument(
        "--intonation",
        type=float,
        help=f"Override VoiceVox intonationScale (default: {DEFAULT_INTONATION_SCALE}).",
    )
    ap.add_argument(
        "--engine-url",
        default="http://127.0.0.1:50021",
        help="Base URL for the VoiceVox engine (default: http://127.0.0.1:50021).",
    )
    ap.add_argument(
        "--ffmpeg",
        default="ffmpeg",
        help="Path to ffmpeg executable (default: ffmpeg).",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing .mp3 files if they already exist.",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds for VoiceVox requests (default: 30).",
    )
    ap.add_argument(
        "--engine-runtime",
        help=(
            "Path to the VoiceVox runtime executable or its directory. "
            "nk auto-detects common VoiceVox installs; use this to override."
        ),
    )
    ap.add_argument(
        "--engine-threads",
        type=int,
        help=(
            "When nk auto-starts the VoiceVox runtime, override its worker thread count "
            "(set to 0 or omit to let the engine decide)."
        ),
    )
    ap.add_argument(
        "--engine-runtime-wait",
        type=float,
        default=30.0,
        help=(
            "Seconds to wait for an auto-started VoiceVox engine to become ready "
            "(default: 30)."
        ),
    )
    ap.add_argument(
        "--pause",
        type=float,
        default=0.4,
        help=(
            "Seconds of trailing silence to request per chunk (VoiceVox postPhonemeLength). "
            "Set to 0 to keep the engine default."
        ),
    )
    ap.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Parallel synthesis workers (default: 1; use 0 for auto).",
    )
    ap.add_argument(
        "--start-index",
        type=int,
        help="Start synthesizing from this 1-based chapter index (skips earlier chapters unless --overwrite).",
    )
    ap.add_argument(
        "--cache-dir",
        help=(
            "Directory to persist chunk WAV caches for resume (default: .nk-tts-cache next to outputs)."
        ),
    )
    ap.add_argument(
        "--keep-cache",
        action="store_true",
        help="Retain cached WAV chunks after successful synthesis.",
    )
    ap.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear nk chunk caches under the provided path (or current directory if omitted).",
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging (pitch overrides, VoiceVox requests).",
    )
    return ap


def build_dav_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Serve nk-generated MP3 files over WebDAV (ideal for Flacbox).",
    )
    _add_version_flag(ap)
    ap.add_argument(
        "root",
        help="Directory containing chapterized books (only .mp3 files will be exposed).",
    )
    ap.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host interface for the WebDAV server (default: 0.0.0.0).",
    )
    ap.add_argument(
        "--port",
        type=int,
        default=1990,
        help="Port for the WebDAV server (default: 1990).",
    )
    ap.add_argument(
        "--auth",
        choices=["pam-login"],
        default="pam-login",
        help="Authentication backend (default: pam-login, uses your macOS login).",
    )
    return ap


def build_play_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Serve chapterized text as a browser-based VoiceVox player.",
    )
    _add_version_flag(ap)
    ap.add_argument(
        "root",
        help="Directory containing chapterized .txt files (one subdirectory per book).",
    )
    ap.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host interface for the web server (default: 0.0.0.0).",
    )
    ap.add_argument(
        "--port",
        type=int,
        default=2046,
        help="Port for the web server (default: 2046).",
    )
    ap.add_argument(
        "--reader-port",
        type=int,
        default=2047,
        help="Port for the companion reader server (default: 2047).",
    )
    ap.add_argument(
        "--reader-host",
        help="Host interface for the reader server (default: same as --host).",
    )
    ap.add_argument(
        "--speaker",
        type=int,
        default=2,
        help="VoiceVox speaker ID to use (default: 2).",
    )
    ap.add_argument(
        "--speed",
        type=float,
        help="Override VoiceVox speedScale (default: engine preset).",
    )
    ap.add_argument(
        "--pitch",
        type=float,
        help="Override VoiceVox pitchScale (default: engine preset).",
    )
    ap.add_argument(
        "--intonation",
        type=float,
        help="Override VoiceVox intonationScale (default: engine preset).",
    )
    ap.add_argument(
        "--engine-url",
        default="http://127.0.0.1:50021",
        help="Base URL for the VoiceVox engine (default: http://127.0.0.1:50021).",
    )
    ap.add_argument(
        "--engine-runtime",
        help="Path to the VoiceVox runtime executable or its directory.",
    )
    ap.add_argument(
        "--engine-threads",
        type=int,
        help=(
            "When nk auto-starts the VoiceVox runtime, override its worker thread count "
            "(omit or set to 0 to let the engine decide)."
        ),
    )
    ap.add_argument(
        "--engine-runtime-wait",
        type=float,
        default=30.0,
        help="Seconds to wait for an auto-started VoiceVox engine (default: 30).",
    )
    ap.add_argument(
        "--ffmpeg",
        default="ffmpeg",
        help="Path to ffmpeg executable (default: ffmpeg).",
    )
    ap.add_argument(
        "--pause",
        type=float,
        default=0.4,
        help="Trailing silence per chunk in seconds (default: 0.4).",
    )
    ap.add_argument(
        "--cache-dir",
        help="Directory to persist chunk caches (default: alongside output).",
    )
    ap.add_argument(
        "--keep-cache",
        action="store_true",
        help="Retain cached WAV chunks after playback completes.",
    )
    ap.add_argument(
        "--no-reader",
        action="store_true",
        help="Skip launching the nk Reader companion service.",
    )
    ap.add_argument(
        "--reload",
        action="store_true",
        help="Reload the server automatically when nk source files change.",
    )
    ap.add_argument(
        "--open",
        nargs="?",
        const=_OPEN_AUTO,
        metavar="HOST",
        help=(
            "Open the player URL in your default web browser. Optionally provide HOST "
            "(e.g., macbookpro) to override the opened hostname; omit to use your machine hostname."
        ),
    )
    return ap


def build_reader_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Visualize token alignment between transformed/original texts using a browser.",
    )
    _add_version_flag(ap)
    ap.add_argument(
        "root",
        help="Directory containing nk chapterized outputs (.txt, .token.json, optional .original.txt).",
    )
    ap.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host interface for the reader (default: 0.0.0.0).",
    )
    ap.add_argument(
        "--port",
        type=int,
        default=2047,
        help="Port for the reader (default: 2047).",
    )
    ap.add_argument(
        "--reload",
        action="store_true",
        help="Reload the server automatically when nk source files change.",
    )
    ap.add_argument(
        "--open",
        nargs="?",
        const=_OPEN_AUTO,
        metavar="HOST",
        help=(
            "Open the reader URL in your default web browser. Optionally provide HOST "
            "(e.g., macbookpro) to override the opened hostname; omit to use your machine hostname."
        ),
    )
    return ap


def build_refine_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Apply custom pitch overrides to a chapterized book."
    )
    _add_version_flag(ap)
    ap.add_argument("book_dir", help="Path to the chapterized book directory.")
    ap.add_argument(
        "--chapter",
        help=(
            "Optional chapter filename (relative to the book directory) to refine. "
            "When omitted, nk processes every chapter."
        ),
    )
    return ap


def build_deps_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Check or install nk runtime dependencies (UniDic, VoiceVox, ffmpeg)."
    )
    _add_version_flag(ap)
    subparsers = ap.add_subparsers(dest="command", required=False)

    check_parser = subparsers.add_parser(
        "check",
        help="Print detected dependency versions and locations.",
    )
    _add_version_flag(check_parser)

    install_parser = subparsers.add_parser(
        "install",
        help="Run the bundled install.sh helper to install runtimes.",
    )
    _add_version_flag(install_parser)
    install_parser.add_argument(
        "--script",
        type=Path,
        help="Override install.sh path (defaults to the copy shipped with nk).",
    )

    uninstall_parser = subparsers.add_parser(
        "uninstall",
        help="Remove dependencies that nk installed via install.sh (tracked in the manifest).",
    )
    _add_version_flag(uninstall_parser)
    uninstall_parser.add_argument(
        "--manifest",
        type=Path,
        help="Override the install manifest location (defaults to ~/.local/share/nk/deps-manifest.json or NK_STATE_DIR).",
    )

    ap.set_defaults(command="check")
    return ap


def build_cast_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Annotate chunk manifests with speaker/voice suggestions.",
    )
    _add_version_flag(ap)
    ap.add_argument(
        "target",
        help="Path to a book directory or to one or more *.tts.json files.",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing speaker/voice fields in manifests.",
    )
    return ap

def build_samples_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Generate VoiceVox samples for every available voice.",
    )
    _add_version_flag(ap)
    ap.add_argument(
        "root",
        nargs="?",
        default="books",
        help="Library root (samples saved under <root>/samples by default).",
    )
    ap.add_argument(
        "--output-dir",
        help="Override the output directory for samples (defaults to <root>/samples).",
    )
    ap.add_argument(
        "--text",
        action="append",
        help="Sample text to synthesize (repeatable; defaults to built-in templates).",
    )
    ap.add_argument(
        "--engine-url",
        default="http://127.0.0.1:50021",
        help="Base URL for the VoiceVox engine (default: http://127.0.0.1:50021).",
    )
    ap.add_argument(
        "--engine-runtime",
        help="Path to the VoiceVox runtime executable or its directory.",
    )
    ap.add_argument(
        "--engine-threads",
        type=int,
        help=(
            "When nk auto-starts the VoiceVox runtime, override its worker thread count "
            "(omit or set to 0 to let the engine decide)."
        ),
    )
    ap.add_argument(
        "--engine-runtime-wait",
        type=float,
        default=30.0,
        help="Seconds to wait for an auto-started VoiceVox engine (default: 30).",
    )
    ap.add_argument(
        "--ffmpeg",
        default="ffmpeg",
        help="Path to ffmpeg executable (default: ffmpeg).",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds for VoiceVox requests (default: 30).",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing sample files.",
    )
    return ap


def _engine_thread_overrides(
    threads: int | None,
) -> tuple[dict[str, str] | None, int | None]:
    if threads is None or threads <= 0:
        return None, None
    clamped = max(1, int(threads))
    env = {
        "VOICEVOX_CPU_NUM_THREADS": str(clamped),
        "RAYON_NUM_THREADS": str(clamped),
    }
    return env, clamped


def _run_convert(args: argparse.Namespace) -> int:
    try:
        backend = NLPBackend()
    except NLPBackendUnavailableError as exc:
        raise SystemExit(str(exc)) from exc
    text = " ".join(args.text).strip()
    if not text:
        raise SystemExit("No text provided for conversion.")
    accumulators = _load_corpus_reading_accumulators()
    tier3, tier2, context_rules = _select_reading_mapping(accumulators, backend)
    processed = _apply_dictionary_mapping(text, tier3, context_rules)
    processed = _apply_dictionary_mapping(processed, tier2, context_rules)
    converted = backend.to_reading_text(processed)
    print(converted)
    return 0


def _run_samples(args: argparse.Namespace) -> int:
    try:
        sample_text = build_sample_text(args.text)
    except ValueError as exc:
        raise SystemExit(f"{exc} Provide --text to override.") from exc

    root = Path(args.root).expanduser()
    output_dir = (
        Path(args.output_dir).expanduser()
        if args.output_dir
        else root / "samples"
    )
    if output_dir.exists() and not output_dir.is_dir():
        raise SystemExit(f"Output path is not a directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    runtime_hint = args.engine_runtime
    auto_runtime = None
    if not runtime_hint:
        auto_runtime = discover_voicevox_runtime(args.engine_url)
    runtime_path = runtime_hint or auto_runtime
    engine_url = args.engine_url
    runtime_env, runtime_thread_flag = _engine_thread_overrides(args.engine_threads)

    try:
        if runtime_path:
            engine_url, dedicated_runtime = ensure_dedicated_voicevox_url(engine_url)
            if dedicated_runtime and engine_url != args.engine_url:
                print(
                    f"Existing VoiceVox detected at {args.engine_url}; "
                    f"launching a dedicated runtime on {engine_url}.",
                    flush=True,
                )
        with managed_voicevox_runtime(
            runtime_path,
            engine_url,
            readiness_timeout=args.engine_runtime_wait,
            extra_env=runtime_env,
            cpu_threads=runtime_thread_flag,
        ):
            client = VoiceVoxClient(base_url=engine_url, timeout=args.timeout)
            try:
                voices = voice_samples_from_payload(client.list_speakers())
                if not voices:
                    raise SystemExit(
                        f"No VoiceVox speakers found at {engine_url}."
                    )
                max_id = max(speaker_id for speaker_id, _ in voices)
                width = max(3, len(str(max_id)))
                total = len(voices)
                print(
                    f"[nk samples] Generating {total} voice samples in {output_dir}"
                )
                for index, (speaker_id, name) in enumerate(voices, start=1):
                    filename = format_voice_sample_filename(
                        speaker_id,
                        name,
                        width=width,
                    )
                    output_path = output_dir / filename
                    if output_path.exists() and not args.overwrite:
                        print(
                            f"[{index}/{total}] {filename} skipped (exists)",
                            flush=True,
                        )
                        continue
                    client.speaker_id = speaker_id
                    wav_bytes = client.synthesize_wav(sample_text)
                    wav_bytes_to_mp3(
                        wav_bytes,
                        output_path,
                        ffmpeg_path=args.ffmpeg,
                        overwrite=args.overwrite,
                    )
                    print(f"[{index}/{total}] {filename}", flush=True)
            finally:
                client.close()
    except (
        VoiceVoxUnavailableError,
        VoiceVoxError,
        VoiceVoxRuntimeError,
        FFmpegError,
        FileNotFoundError,
        ValueError,
    ) as exc:
        raise SystemExit(str(exc)) from exc

    return 0


def _ensure_tts_source_ready(
    input_path: Path,
    *,
    nlp: NLPBackend | None = None,
    quiet: bool = False,
) -> Path:
    if not input_path.exists():
        return input_path
    if input_path.is_file() and input_path.suffix.lower() == ".epub":
        target_dir = input_path.with_suffix("")
        metadata_missing = not (target_dir / BOOK_METADATA_FILENAME).exists()
        m4b_missing = not (target_dir / M4B_MANIFEST_FILENAME).exists()
        needs_chapter = (
            metadata_missing
            or m4b_missing
            or not target_dir.exists()
            or not any(target_dir.glob("*.txt"))
        )
        if needs_chapter and not quiet:
            print(f"[nk tts] Chapterizing {input_path.name}")
        if needs_chapter:
            backend = nlp
            if backend is None:
                try:
                    backend = NLPBackend()
                except NLPBackendUnavailableError as exc:
                    raise SystemExit(str(exc)) from exc
            chapters, ruby_evidence = epub_to_chapter_texts(
                str(input_path),
                nlp=backend,
                transform="partial",
            )
            cover = get_epub_cover(str(input_path))
            write_book_package(
                target_dir,
                chapters,
                source_epub=input_path,
                cover_image=cover,
                ruby_evidence=ruby_evidence,
            )
        else:
            regenerate_m4b_manifest(target_dir)
        return target_dir
    return input_path


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


def _chapterize_epub(
    epub_path: Path,
    backend: NLPBackend,
    *,
    progress_display: Progress | None,
    console: Console,
    transform: str,
) -> None:
    book_label = epub_path.name
    output_dir = epub_path.with_suffix("")
    task_id: int | None = None
    override_progress_step = 1.0

    def _format_override_label(
        path_value: object, index_value: object, total_value: object
    ) -> str:
        path_label = ""
        if isinstance(path_value, Path):
            path_label = path_value.name
        elif isinstance(path_value, str):
            path_label = Path(path_value).name
        prefix = ""
        if isinstance(index_value, int):
            prefix = str(index_value)
            if isinstance(total_value, int) and total_value > 0:
                prefix = f"{index_value}/{total_value}"
        if prefix and path_label:
            return f"{prefix} {path_label}"
        return path_label or prefix

    if progress_display:
        task_id = progress_display.add_task(book_label, total=1)
    else:
        console.print(f"[nk] {book_label}")

    def _progress_callback(event: dict[str, object]) -> None:
        event_type = event.get("event")
        total = event.get("total")
        if isinstance(total, int) and total <= 0:
            total = None
        index = event.get("index")
        if not isinstance(index, int):
            index = None
        title = event.get("title") or event.get("title_hint")
        if not isinstance(title, str) or not title.strip():
            source = event.get("source")
            if isinstance(source, Path):
                title = source.stem
            elif isinstance(source, str):
                title = Path(source).stem
            else:
                title = ""
        description = _format_chapter_progress_label(book_label, index, total, title)
        if progress_display and task_id is not None:
            task = progress_display.tasks[task_id]
            if isinstance(total, int) and (task.total is None or task.total == 1):
                progress_display.update(
                    task_id, total=total, completed=min(task.completed, total - 1)
                )
            if event_type == "chapter_prepare":
                progress_display.update(
                    task_id, description=f"{book_label} · preparing"
                )
            elif event_type == "chapter_start":
                progress_display.update(task_id, description=description)
            elif event_type == "chapter_done":
                completed_value = (
                    index if isinstance(index, int) else task.completed + 1
                )
                progress_display.update(
                    task_id,
                    completed=completed_value,
                    description=description,
                )
        else:
            if event_type == "chapter_done":
                console.print(f"  {description}")

    chapters, ruby_evidence = epub_to_chapter_texts(
        str(epub_path),
        nlp=backend,
        progress=_progress_callback,
        transform=transform,
    )
    base_total = len(chapters) or 1
    if progress_display and task_id is not None:
        task = progress_display.tasks[task_id]
        base_total = task.total or base_total
        progress_display.update(
            task_id,
            total=base_total + 1,
            completed=min(task.completed, base_total),
            description=f"{book_label} · writing…",
        )
    else:
        console.print(f"[nk] Writing {book_label}…", style="dim")
    if progress_display and task_id is not None:
        task = progress_display.tasks[task_id]
        base_completed = min(task.completed, base_total)
    else:
        base_completed = base_total
    cover = get_epub_cover(str(epub_path))
    package = write_book_package(
        output_dir,
        chapters,
        source_epub=epub_path,
        cover_image=cover,
        ruby_evidence=ruby_evidence,
        apply_overrides=False,
    )
    overrides: list[OverrideRule] = []
    removals = []
    try:
        overrides, removals = load_refine_config(output_dir)
    except ValueError as exc:
        console.print(
            f"[nk] Warn: failed to load custom_token.json: {exc}", style="yellow"
        )
    has_refinements = bool(overrides or removals)
    if progress_display and task_id is not None:
        task = progress_display.tasks[task_id]
        completed_after_write = min(
            task.completed, task.total - 1 if task.total else base_completed
        )
        progress_display.update(
            task_id,
            completed=completed_after_write + 1,
            description=f"{book_label} · writing…",
        )
        if has_refinements:
            override_total = len(chapters)
            if override_total > 0:
                override_progress_step = 1.0 / override_total
            progress_display.update(
                task_id,
                total=(task.total or (base_total + 1)) + 1,
                description=f"{book_label} · applying overrides…",
            )
    else:
        if has_refinements:
            console.print(f"[nk] Applying overrides for {book_label}…", style="dim")

    def _refine_progress_handler(event: dict[str, object]) -> None:
        nonlocal override_progress_step
        if not (progress_display and task_id is not None):
            return
        event_type = event.get("event")
        task = progress_display.tasks[task_id]
        if event_type == "book_start":
            total_chapters = event.get("total_chapters")
            if isinstance(total_chapters, int) and total_chapters > 0:
                override_progress_step = 1.0 / total_chapters
                base_completed = task.completed or 0
                progress_display.update(
                    task_id,
                    total=max(task.total or 0, base_completed + 1),
                )
        elif event_type == "chapter_start":
            label = _format_override_label(
                event.get("path"),
                event.get("index"),
                event.get("total"),
            )
            desc = f"{book_label} · applying overrides…"
            if label:
                desc = f"{desc} · {label}"
            progress_display.update(task_id, description=desc)
        elif event_type == "chapter_done":
            progress_display.advance(task_id, override_progress_step)
            label = _format_override_label(
                event.get("path"),
                event.get("index"),
                event.get("total"),
            )
            if label:
                progress_display.update(
                    task_id, description=f"{book_label} · applying overrides… · {label}"
                )

    try:
        refined = (
            refine_book(
                output_dir,
                overrides if overrides else None,
                removals=removals,
                progress=_refine_progress_handler,
            )
            if has_refinements
            else 0
        )
    except ValueError as exc:
        console.print(f"[nk] Warn: failed to apply overrides: {exc}", style="yellow")
        refined = 0
    if progress_display and task_id is not None:
        task = progress_display.tasks[task_id]
        final_total = task.total or (base_total + (2 if has_refinements else 1))
        progress_display.update(
            task_id,
            total=final_total,
            completed=final_total,
            description=f"{book_label} · complete",
        )
    else:
        if refined:
            console.print(
                f"[nk] Applied {refined} refine pass(es) from custom_token.json",
                style="dim",
            )
        console.print(f"  → {output_dir}", style="dim")


def _run_tts(args: argparse.Namespace) -> int:
    set_debug_logging(bool(getattr(args, "debug", False)))
    if args.clear_cache:
        search_root = Path(args.input_path or ".").expanduser().resolve()
        cleared = False
        for cache_dir in sorted(search_root.rglob(".nk-tts-cache")):
            if cache_dir.is_dir():
                shutil.rmtree(cache_dir, ignore_errors=True)
                cleared = True
        if not cleared:
            print(f"No .nk-tts-cache directories found under {search_root}")
        else:
            print(f"Cleared caches under {search_root}")
        return 0

    if args.input_path is None:
        raise SystemExit("Input path is required unless --clear-cache is used.")

    speaker_override_set = args.speaker is not None
    speed_override_set = args.speed is not None
    pitch_override_set = args.pitch is not None
    intonation_override_set = args.intonation is not None

    input_path = Path(args.input_path)
    input_path = _ensure_tts_source_ready(input_path)
    defaults_book_dir = input_path if input_path.is_dir() else input_path.parent
    metadata_path = (
        defaults_book_dir / BOOK_METADATA_FILENAME
        if defaults_book_dir.is_dir()
        else None
    )
    metadata_for_defaults = None
    if defaults_book_dir.is_dir():
        metadata_for_defaults = load_book_metadata(defaults_book_dir)
    stored_defaults = (
        metadata_for_defaults.tts_defaults if metadata_for_defaults else None
    )

    if args.speaker is None:
        if stored_defaults and stored_defaults.speaker is not None:
            args.speaker = stored_defaults.speaker
        else:
            args.speaker = DEFAULT_SPEAKER_ID
    if args.speed is None:
        if stored_defaults and stored_defaults.speed is not None:
            args.speed = stored_defaults.speed
        else:
            args.speed = DEFAULT_SPEED_SCALE
    if args.pitch is None:
        if stored_defaults and stored_defaults.pitch is not None:
            args.pitch = stored_defaults.pitch
        else:
            args.pitch = DEFAULT_PITCH_SCALE
    if args.intonation is None:
        if stored_defaults and stored_defaults.intonation is not None:
            args.intonation = stored_defaults.intonation
        else:
            args.intonation = DEFAULT_INTONATION_SCALE

    def _format_value(name: str, value: float | int | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, float):
            return f"{name}={format(value, 'g')}"
        return f"{name}={value}"

    def _format_setting(
        name: str, value: float | int | None, source: str | None = None
    ) -> str:
        if value is None:
            return f"{name}=engine default"
        text = _format_value(name, value) or f"{name}={value}"
        if source == "cli":
            return f"{text} [CLI]"
        if source == "saved":
            return f"{text} [saved]"
        return text

    def _format_transition(
        name: str, previous: float | int | None, new_value: float | int | None
    ) -> str:
        def _value_text(val: float | int | None) -> str:
            if val is None:
                return "engine default"
            if isinstance(val, float):
                return format(val, "g")
            return str(val)

        return f"{name} {_value_text(previous)} → {_value_text(new_value)}"

    def _format_engine_defaults(values: Mapping[str, float]) -> str:
        order = ("speed", "pitch", "intonation")
        parts: list[str] = []
        for name in order:
            if name in values:
                parts.append(f"{name}={format(values[name], 'g')}")
        for name in sorted(values.keys()):
            if name in order:
                continue
            parts.append(f"{name}={format(values[name], 'g')}")
        return ", ".join(parts) if parts else "none"

    setting_sources: dict[str, str | None] = {}
    if speaker_override_set:
        setting_sources["speaker"] = "cli"
    elif stored_defaults and stored_defaults.speaker is not None:
        setting_sources["speaker"] = "saved"
    else:
        setting_sources["speaker"] = None

    def _source_for_scale(
        override_flag: bool,
        stored_value: float | None,
        current_value: float | None,
    ) -> str | None:
        if override_flag:
            return "cli"
        if stored_value is not None and current_value is not None:
            return "saved"
        return None

    setting_sources["speed"] = _source_for_scale(
        speed_override_set,
        stored_defaults.speed if stored_defaults else None,
        args.speed,
    )
    setting_sources["pitch"] = _source_for_scale(
        pitch_override_set,
        stored_defaults.pitch if stored_defaults else None,
        args.pitch,
    )
    setting_sources["intonation"] = _source_for_scale(
        intonation_override_set,
        stored_defaults.intonation if stored_defaults else None,
        args.intonation,
    )

    saved_values_summary: list[str] = []
    if stored_defaults:
        for name, value in (
            ("speaker", stored_defaults.speaker),
            ("speed", stored_defaults.speed),
            ("pitch", stored_defaults.pitch),
            ("intonation", stored_defaults.intonation),
        ):
            formatted = _format_value(name, value)
            if formatted:
                saved_values_summary.append(formatted)
    if saved_values_summary:
        source_path = metadata_path if metadata_path else defaults_book_dir
        saved_summary = ", ".join(saved_values_summary)
        print(
            f"[nk tts] Loaded saved voice defaults from {source_path} ({saved_summary})."
        )

    voice_settings = {
        "speaker": args.speaker,
        "speed": args.speed,
        "pitch": args.pitch,
        "intonation": args.intonation,
    }
    summary_parts = [
        _format_setting(name, value, setting_sources.get(name))
        for name, value in voice_settings.items()
    ]
    summary = ", ".join(summary_parts)
    print(f"[nk tts] Voice settings: {summary}.")

    auto_default_fields = {
        "speed": args.speed is None,
        "pitch": args.pitch is None,
        "intonation": args.intonation is None,
    }

    changed_fields: list[str] = []
    if (
        speaker_override_set
        and stored_defaults
        and stored_defaults.speaker is not None
        and stored_defaults.speaker != args.speaker
    ):
        changed_fields.append(
            _format_transition("speaker", stored_defaults.speaker, args.speaker)
        )
    if (
        speed_override_set
        and stored_defaults
        and stored_defaults.speed is not None
        and args.speed is not None
        and stored_defaults.speed != args.speed
    ):
        changed_fields.append(
            _format_transition("speed", stored_defaults.speed, args.speed)
        )
    if (
        pitch_override_set
        and stored_defaults
        and stored_defaults.pitch is not None
        and args.pitch is not None
        and stored_defaults.pitch != args.pitch
    ):
        changed_fields.append(
            _format_transition("pitch", stored_defaults.pitch, args.pitch)
        )
    if (
        intonation_override_set
        and stored_defaults
        and stored_defaults.intonation is not None
        and args.intonation is not None
        and stored_defaults.intonation != args.intonation
    ):
        changed_fields.append(
            _format_transition(
                "intonation", stored_defaults.intonation, args.intonation
            )
        )

    if not saved_values_summary and not (
        speaker_override_set
        or speed_override_set
        or pitch_override_set
        or intonation_override_set
    ):
        print("[nk tts] No saved voice defaults found; using built-in engine settings.")

    if changed_fields:
        print(
            "[nk tts] Voice overrides differ from previously saved defaults "
            f"({'; '.join(changed_fields)}). Expect synthesized voices to change."
        )

    baseline_updates: dict[str, float | int] = {}
    if (
        not speaker_override_set
        and args.speaker is not None
        and (stored_defaults is None or stored_defaults.speaker != args.speaker)
    ):
        baseline_updates["speaker"] = args.speaker
    if (
        not speed_override_set
        and args.speed is not None
        and (stored_defaults is None or stored_defaults.speed != args.speed)
    ):
        baseline_updates["speed"] = args.speed
    if (
        not pitch_override_set
        and args.pitch is not None
        and (stored_defaults is None or stored_defaults.pitch != args.pitch)
    ):
        baseline_updates["pitch"] = args.pitch
    if (
        not intonation_override_set
        and args.intonation is not None
        and (stored_defaults is None or stored_defaults.intonation != args.intonation)
    ):
        baseline_updates["intonation"] = args.intonation

    engine_defaults_lock = threading.Lock()
    observed_engine_defaults: dict[str, float] = {}

    def _capture_engine_defaults(defaults: Mapping[str, float]) -> None:
        if not defaults:
            return
        normalized = {
            key: float(value)
            for key, value in defaults.items()
            if isinstance(value, (int, float))
            and key in {"speed", "pitch", "intonation"}
        }
        filtered = {
            key: normalized[key]
            for key, flag in auto_default_fields.items()
            if flag and key in normalized
        }
        if not filtered:
            return
        newly_added: dict[str, float] = {}
        with engine_defaults_lock:
            for key, value in filtered.items():
                if key not in observed_engine_defaults:
                    observed_engine_defaults[key] = value
                    newly_added[key] = value
        if not newly_added:
            return
        summary = _format_engine_defaults(newly_added)
        print(f"[nk tts] VoiceVox defaults in use ({summary}).")

    def _persist_engine_defaults() -> None:
        if not defaults_book_dir.is_dir():
            return
        pending: dict[str, float | int] = {}
        pending.update(baseline_updates)
        pending.update(observed_engine_defaults)
        if not pending:
            return
        if update_book_tts_defaults(defaults_book_dir, pending):
            saved_path = (
                metadata_path
                if metadata_path
                else defaults_book_dir / BOOK_METADATA_FILENAME
            )
            summary_values = [
                _format_value(name, pending[name]) or f"{name}={pending[name]}"
                for name in ("speaker", "speed", "pitch", "intonation")
                if name in pending
            ]
            summary_text = ", ".join(summary_values)
            print(f"[nk tts] Saved voice defaults to {saved_path} ({summary_text}).")

    remember_updates: dict[str, float | int] = {}
    if speaker_override_set and args.speaker is not None:
        remember_updates["speaker"] = args.speaker
    if speed_override_set and args.speed is not None:
        remember_updates["speed"] = args.speed
    if pitch_override_set and args.pitch is not None:
        remember_updates["pitch"] = args.pitch
    if intonation_override_set and args.intonation is not None:
        remember_updates["intonation"] = args.intonation
    saved_overrides = False
    if remember_updates and defaults_book_dir.is_dir():
        saved_overrides = update_book_tts_defaults(defaults_book_dir, remember_updates)
        if saved_overrides:
            saved_path = (
                metadata_path
                if metadata_path
                else defaults_book_dir / BOOK_METADATA_FILENAME
            )
            saved_pairs = ", ".join(
                _format_value(name, remember_updates[name])
                or f"{name}={remember_updates[name]}"
                for name in ("speaker", "speed", "pitch", "intonation")
                if name in remember_updates
            )
            print(f"[nk tts] Saved voice overrides to {saved_path} ({saved_pairs}).")

    # Persist pending defaults immediately so manual settings survive if synthesis is interrupted.
    _persist_engine_defaults()

    output_dir = Path(args.output_dir) if args.output_dir else None
    try:
        targets = resolve_text_targets(input_path, output_dir)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    total_targets = len(targets)
    if total_targets == 0:
        raise SystemExit("No .txt files found for synthesis.")

    if args.start_index is not None:
        try:
            targets = _slice_targets_by_index(targets, args.start_index)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        if args.start_index > 1:
            skipped = min(total_targets, args.start_index - 1)
            print(
                f"Skipping {skipped} chapters; starting synthesis at index {args.start_index}."
            )

    total_targets = len(targets)
    printed_progress = {"value": False}

    narrator_voice = VoiceProfile(
        speaker=args.speaker,
        speed=args.speed,
        pitch=args.pitch,
        intonation=args.intonation,
    )
    voice_overlays: dict[str, VoiceProfile] | None = None
    if metadata_for_defaults and metadata_for_defaults.tts_voices:
        voice_overlays = {}
        for name, defaults in metadata_for_defaults.tts_voices.items():
            profile = _voice_profile_from_defaults(defaults)
            if profile:
                voice_overlays[name] = profile

    class _RichProgress:
        def __init__(self, enabled: bool, total: int) -> None:
            self.console = Console(stderr=True)
            self.enabled = enabled and total > 0 and self.console.is_terminal
            self.lock = threading.Lock()
            self.task_by_key: dict[str, dict[str, object]] = {}
            if not self.enabled:
                self.progress: Progress | None = None
                self.overall_task = None
                return
            self.progress = Progress(
                TextColumn("{task.description}", justify="left"),
                BarColumn(bar_width=None),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                TextColumn("{task.fields[detail]}", justify="left"),
                console=self.console,
                auto_refresh=True,
                transient=False,
            )
            self.progress.start()
            self.overall_task = self.progress.add_task(
                "All chapters", total=total, detail=""
            )

        @staticmethod
        def _truncate(text: str, width: int = 24) -> str:
            text = text.strip()
            if len(text) <= width:
                return text
            return text[: max(0, width - 1)] + "…"

        def _source_label(self, source: object) -> tuple[str, str]:
            if isinstance(source, Path):
                return str(source.resolve()), source.name
            if source is None:
                return "", ""
            text = str(source)
            return text, text

        def _format_label(self, raw: str) -> str:
            if not raw:
                return "chapter"
            name = raw
            if "." in name:
                name = Path(name).stem
            name = name.replace("_", " ")
            prefix = ""
            remainder = name
            parts = name.split(maxsplit=1)
            if parts and parts[0].isdigit():
                prefix = parts[0]
                remainder = parts[1] if len(parts) > 1 else ""
            remainder = remainder.strip()
            if remainder:
                remainder = self._truncate(remainder, 18)
                label = f"{prefix} {remainder}".strip()
            else:
                label = prefix or self._truncate(name, 20)
            return label or "chapter"

        def handle(self, event: dict[str, object]) -> bool:
            if not self.enabled or self.progress is None:
                return False
            event_type = event.get("event")
            key, raw_label = self._source_label(event.get("source"))
            label = self._format_label(raw_label)
            chunk_count = event.get("chunk_count")
            chunk_index = event.get("chunk_index")
            output = event.get("output")
            reason = event.get("reason")
            with self.lock:
                if event_type == "target_start":
                    total = (
                        chunk_count
                        if isinstance(chunk_count, int) and chunk_count > 0
                        else None
                    )
                    desc = label
                    task_id = self.progress.add_task(desc, total=total, detail="")
                    self.task_by_key[key] = {
                        "task_id": task_id,
                        "total": total,
                    }
                elif event_type == "chunk_start":
                    info = self.task_by_key.get(key)
                    if info is not None:
                        task_id = info["task_id"]
                        total = info["total"]
                        if (
                            total is None
                            and isinstance(chunk_count, int)
                            and chunk_count > 0
                        ):
                            total = chunk_count
                            info["total"] = total
                            self.progress.update(task_id, total=total)
                        if isinstance(chunk_index, int):
                            completed = chunk_index
                            if total is not None:
                                completed = min(completed, total)
                                detail = f"{chunk_index}/{total} chunks"
                            else:
                                detail = f"chunk {chunk_index}"
                            self.progress.update(
                                task_id, completed=completed, detail=detail
                            )
                elif event_type == "target_done":
                    info = self.task_by_key.pop(key, None)
                    if info is not None:
                        task_id = info["task_id"]
                        total = info["total"]
                        if isinstance(total, int) and total > 0:
                            detail = f"{total}/{total} chunks"
                        elif isinstance(output, Path):
                            detail = self._truncate(output.name, 28)
                        elif output:
                            detail = self._truncate(str(output), 28)
                        else:
                            detail = "completed"
                        if total is not None:
                            self.progress.update(
                                task_id, completed=total, detail=detail
                            )
                        else:
                            current = self.progress.tasks[task_id].completed
                            self.progress.update(
                                task_id, completed=current, detail=detail
                            )
                        self.progress.stop_task(task_id)
                    if self.overall_task is not None:
                        self.progress.advance(self.overall_task, 1)
                elif event_type == "target_skipped":
                    info = self.task_by_key.pop(key, None)
                    if info is not None:
                        task_id = info["task_id"]
                        detail = f"skipped ({reason})" if reason else "skipped"
                        self.progress.update(task_id, detail=detail)
                        self.progress.stop_task(task_id)
                    if self.overall_task is not None:
                        self.progress.advance(self.overall_task, 1)
                else:
                    return False
            return True

        def close(self) -> None:
            if not self.enabled or self.progress is None:
                return
            with self.lock:
                self.progress.stop()

    progress_handler = _RichProgress(enabled=True, total=total_targets)
    cancel_event = threading.Event()

    def _fallback_print(event: dict[str, object]) -> None:
        printed_progress["value"] = True
        event_type = event.get("event")
        index = event.get("index")
        total = event.get("total")
        source = event.get("source")
        output = event.get("output")
        chunk_count = event.get("chunk_count")
        if isinstance(source, Path):
            source_name = source.name
        else:
            source_name = str(source) if source is not None else ""
        if event_type == "target_start":
            chunk_info = (
                f" ({chunk_count} chunks)"
                if isinstance(chunk_count, int) and chunk_count > 1
                else ""
            )
            print(f"[{index}/{total}] {source_name}{chunk_info}", flush=True)
        elif event_type == "chunk_start":
            chunk_index = event.get("chunk_index")
            chunk_total = event.get("chunk_count")
            if isinstance(chunk_total, int) and chunk_total > 1:
                print(
                    f"[{index}/{total}] chunk {chunk_index}/{chunk_total} ({source_name})",
                    flush=True,
                )
        elif event_type == "target_done":
            output_str = str(output) if output is not None else ""
            print(f"[{index}/{total}] {source_name} -> {output_str}", flush=True)
        elif event_type == "target_skipped":
            reason = event.get("reason", "skipped")
            print(f"[{index}/{total}] {source_name} skipped ({reason})", flush=True)

    def _progress_printer(event: dict[str, object]) -> None:
        handled = progress_handler.handle(event)
        if handled:
            if event.get("event") in {"target_start", "target_done", "target_skipped"}:
                printed_progress["value"] = True
            return
        _fallback_print(event)

    runtime_hint = args.engine_runtime
    auto_runtime = None
    if not runtime_hint:
        auto_runtime = discover_voicevox_runtime(args.engine_url)

    runtime_path = runtime_hint or auto_runtime
    engine_url = args.engine_url
    runtime_env, runtime_thread_flag = _engine_thread_overrides(args.engine_threads)
    cache_base = Path(args.cache_dir).expanduser() if args.cache_dir else None

    generated: list[Path] = []

    try:
        if runtime_path:
            engine_url, dedicated_runtime = ensure_dedicated_voicevox_url(engine_url)
            if dedicated_runtime and engine_url != args.engine_url:
                print(
                    f"Existing VoiceVox detected at {args.engine_url}; "
                    f"launching a dedicated runtime on {engine_url}.",
                    flush=True,
                )
        with managed_voicevox_runtime(
            runtime_path,
            engine_url,
            readiness_timeout=args.engine_runtime_wait,
            extra_env=runtime_env,
            cpu_threads=runtime_thread_flag,
        ):
            generated = synthesize_texts_to_mp3(
                targets,
                speaker_id=args.speaker,
                base_url=engine_url,
                ffmpeg_path=args.ffmpeg,
                overwrite=args.overwrite,
                timeout=args.timeout,
                post_phoneme_length=max(args.pause, 0.0)
                if args.pause is not None
                else None,
                speed_scale=args.speed,
                pitch_scale=args.pitch,
                intonation_scale=args.intonation,
                jobs=args.jobs,
                cache_dir=cache_base,
                keep_cache=args.keep_cache,
                progress=_progress_printer,
                cancel_event=cancel_event,
                engine_defaults_callback=_capture_engine_defaults,
                narrator_voice=narrator_voice,
                voice_overlays=voice_overlays,
            )
    except KeyboardInterrupt:
        cancel_event.set()
        print("\nInterrupted. Stopping immediately.", flush=True)
        return 130
    except (
        VoiceVoxUnavailableError,
        VoiceVoxError,
        VoiceVoxRuntimeError,
        FFmpegError,
        FileNotFoundError,
        ValueError,
    ) as exc:
        raise SystemExit(str(exc)) from exc
    finally:
        progress_handler.close()
        _persist_engine_defaults()

    if not generated:
        print("No audio generated (all input texts were empty).")
    else:
        if not printed_progress["value"]:
            for path in generated:
                print(path)
    return 0


def _run_refine(args: argparse.Namespace) -> int:
    book_dir = Path(args.book_dir).expanduser()
    if not book_dir.is_dir():
        raise SystemExit(f"Book directory not found: {book_dir}")
    chapter_path: Path | None = None
    if args.chapter:
        candidate = Path(args.chapter)
        if not candidate.is_absolute():
            candidate = (book_dir / candidate).resolve()
        else:
            candidate = candidate.resolve()
        try:
            candidate.relative_to(book_dir)
        except ValueError as exc:
            raise SystemExit(
                "Chapter must be inside the provided book directory."
            ) from exc
        if candidate.suffix.lower() != ".txt":
            raise SystemExit("Chapter must be a .txt file.")
        if not candidate.exists():
            raise SystemExit(f"Chapter file not found: {candidate}")
        chapter_path = candidate
    try:
        overrides, removals = load_refine_config(book_dir)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not overrides and not removals:
        print(f"No overrides or remove rules found in {book_dir / 'custom_token.json'}")
        return 0
    console = Console()
    progress_display: Progress | None = None
    progress_task: int | None = None

    def _format_chapter_label(
        path_value: object, index_value: object, total_value: object
    ) -> str:
        path_label = ""
        if isinstance(path_value, Path):
            try:
                path_label = str(path_value.relative_to(book_dir))
            except ValueError:
                path_label = path_value.name
        elif isinstance(path_value, str):
            path_label = path_value
        prefix = ""
        if isinstance(index_value, int):
            prefix = str(index_value)
            if isinstance(total_value, int) and total_value > 0:
                prefix = f"{index_value}/{total_value}"
        if prefix and path_label:
            return f"{prefix} {path_label}"
        return path_label or prefix

    def _progress_handler(event: dict[str, object]) -> None:
        nonlocal progress_task
        event_type = event.get("event")
        if progress_display is not None and progress_task is not None:
            if event_type == "book_start":
                total_chapters = event.get("total_chapters")
                if isinstance(total_chapters, int) and total_chapters > 0:
                    progress_display.update(
                        progress_task, total=total_chapters, completed=0
                    )
            elif event_type == "chapter_start":
                label = _format_chapter_label(
                    event.get("path"),
                    event.get("index"),
                    event.get("total"),
                )
                if label:
                    progress_display.update(progress_task, chapter=label)
                current_total = progress_display.tasks[progress_task].total or 0
                if current_total == 0:
                    total_chapters = event.get("total")
                    if isinstance(total_chapters, int) and total_chapters > 0:
                        progress_display.update(progress_task, total=total_chapters)
            elif event_type == "chapter_done":
                label = _format_chapter_label(
                    event.get("path"),
                    event.get("index"),
                    event.get("total"),
                )
                if label:
                    progress_display.update(progress_task, chapter=label)
                progress_display.advance(progress_task, 1)
            return
        if event_type == "chapter_start":
            label = _format_chapter_label(
                event.get("path"),
                event.get("index"),
                event.get("total"),
            )
            token_total = event.get("token_total")
            token_text = (
                f" ({token_total} tokens)"
                if isinstance(token_total, int) and token_total > 0
                else ""
            )
            if label:
                print(f"[nk refine] {label}{token_text}")
        elif event_type == "chapter_done":
            label = _format_chapter_label(
                event.get("path"),
                event.get("index"),
                event.get("total"),
            )
            if label:
                status = "updated" if event.get("changed") else "no changes"
                print(f"[nk refine] {label} -> {status}")

    if console.is_terminal:
        progress_display = Progress(
            SpinnerColumn(),
            TextColumn("{task.fields[chapter]}", justify="left"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        )

    def _execute_refine() -> int:
        if chapter_path:
            refined = refine_chapter(
                chapter_path,
                overrides,
                removals=removals,
                progress=_progress_handler,
                chapter_index=1,
                chapter_total=1,
            )
            rel = chapter_path.relative_to(book_dir)
            if refined:
                print(f"Refined {rel}")
            else:
                print(f"No changes required for {rel}")
            return 0
        updated = refine_book(book_dir, overrides, removals=removals, progress=_progress_handler)
        if updated:
            print(f"Refined {updated} chapter(s).")
        else:
            print("No chapters required changes.")
        return 0

    if progress_display:
        with progress_display:
            progress_task = progress_display.add_task(
                "Applying overrides", total=0, chapter=""
            )
            return _execute_refine()
    return _execute_refine()


def _slice_targets_by_index(
    targets: list[TTSTarget], start_index: int | None
) -> list[TTSTarget]:
    if not targets:
        raise ValueError("No chapters available for synthesis.")
    if start_index is None or start_index <= 1:
        return targets
    if start_index > len(targets):
        raise ValueError(
            f"--start-index {start_index} exceeds total chapters ({len(targets)})."
        )
    return targets[start_index - 1 :]


def _run_deps_check() -> int:
    statuses = dependency_statuses()
    all_ok = True
    for status in statuses:
        state = "OK" if status.available else "MISSING"
        print(f"{status.name}: {state}")
        path_text = str(status.path) if status.path else "not detected"
        print(f"  path: {path_text}")
        if status.version:
            print(f"  version: {status.version}")
        if status.detail:
            print(f"  note: {status.detail}")
        print()
        if not status.available:
            all_ok = False
    return 0 if all_ok else 1


def _run_deps_install(args: argparse.Namespace) -> int:
    script_path = getattr(args, "script", None)
    try:
        return install_dependencies(script_path=script_path)
    except DependencyInstallError as exc:
        raise SystemExit(str(exc)) from exc


def _run_deps_uninstall(args: argparse.Namespace) -> int:
    manifest_path = getattr(args, "manifest", None)
    try:
        results = uninstall_dependencies(manifest_path=manifest_path)
    except DependencyUninstallError as exc:
        raise SystemExit(str(exc)) from exc

    exit_code = 0
    for result in results:
        print(f"{result.name}: {result.status}")
        if result.detail:
            print(f"  {result.detail}")
        if result.status in {"error", "unsafe", "nonempty"}:
            exit_code = 1
    return exit_code


def _run_deps(args: argparse.Namespace) -> int:
    command = getattr(args, "command", None) or "check"
    if command == "install":
        return _run_deps_install(args)
    if command == "uninstall":
        return _run_deps_uninstall(args)
    return _run_deps_check()


def _run_cast(args: argparse.Namespace) -> int:
    raise NotImplementedError("LLM-based cast workflow not implemented yet.")


def _run_play(args: argparse.Namespace) -> None:
    root = Path(args.root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Books root not found: {root}")
    cache_dir = Path(args.cache_dir).expanduser().resolve() if args.cache_dir else None
    engine_runtime = (
        Path(args.engine_runtime).expanduser().resolve()
        if args.engine_runtime
        else None
    )

    config = PlayerConfig(
        root=root,
        speaker=args.speaker,
        engine_url=args.engine_url,
        engine_runtime=engine_runtime,
        engine_wait=args.engine_runtime_wait,
        engine_threads=args.engine_threads,
        ffmpeg_path=args.ffmpeg,
        pause=args.pause,
        cache_dir=cache_dir,
        keep_cache=args.keep_cache,
        speed_scale=args.speed,
        pitch_scale=args.pitch,
        intonation_scale=args.intonation,
    )

    reader_process: Process | None = None
    reader_url: str | None = None
    reader_host = args.reader_host or args.host or "127.0.0.1"
    reader_port = args.reader_port
    if not args.no_reader:
        reader_log_config = build_uvicorn_log_config()
        try:
            reader_process = _start_reader_process(
                root,
                reader_host,
                reader_port,
                reader_log_config,
            )
        except Exception as exc:
            raise SystemExit(
                f"Failed to start nk reader companion on {reader_host}:{reader_port}: {exc}"
            ) from exc
        reader_public_ip = _resolve_local_ip(reader_host)
        reader_url = f"http://{reader_public_ip}:{reader_port}/"

    public_ip = _resolve_local_ip(args.host)
    url = f"http://{public_ip}:{args.port}/"
    open_url = url
    if args.open:
        open_host = (
            _preferred_open_host(args.host, public_ip)
            if args.open == _OPEN_AUTO
            else args.open
        )
        open_url = f"http://{open_host}:{args.port}/"
    print(f"Serving nk play from {root}")
    print(f"Player URL: {url}")
    if reader_url:
        print(f"Reader URL: {reader_url}")
    if args.open:
        print(f"Opening browser at {open_url}")
        _open_in_browser(open_url)
    print("Press Ctrl+C to stop.\n")
    log_config = build_uvicorn_log_config()
    try:
        if args.reload:
            _set_player_reload_config(config, reader_url=reader_url)
            uvicorn.run(
                "nk.cli:_player_reload_app",
                host=args.host,
                port=args.port,
                log_level="info",
                log_config=log_config,
                reload=True,
                reload_dirs=_reload_watch_dirs(),
                factory=True,
            )
        else:
            app = create_app(config, reader_url=reader_url)
            uvicorn.run(
                app,
                host=args.host,
                port=args.port,
                log_level="info",
                log_config=log_config,
            )
    finally:
        _stop_reader_process(reader_process)


def _run_read(args: argparse.Namespace) -> int:
    root = Path(args.root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Reader root not found: {root}")
    public_ip = _resolve_local_ip(args.host)
    url = f"http://{public_ip}:{args.port}/"
    open_url = url
    if args.open:
        open_host = (
            _preferred_open_host(args.host, public_ip)
            if args.open == _OPEN_AUTO
            else args.open
        )
        open_url = f"http://{open_host}:{args.port}/"
    print(f"Serving nk read from {root}")
    print(f"Reader URL: {url}")
    if args.open:
        print(f"Opening browser at {open_url}")
        _open_in_browser(open_url)
    print("Press Ctrl+C to stop.\n")
    log_config = build_uvicorn_log_config()
    if args.reload:
        _set_reader_reload_config(root)
        uvicorn.run(
            "nk.cli:_reader_reload_app",
            host=args.host,
            port=args.port,
            log_level="info",
            log_config=log_config,
            reload=True,
            reload_dirs=_reload_watch_dirs(),
            factory=True,
        )
    else:
        app = create_reader_app(root)
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            log_level="info",
            log_config=log_config,
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    if argv and argv[0] in {"tts", "ts"}:
        tts_parser = build_tts_parser()
        tts_args = tts_parser.parse_args(argv[1:])
        return _run_tts(tts_args)
    if argv and argv[0] == "play":
        play_parser = build_play_parser()
        play_args = play_parser.parse_args(argv[1:])
        _run_play(play_args)
        return 0
    if argv and argv[0] == "read":
        read_parser = build_reader_parser()
        read_args = read_parser.parse_args(argv[1:])
        return _run_read(read_args)
    if argv and argv[0] == "dav":
        dav_parser = build_dav_parser()
        dav_args = dav_parser.parse_args(argv[1:])
        return _run_dav(dav_args)
    if argv and argv[0] == "convert":
        convert_parser = build_convert_parser()
        convert_args = convert_parser.parse_args(argv[1:])
        return _run_convert(convert_args)
    if argv and argv[0] == "cast":
        cast_parser = build_cast_parser()
        cast_args = cast_parser.parse_args(argv[1:])
        return _run_cast(cast_args)
    if argv and argv[0] == "deps":
        deps_parser = build_deps_parser()
        deps_args = deps_parser.parse_args(argv[1:])
        return _run_deps(deps_args)
    if argv and argv[0] == "samples":
        samples_parser = build_samples_parser()
        samples_args = samples_parser.parse_args(argv[1:])
        return _run_samples(samples_args)
    if argv and argv[0] == "refine":
        refine_parser = build_refine_parser()
        refine_args = refine_parser.parse_args(argv[1:])
        return _run_refine(refine_args)

    parser = build_parser()
    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)

    inp_path = Path(args.input_path)
    if not inp_path.exists():
        raise FileNotFoundError(f"Input path not found: {inp_path}")

    try:
        backend = NLPBackend()
    except NLPBackendUnavailableError as exc:
        raise SystemExit(str(exc)) from exc

    console = Console()
    if inp_path.is_dir():
        epubs = sorted(p for p in inp_path.iterdir() if p.suffix.lower() == ".epub")
        if not epubs:
            raise FileNotFoundError(f"No .epub files found in directory: {inp_path}")
    else:
        if inp_path.suffix.lower() != ".epub":
            raise ValueError(f"Input must be an .epub file or directory: {inp_path}")
        epubs = [inp_path]

    if console.is_terminal:
        chapter_progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}", justify="left"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=False,
        )
        with chapter_progress:
            for epub_path in epubs:
                _chapterize_epub(
                    epub_path,
                    backend,
                    progress_display=chapter_progress,
                    console=console,
                    transform=args.transform,
                )
    else:
        for epub_path in epubs:
            _chapterize_epub(
                epub_path,
                backend,
                progress_display=None,
                console=console,
                transform=args.transform,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


def _slice_targets_by_index(
    targets: list[TTSTarget], start_index: int | None
) -> list[TTSTarget]:
    if not targets:
        raise ValueError("No chapters available for synthesis.")
    if start_index is None or start_index <= 1:
        return targets
    if start_index > len(targets):
        raise ValueError(
            f"--start-index {start_index} exceeds total chapters ({len(targets)})."
        )
    return targets[start_index - 1 :]


def _run_dav(args: argparse.Namespace) -> int:
    root = Path(args.root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Books root not found: {root}")

    view_root = _prepare_mp3_view(root)
    try:
        from cheroot import wsgi as cheroot_wsgi
        from wsgidav.dc.pam_dc import PAMDomainController
        from wsgidav.fs_dav_provider import FilesystemProvider
        from wsgidav.wsgidav_app import WsgiDAVApp
    except ImportError as exc:
        shutil.rmtree(view_root, ignore_errors=True)
        raise SystemExit(
            f"WsgiDAV is required for `nk dav`. Install dependencies and retry. ({exc})"
        )

    view_root, observer = _prepare_mp3_view(root)
    provider = FilesystemProvider(str(view_root))
    config = {
        "host": args.host,
        "port": args.port,
        "provider_mapping": {"/": provider},
        "simple_dc": {"user_mapping": {"*": True}},
        "http_authenticator": {
            "domain_controller": PAMDomainController,
            "accept_basic": True,
            "accept_digest": False,
            "default_to_digest": False,
            "trusted_auth_header": None,
        },
        "dir_browser": {"enable": True, "davmount": False, "msmount": False},
        "logging": {"enable_loggers": []},
    }
    app = WsgiDAVApp(config)
    server = cheroot_wsgi.Server((args.host, args.port), app)

    public_ip = _resolve_local_ip(args.host)
    url = f"http://{public_ip}:{args.port}/"
    print(f"Serving MP3 view of {root}")
    print(f"Only .mp3 files are exposed. Temporary view: {view_root}")
    print(f"WebDAV URL: {url}")
    print("Authentication: macOS login (PAM). Press Ctrl+C to stop.\n")

    try:
        server_thread = threading.Thread(target=server.start, daemon=True)
        server_thread.start()
        while server_thread.is_alive():
            server_thread.join(timeout=0.5)
    except KeyboardInterrupt:
        print("\nStopping nk dav...")
    finally:
        if hasattr(server, "stop"):
            server.stop()
        if observer is not None:
            observer.stop()
            observer.join()
        shutil.rmtree(view_root, ignore_errors=True)
    return 0


def _prepare_mp3_view(root: Path) -> tuple[Path, PollingObserver | None]:
    temp_root = Path(tempfile.mkdtemp(prefix="nk-dav-"))
    mp3_paths = sorted(p for p in root.rglob("*.mp3") if p.is_file())
    for mp3 in mp3_paths:
        _mirror_mp3(root, temp_root, mp3)
    observer: PollingObserver | None = None
    try:
        observer = PollingObserver()
        handler = _Mp3ViewEventHandler(root, temp_root)
        observer.schedule(handler, str(root), recursive=True)
        observer.start()
    except Exception:
        observer = None
    return temp_root, observer


def _mirror_mp3(source_root: Path, view_root: Path, mp3_path: Path) -> None:
    try:
        rel = mp3_path.relative_to(source_root)
    except ValueError:
        return
    destination = view_root / rel
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination.unlink()
    try:
        os.link(mp3_path, destination)
    except OSError:
        shutil.copy2(mp3_path, destination)


def _remove_from_view(source_root: Path, view_root: Path, path: Path) -> None:
    try:
        rel = path.relative_to(source_root)
    except ValueError:
        return
    target = view_root / rel
    if target.is_dir():
        shutil.rmtree(target, ignore_errors=True)
    else:
        target.unlink(missing_ok=True)


class _Mp3ViewEventHandler(FileSystemEventHandler):
    def __init__(self, source_root: Path, view_root: Path) -> None:
        self.source_root = source_root
        self.view_root = view_root

    def on_created(self, event) -> None:  # type: ignore[override]
        src_path = Path(event.src_path)
        if event.is_directory:
            rel = self._relative(src_path)
            if rel is not None:
                (self.view_root / rel).mkdir(parents=True, exist_ok=True)
            return
        if src_path.suffix.lower() == ".mp3":
            _mirror_mp3(self.source_root, self.view_root, src_path)

    def on_modified(self, event) -> None:  # type: ignore[override]
        src_path = Path(event.src_path)
        if not event.is_directory and src_path.suffix.lower() == ".mp3":
            _mirror_mp3(self.source_root, self.view_root, src_path)

    def on_deleted(self, event) -> None:  # type: ignore[override]
        src_path = Path(event.src_path)
        _remove_from_view(self.source_root, self.view_root, src_path)

    def on_moved(self, event) -> None:  # type: ignore[override]
        src_path = Path(event.src_path)
        dest_path = Path(event.dest_path)
        _remove_from_view(self.source_root, self.view_root, src_path)
        if event.is_directory:
            rel = self._relative(dest_path)
            if rel is not None:
                (self.view_root / rel).mkdir(parents=True, exist_ok=True)
        elif dest_path.suffix.lower() == ".mp3":
            _mirror_mp3(self.source_root, self.view_root, dest_path)

    def _relative(self, path: Path) -> Path | None:
        try:
            return path.relative_to(self.source_root)
        except ValueError:
            return None


def _resolve_local_ip(host: str) -> str:
    if host not in {"", "0.0.0.0"}:
        return host
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"


def _preferred_open_host(requested_host: str, resolved_host: str) -> str:
    """
    Choose a friendly hostname for browser opening:
    - Use requested_host when it is a concrete interface (not 0.0.0.0).
    - Otherwise prefer a fast local hostname (no network lookups), falling back to resolved_host.
    """

    if requested_host not in {"", "0.0.0.0"}:
        return requested_host

    host = _fast_hostname()
    return host or resolved_host


def _fast_hostname() -> str | None:
    candidates = [socket.gethostname()]
    for raw in candidates:
        if not raw:
            continue
        host = raw.split(".")[0].strip()
        if host and not host.lower().startswith("localhost"):
            return host
    return None


def _open_in_browser(url: str) -> None:
    try:
        webbrowser.open(url, new=2)
    except Exception as exc:  # pragma: no cover - best effort
        print(f"[nk] Warn: failed to open a browser at {url}: {exc}", file=sys.stderr)
