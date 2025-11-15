from __future__ import annotations

import argparse
import os
import shutil
import socket
import sys
import tempfile
from importlib import metadata
from pathlib import Path
import threading
from typing import Mapping

import uvicorn
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

import tomllib

from .book_io import (
    BOOK_METADATA_FILENAME,
    M4B_MANIFEST_FILENAME,
    load_book_metadata,
    regenerate_m4b_manifest,
    update_book_tts_defaults,
    write_book_package,
)
from .core import epub_to_chapter_texts, epub_to_txt, get_epub_cover
from .nlp import NLPBackend, NLPBackendUnavailableError
from .tools import DEFAULT_UNIDIC_URL, UniDicInstallError, ensure_unidic_installed, resolve_managed_unidic
from .tts import (
    FFmpegError,
    TTSTarget,
    VoiceVoxError,
    VoiceVoxRuntimeError,
    VoiceVoxUnavailableError,
    _play_chunk_simpleaudio,
    _simpleaudio,
    ensure_dedicated_voicevox_url,
    discover_voicevox_runtime,
    managed_voicevox_runtime,
    resolve_text_targets,
    synthesize_texts_to_mp3,
    set_debug_logging,
)
from .refine import load_override_config, refine_book
from .web import WebConfig, create_app
from .voice_defaults import (
    DEFAULT_INTONATION_SCALE,
    DEFAULT_PITCH_SCALE,
    DEFAULT_SPEAKER_ID,
    DEFAULT_SPEED_SCALE,
)


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
    )
    _add_version_flag(ap)
    ap.add_argument(
        "input_path",
        help="Path to input .epub or a directory containing .epub files",
    )
    ap.add_argument(
        "-o",
        "--output-name",
        help="Optional name for the output .txt (same folder as input)",
    )
    ap.add_argument(
        "-m",
        "--mode",
        choices=["advanced", "fast"],
        default="advanced",
        help=(
            "Propagation strategy: 'advanced' (default) verifies ruby readings with Sudachi and "
            "fills every kanji; 'fast' relies on ruby evidence only."
        ),
    )
    ap.add_argument(
        "--single-file",
        action="store_true",
        help="Emit a single combined .txt (legacy mode). Default outputs per-chapter files.",
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
        "--mode",
        choices=["advanced", "fast"],
        default="advanced",
        help=(
            "Propagation mode used if an EPUB input is provided. "
            "'advanced' (default) verifies rubies via UniDic; 'fast' trusts in-book evidence."
        ),
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
        "--live",
        action="store_true",
        help="Stream synthesized audio chunk-by-chunk instead of writing MP3 files.",
    )
    ap.add_argument(
        "--live-prebuffer",
        type=int,
        default=2,
        help="Number of chunks to buffer before starting live playback (default: 2).",
    )
    ap.add_argument(
        "--live-start",
        type=int,
        help="Chapter index to start live playback from (1-based).",
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


def build_web_parser() -> argparse.ArgumentParser:
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
        "--live-prebuffer",
        type=int,
        default=2,
        help="Chunks to buffer before live playback begins (default: 2).",
    )
    return ap


def build_refine_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Apply custom pitch overrides to a chapterized book.")
    _add_version_flag(ap)
    ap.add_argument("book_dir", help="Path to the chapterized book directory.")
    return ap


def build_tools_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="nk helper utilities")
    _add_version_flag(ap)
    subparsers = ap.add_subparsers(dest="tool_cmd")

    install = subparsers.add_parser(
        "install-unidic",
        help="Download and register UniDic 3.1.1 inside the current virtualenv.",
    )
    install.add_argument(
        "--zip",
        help="Path to a previously downloaded unidic-cwj-3.1.1 zip archive.",
    )
    install.add_argument(
        "--url",
        default=DEFAULT_UNIDIC_URL,
        help="Download URL for the UniDic archive (default: %(default)s).",
    )
    install.add_argument(
        "--force",
        action="store_true",
        help="Reinstall even if the requested version already exists.",
    )

    status = subparsers.add_parser(
        "unidic-status",
        help="Show the currently detected UniDic dictionary path.",
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
    converted = backend.to_reading_text(text)
    print(converted)
    return 0


def _ensure_tts_source_ready(
    input_path: Path,
    *,
    mode: str,
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
            print(f"[nk tts] Chapterizing {input_path.name} (mode={mode})")
        if needs_chapter:
            backend = None
            if mode == "advanced":
                try:
                    backend = NLPBackend()
                except NLPBackendUnavailableError as exc:
                    raise SystemExit(str(exc)) from exc
            chapters = epub_to_chapter_texts(str(input_path), mode=mode, nlp=backend)
            cover = get_epub_cover(str(input_path))
            write_book_package(
                target_dir,
                chapters,
                source_epub=input_path,
                cover_image=cover,
            )
        else:
            regenerate_m4b_manifest(target_dir)
        return target_dir
    return input_path


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
    input_path = _ensure_tts_source_ready(input_path, mode=args.mode)
    defaults_book_dir = input_path if input_path.is_dir() else input_path.parent
    metadata_path = (
        defaults_book_dir / BOOK_METADATA_FILENAME
        if defaults_book_dir.is_dir()
        else None
    )
    metadata_for_defaults = None
    if defaults_book_dir.is_dir():
        metadata_for_defaults = load_book_metadata(defaults_book_dir)
    stored_defaults = metadata_for_defaults.tts_defaults if metadata_for_defaults else None

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

    def _format_setting(name: str, value: float | int | None, source: str | None = None) -> str:
        if value is None:
            return f"{name}=engine default"
        text = _format_value(name, value) or f"{name}={value}"
        if source == "cli":
            return f"{text} [CLI]"
        if source == "saved":
            return f"{text} [saved]"
        return text

    def _format_transition(name: str, previous: float | int | None, new_value: float | int | None) -> str:
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
        print(f"[nk tts] Loaded saved voice defaults from {source_path} ({saved_summary}).")

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
        changed_fields.append(_format_transition("speaker", stored_defaults.speaker, args.speaker))
    if (
        speed_override_set
        and stored_defaults
        and stored_defaults.speed is not None
        and args.speed is not None
        and stored_defaults.speed != args.speed
    ):
        changed_fields.append(_format_transition("speed", stored_defaults.speed, args.speed))
    if (
        pitch_override_set
        and stored_defaults
        and stored_defaults.pitch is not None
        and args.pitch is not None
        and stored_defaults.pitch != args.pitch
    ):
        changed_fields.append(_format_transition("pitch", stored_defaults.pitch, args.pitch))
    if (
        intonation_override_set
        and stored_defaults
        and stored_defaults.intonation is not None
        and args.intonation is not None
        and stored_defaults.intonation != args.intonation
    ):
        changed_fields.append(
            _format_transition("intonation", stored_defaults.intonation, args.intonation)
        )

    if not saved_values_summary and not (
        speaker_override_set or speed_override_set or pitch_override_set or intonation_override_set
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
            if isinstance(value, (int, float)) and key in {"speed", "pitch", "intonation"}
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
            saved_path = metadata_path if metadata_path else defaults_book_dir / BOOK_METADATA_FILENAME
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
            saved_path = metadata_path if metadata_path else defaults_book_dir / BOOK_METADATA_FILENAME
            saved_pairs = ", ".join(
                _format_value(name, remember_updates[name]) or f"{name}={remember_updates[name]}"
                for name in ("speaker", "speed", "pitch", "intonation")
                if name in remember_updates
            )
            print(f"[nk tts] Saved voice overrides to {saved_path} ({saved_pairs}).")

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
            print(f"Skipping {skipped} chapters; starting synthesis at index {args.start_index}.")

    live_mode = bool(args.live)
    total_targets = len(targets)
    printed_progress = {"value": False}

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
            self.overall_task = self.progress.add_task("All chapters", total=total, detail="")

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
            if not self.enabled or bool(event.get("live")) or self.progress is None:
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
                    total = chunk_count if isinstance(chunk_count, int) and chunk_count > 0 else None
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
                        if total is None and isinstance(chunk_count, int) and chunk_count > 0:
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
                            self.progress.update(task_id, completed=completed, detail=detail)
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
                            self.progress.update(task_id, completed=total, detail=detail)
                        else:
                            current = self.progress.tasks[task_id].completed
                            self.progress.update(task_id, completed=current, detail=detail)
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

    progress_handler = _RichProgress(enabled=not live_mode, total=total_targets)
    cancel_event = threading.Event()

    def _fallback_print(event: dict[str, object]) -> None:
        printed_progress["value"] = True
        event_type = event.get("event")
        index = event.get("index")
        total = event.get("total")
        source = event.get("source")
        output = event.get("output")
        chunk_count = event.get("chunk_count")
        live = bool(event.get("live"))
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
            if live:
                status = "live playback done"
                print(f"[{index}/{total}] {source_name} -> {status}", flush=True)
            else:
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
    playback_fn = None
    if live_mode:
        if _simpleaudio is None:
            raise SystemExit(
                "Live playback requires the `simpleaudio` package. Install it with `pip install simpleaudio`."
            )
        playback_fn = _play_chunk_simpleaudio

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
            live_targets = targets
            if live_mode and args.live_start:
                start_idx = max(1, args.live_start)
                if start_idx > len(targets):
                    raise SystemExit(
                        f"--live-start {start_idx} exceeds total targets ({len(targets)})."
                    )
                live_targets = targets[start_idx - 1 :]
                print(
                    f"Skipping {start_idx - 1} chapters; starting live playback at index {start_idx}.",
                    flush=True,
                )
            generated = synthesize_texts_to_mp3(
                live_targets,
                speaker_id=args.speaker,
                base_url=engine_url,
                ffmpeg_path=args.ffmpeg,
                overwrite=args.overwrite,
                timeout=args.timeout,
                post_phoneme_length=max(args.pause, 0.0) if args.pause is not None else None,
                speed_scale=args.speed,
                pitch_scale=args.pitch,
                intonation_scale=args.intonation,
                jobs=args.jobs,
                cache_dir=cache_base,
                keep_cache=args.keep_cache,
                live_playback=live_mode,
                playback_callback=playback_fn,
                live_prebuffer=max(1, args.live_prebuffer),
                progress=_progress_printer,
                cancel_event=cancel_event,
                engine_defaults_callback=_capture_engine_defaults,
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

    if live_mode:
        if not printed_progress["value"]:
            print("No audio played (all targets skipped).")
        return 0

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
    try:
        overrides = load_override_config(book_dir)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not overrides:
        print(f"No overrides found in {book_dir / 'custom_pitch.json'}")
        return 0
    updated = refine_book(book_dir, overrides)
    if updated:
        print(f"Refined {updated} chapter(s).")
    else:
        print("No chapters required changes.")
    return 0


def _slice_targets_by_index(targets: list[TTSTarget], start_index: int | None) -> list[TTSTarget]:
    if not targets:
        raise ValueError("No chapters available for synthesis.")
    if start_index is None or start_index <= 1:
        return targets
    if start_index > len(targets):
        raise ValueError(f"--start-index {start_index} exceeds total chapters ({len(targets)}).")
    return targets[start_index - 1 :]


def _run_tools(args: argparse.Namespace) -> int:
    if not args.tool_cmd:
        raise SystemExit("A tools subcommand is required. Use --help for options.")

    if args.tool_cmd == "install-unidic":
        try:
            status = ensure_unidic_installed(url=args.url, zip_path=args.zip, force=args.force)
        except UniDicInstallError as exc:
            raise SystemExit(str(exc)) from exc
        print(f"UniDic {status.version} installed at {status.path}")
        print("Set NK_UNIDIC_DIR to override or rerun 'nk tools install-unidic' to reinstall.")
        return 0

    if args.tool_cmd == "unidic-status":
        status = resolve_managed_unidic()
        if status.path and (status.path / "dicrc").exists():
            print(f"Managed UniDic path: {status.path}")
            print(f"Version: {status.version or 'unknown'}")
        else:
            print("No managed UniDic installation detected. Use 'nk tools install-unidic'.")
        env_dir = os.environ.get("NK_UNIDIC_DIR")
        if env_dir:
            print(f"NK_UNIDIC_DIR is set to: {env_dir}")
        return 0

    raise SystemExit(f"Unknown tools subcommand: {args.tool_cmd}")


def _run_web(args: argparse.Namespace) -> None:
    root = Path(args.root).expanduser().resolve()
    cache_dir = Path(args.cache_dir).expanduser().resolve() if args.cache_dir else None
    engine_runtime = Path(args.engine_runtime).expanduser().resolve() if args.engine_runtime else None

    config = WebConfig(
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
        live_prebuffer=args.live_prebuffer,
        speed_scale=args.speed,
        pitch_scale=args.pitch,
        intonation_scale=args.intonation,
    )

    app = create_app(config)
    public_ip = _resolve_local_ip(args.host)
    url = f"http://{public_ip}:{args.port}/"
    print(f"Serving nk web from {root}")
    print(f"Web URL: {url}")
    print("Press Ctrl+C to stop.\n")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    if argv and argv[0] in {"tts", "ts"}:
        tts_parser = build_tts_parser()
        tts_args = tts_parser.parse_args(argv[1:])
        return _run_tts(tts_args)
    if argv and argv[0] == "web":
        web_parser = build_web_parser()
        web_args = web_parser.parse_args(argv[1:])
        _run_web(web_args)
        return 0
    if argv and argv[0] == "dav":
        dav_parser = build_dav_parser()
        dav_args = dav_parser.parse_args(argv[1:])
        return _run_dav(dav_args)
    if argv and argv[0] == "convert":
        convert_parser = build_convert_parser()
        convert_args = convert_parser.parse_args(argv[1:])
        return _run_convert(convert_args)
    if argv and argv[0] == "tools":
        tools_parser = build_tools_parser()
        tools_args = tools_parser.parse_args(argv[1:])
        return _run_tools(tools_args)
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
    emit_chapterized = not args.single_file
    if emit_chapterized and args.output_name:
        raise ValueError("Output name can only be used with --single-file.")

    backend = None
    if args.mode == "advanced":
        try:
            backend = NLPBackend()
        except NLPBackendUnavailableError as exc:
            raise SystemExit(str(exc)) from exc

    if inp_path.is_dir():
        if args.output_name:
            raise ValueError("Output name cannot be used when processing a directory.")
        epubs = sorted(p for p in inp_path.iterdir() if p.suffix.lower() == ".epub")
        if not epubs:
            raise FileNotFoundError(f"No .epub files found in directory: {inp_path}")
        for epub_path in epubs:
            if emit_chapterized:
                chapters = epub_to_chapter_texts(str(epub_path), mode=args.mode, nlp=backend)
                output_dir = epub_path.with_suffix("")
                cover = get_epub_cover(str(epub_path))
                write_book_package(
                    output_dir,
                    chapters,
                    source_epub=epub_path,
                    cover_image=cover,
                )
            else:
                txt = epub_to_txt(str(epub_path), mode=args.mode, nlp=backend)
                output_path = epub_path.with_suffix(".txt")
                output_path.write_text(txt, encoding="utf-8")
    else:
        if inp_path.suffix.lower() != ".epub":
            raise ValueError(f"Input must be an .epub file or directory: {inp_path}")

        if emit_chapterized:
            chapters = epub_to_chapter_texts(str(inp_path), mode=args.mode, nlp=backend)
            output_dir = inp_path.with_suffix("")
            cover = get_epub_cover(str(inp_path))
            write_book_package(
                output_dir,
                chapters,
                source_epub=inp_path,
                cover_image=cover,
            )
        else:
            if args.output_name:
                out_name_path = Path(args.output_name)
                if out_name_path.parent not in (Path("."), Path("")):
                    raise ValueError(
                        "Output name must not contain directory components; "
                        "it is saved next to the EPUB."
                    )
                output_path = inp_path.with_name(out_name_path.name)
            else:
                output_path = inp_path.with_suffix(".txt")

            txt = epub_to_txt(str(inp_path), mode=args.mode, nlp=backend)
            output_path.write_text(txt, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
def _slice_targets_by_index(targets: list[TTSTarget], start_index: int | None) -> list[TTSTarget]:
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
        from wsgidav.fs_dav_provider import FilesystemProvider
        from wsgidav.wsgidav_app import WsgiDAVApp
        from wsgidav.dc.pam_dc import PAMDomainController
    except ImportError as exc:
        shutil.rmtree(view_root, ignore_errors=True)
        raise SystemExit(f"WsgiDAV is required for `nk dav`. Install dependencies and retry. ({exc})")

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
