from __future__ import annotations

import argparse
import re
import shutil
import sys
import unicodedata
from pathlib import Path, PurePosixPath
import threading

import uvicorn
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .core import ChapterText, epub_to_chapter_texts, epub_to_txt
from .nlp import NLPBackend, NLPBackendUnavailableError
from .tts import (
    FFmpegError,
    VoiceVoxError,
    VoiceVoxRuntimeError,
    VoiceVoxUnavailableError,
    _play_chunk_simpleaudio,
    _simpleaudio,
    discover_voicevox_runtime,
    managed_voicevox_runtime,
    resolve_text_targets,
    synthesize_texts_to_mp3,
)
from .web import WebConfig, create_app


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="EPUB → TXT with ruby propagation and base removal. Use `nk tts` for speech.",
    )
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
        "--chapterized",
        action="store_true",
        help="Emit per-chapter .txt files (follows EPUB spine order).",
    )
    return ap


def build_tts_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Synthesize MP3s from .txt files using a running VoiceVox engine.",
    )
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
        default=2,
        help="VoiceVox speaker ID to use (default: 2).",
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
        default=0,
        help="Parallel synthesis workers (default: auto).",
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
    return ap


def build_web_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Serve chapterized text as a browser-based VoiceVox player.",
    )
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
        "--engine-url",
        default="http://127.0.0.1:50021",
        help="Base URL for the VoiceVox engine (default: http://127.0.0.1:50021).",
    )
    ap.add_argument(
        "--engine-runtime",
        help="Path to the VoiceVox runtime executable or its directory.",
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


def _slugify_for_filename(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    cleaned_chars: list[str] = []
    for ch in normalized:
        if ch in {"/", "\\", ":", "*", "?", '"', "<", ">", "|"}:
            cleaned_chars.append("_")
            continue
        if ord(ch) < 32:
            continue
        if ch.isspace():
            cleaned_chars.append("_")
            continue
        cleaned_chars.append(ch)
    slug = "".join(cleaned_chars)
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug[:80]


def _chapter_basename(index: int, chapter: ChapterText, used_names: set[str]) -> str:
    prefix = f"{index + 1:03d}"
    candidates: list[str] = []
    if chapter.title:
        slug = _slugify_for_filename(chapter.title)
        if slug:
            candidates.append(slug)
    source_stem = PurePosixPath(chapter.source).stem
    stem_slug = _slugify_for_filename(source_stem)
    if stem_slug:
        candidates.append(stem_slug)
    for slug in candidates:
        candidate = f"{prefix}_{slug}"
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
    fallback = prefix
    suffix = 1
    candidate = fallback
    while candidate in used_names:
        suffix += 1
        candidate = f"{fallback}_{suffix}"
    used_names.add(candidate)
    return candidate


def _write_chapter_files(output_dir: Path, chapters: list[ChapterText]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    used_names: set[str] = set()
    for index, chapter in enumerate(chapters):
        basename = _chapter_basename(index, chapter, used_names)
        output_path = output_dir / f"{basename}.txt"
        output_path.write_text(chapter.text, encoding="utf-8")


def _run_tts(args: argparse.Namespace) -> int:
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

    input_path = Path(args.input_path)
    output_dir = Path(args.output_dir) if args.output_dir else None
    try:
        targets = resolve_text_targets(input_path, output_dir)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

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
                        if isinstance(output, Path):
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
        with managed_voicevox_runtime(
            runtime_path,
            args.engine_url,
            readiness_timeout=args.engine_runtime_wait,
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
                base_url=args.engine_url,
                ffmpeg_path=args.ffmpeg,
                overwrite=args.overwrite,
                timeout=args.timeout,
                post_phoneme_length=max(args.pause, 0.0) if args.pause is not None else None,
                jobs=args.jobs,
                cache_dir=cache_base,
                keep_cache=args.keep_cache,
                live_playback=live_mode,
                playback_callback=playback_fn,
                live_prebuffer=max(1, args.live_prebuffer),
                progress=_progress_printer,
            )
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
        ffmpeg_path=args.ffmpeg,
        pause=args.pause,
        cache_dir=cache_dir,
        keep_cache=args.keep_cache,
        live_prebuffer=args.live_prebuffer,
    )

    app = create_app(config)
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

    parser = build_parser()
    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)

    inp_path = Path(args.input_path)
    if not inp_path.exists():
        raise FileNotFoundError(f"Input path not found: {inp_path}")
    if args.chapterized and args.output_name:
        raise ValueError("Output name cannot be used with --chapterized.")

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
            if args.chapterized:
                chapters = epub_to_chapter_texts(str(epub_path), mode=args.mode, nlp=backend)
                output_dir = epub_path.with_suffix("")
                _write_chapter_files(output_dir, chapters)
            else:
                txt = epub_to_txt(str(epub_path), mode=args.mode, nlp=backend)
                output_path = epub_path.with_suffix(".txt")
                output_path.write_text(txt, encoding="utf-8")
    else:
        if inp_path.suffix.lower() != ".epub":
            raise ValueError(f"Input must be an .epub file or directory: {inp_path}")

        if args.chapterized:
            chapters = epub_to_chapter_texts(str(inp_path), mode=args.mode, nlp=backend)
            output_dir = inp_path.with_suffix("")
            _write_chapter_files(output_dir, chapters)
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
