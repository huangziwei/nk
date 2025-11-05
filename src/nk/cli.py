from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path, PurePosixPath

from .core import ChapterText, epub_to_chapter_texts, epub_to_txt
from .nlp import NLPBackend, NLPBackendUnavailableError
from .tts import (
    FFmpegError,
    VoiceVoxError,
    VoiceVoxRuntimeError,
    VoiceVoxUnavailableError,
    discover_voicevox_runtime,
    managed_voicevox_runtime,
    resolve_text_targets,
    synthesize_texts_to_mp3,
)


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
    input_path = Path(args.input_path)
    output_dir = Path(args.output_dir) if args.output_dir else None
    try:
        targets = resolve_text_targets(input_path, output_dir)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    printed_progress = {"value": False}

    def _progress_printer(event: dict[str, object]) -> None:
        printed_progress["value"] = True
        event_type = event.get("event")
        index = event.get("index")
        total = event.get("total")
        source = event.get("source")
        output = event.get("output")
        chunk_count = event.get("chunk_count")
        if event_type == "target_start":
            name = source.name if isinstance(source, Path) else str(source)
            chunk_info = f" ({chunk_count} chunks)" if isinstance(chunk_count, int) and chunk_count > 1 else ""
            print(f"[{index}/{total}] {name}{chunk_info}", flush=True)
        elif event_type == "chunk_start":
            chunk_index = event.get("chunk_index")
            chunk_total = event.get("chunk_count")
            if isinstance(chunk_total, int) and chunk_total > 1:
                print(f"  chunk {chunk_index}/{chunk_total}", flush=True)
        elif event_type == "target_done":
            path = output if isinstance(output, Path) else output
            print(f"  -> {path}", flush=True)
        elif event_type == "target_skipped":
            reason = event.get("reason", "skipped")
            name = source.name if isinstance(source, Path) else str(source)
            print(f"[{index}/{total}] {name} skipped ({reason})", flush=True)

    runtime_hint = args.engine_runtime
    auto_runtime = None
    if not runtime_hint:
        auto_runtime = discover_voicevox_runtime(args.engine_url)

    runtime_path = runtime_hint or auto_runtime

    try:
        with managed_voicevox_runtime(
            runtime_path,
            args.engine_url,
            readiness_timeout=args.engine_runtime_wait,
        ):
            generated = synthesize_texts_to_mp3(
                targets,
                speaker_id=args.speaker,
                base_url=args.engine_url,
                ffmpeg_path=args.ffmpeg,
                overwrite=args.overwrite,
                timeout=args.timeout,
                post_phoneme_length=max(args.pause, 0.0) if args.pause is not None else None,
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

    if not generated:
        print("No audio generated (all input texts were empty).")
    else:
        if not printed_progress["value"]:
            for path in generated:
                print(path)
    return 0


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    if argv and argv[0] in {"tts", "ts"}:
        tts_parser = build_tts_parser()
        tts_args = tts_parser.parse_args(argv[1:])
        return _run_tts(tts_args)

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
