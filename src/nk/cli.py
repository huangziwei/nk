from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path, PurePosixPath

from .core import ChapterText, epub_to_chapter_texts, epub_to_txt
from .nlp import NLPBackend, NLPBackendUnavailableError


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="EPUB → TXT with ruby propagation and base removal.",
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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
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
