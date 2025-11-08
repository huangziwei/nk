from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable

try:
    from PIL import Image
except Exception:  # pragma: no cover - optional image padding
    Image = None  # type: ignore[assignment]

from .core import ChapterText, CoverImage

BOOK_METADATA_FILENAME = ".nk-book.json"
M4B_MANIFEST_FILENAME = "m4b.json"
_SUPPORTED_COVER_EXTS = (".jpg", ".jpeg", ".png")


@dataclass
class ChapterFileRecord:
    chapter: ChapterText
    path: Path
    index: int


@dataclass
class BookPackage:
    output_dir: Path
    chapter_records: list[ChapterFileRecord]
    metadata_path: Path
    cover_path: Path | None
    book_title: str | None
    m4b_manifest_path: Path


@dataclass
class ChapterMetadata:
    index: int | None
    title: str | None
    original_title: str | None


@dataclass
class LoadedBookMetadata:
    title: str | None
    cover_path: Path | None
    chapters: dict[str, ChapterMetadata]


def _slugify_for_filename(text: str) -> str:
    cleaned_chars: list[str] = []
    for ch in text.strip():
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
    title_source = chapter.original_title or chapter.title
    if title_source:
        slug = _slugify_for_filename(title_source)
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


def _write_chapter_texts(output_dir: Path, chapters: Iterable[ChapterText]) -> list[ChapterFileRecord]:
    output_dir.mkdir(parents=True, exist_ok=True)
    used_names: set[str] = set()
    records: list[ChapterFileRecord] = []
    for index, chapter in enumerate(chapters):
        basename = _chapter_basename(index, chapter, used_names)
        path = output_dir / f"{basename}.txt"
        path.write_text(chapter.text, encoding="utf-8")
        records.append(ChapterFileRecord(chapter=chapter, path=path, index=index + 1))
    return records


def _resolve_book_title(chapters: Iterable[ChapterText], output_dir: Path) -> str | None:
    for chapter in chapters:
        if chapter.book_title:
            return chapter.book_title
    return output_dir.name or None


def _cover_extension(cover: CoverImage) -> str | None:
    suffix = PurePosixPath(cover.path).suffix.lower()
    if suffix in _SUPPORTED_COVER_EXTS:
        return ".jpg" if suffix == ".jpeg" else suffix
    media_type = (cover.media_type or "").lower()
    if media_type in {"image/jpeg", "image/jpg"}:
        return ".jpg"
    if media_type == "image/png":
        return ".png"
    return None


def _write_cover_image(output_dir: Path, cover: CoverImage) -> Path | None:
    extension = _cover_extension(cover)
    if extension is None:
        return None
    normalized_ext = ".jpg" if extension == ".jpeg" else extension
    for ext in _SUPPORTED_COVER_EXTS:
        (output_dir / f"cover{ext}").unlink(missing_ok=True)
    cover_path = output_dir / f"cover{normalized_ext}"
    cover_path.write_bytes(cover.data)
    ensure_cover_is_square(cover_path)
    return cover_path


def _write_m4b_manifest(
    output_dir: Path,
    book_title: str | None,
    records: list[ChapterFileRecord],
    cover_path: Path | None,
) -> Path:
    title = book_title or output_dir.name
    tracks: list[dict[str, object]] = []
    for record in records:
        mp3_name = record.path.with_suffix(".mp3").name
        chapter_title = (
            record.chapter.original_title
            or record.chapter.title
            or record.path.stem
        )
        tracks.append(
            {
                "file": mp3_name,
                "chapter": chapter_title,
                "index": record.index,
            }
        )
    payload: dict[str, object] = {
        "name": title,
        "album": title,
        "artist": title,
        "tracks": tracks,
        "version": 1,
    }
    if cover_path is not None and cover_path.exists():
        payload["cover"] = cover_path.name
    manifest_path = output_dir / M4B_MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def ensure_cover_is_square(cover_path: Path) -> None:
    if Image is None:
        return
    try:
        with Image.open(cover_path) as img:
            img = img.convert("RGB")
            width, height = img.size
            if width == height or width == 0 or height == 0:
                return
            size = max(width, height)
            try:
                resample = Image.Resampling.LANCZOS  # type: ignore[attr-defined]
            except AttributeError:  # pragma: no cover - older Pillow
                resample = Image.LANCZOS if hasattr(Image, "LANCZOS") else Image.BICUBIC
            dominant = (
                img.resize((1, 1), resample=resample)
                .convert("RGB")
                .getpixel((0, 0))
            )
            canvas = Image.new("RGB", (size, size), dominant)
            offset = ((size - width) // 2, (size - height) // 2)
            canvas.paste(img, offset)
            save_kwargs = {"quality": 92} if cover_path.suffix.lower() in {".jpg", ".jpeg"} else {}
            canvas.save(cover_path, **save_kwargs)
    except Exception:  # pragma: no cover - best effort padding
        return


def _build_metadata_payload(
    book_title: str | None,
    records: list[ChapterFileRecord],
    *,
    source_epub: Path | None,
    cover_path: Path | None,
) -> dict:
    chapters_payload = []
    for record in records:
        chapters_payload.append(
            {
                "index": record.index,
                "file": record.path.name,
                "title": record.chapter.title,
                "original_title": record.chapter.original_title,
                "source": record.chapter.source,
            }
        )
    payload: dict[str, object] = {
        "version": 1,
        "title": book_title,
        "chapters": chapters_payload,
    }
    if cover_path is not None:
        payload["cover"] = cover_path.name
    if source_epub is not None:
        payload["epub"] = source_epub.name
    return payload


def write_book_package(
    output_dir: Path,
    chapters: list[ChapterText],
    *,
    source_epub: Path | None = None,
    cover_image: CoverImage | None = None,
) -> BookPackage:
    records = _write_chapter_texts(output_dir, chapters)
    book_title = _resolve_book_title(chapters, output_dir)
    cover_path = _write_cover_image(output_dir, cover_image) if cover_image else None
    metadata_payload = _build_metadata_payload(
        book_title,
        records,
        source_epub=source_epub,
        cover_path=cover_path,
    )
    metadata_path = output_dir / BOOK_METADATA_FILENAME
    metadata_path.write_text(
        json.dumps(metadata_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    m4b_manifest_path = _write_m4b_manifest(output_dir, book_title, records, cover_path)
    return BookPackage(
        output_dir=output_dir,
        chapter_records=records,
        metadata_path=metadata_path,
        cover_path=cover_path,
        book_title=book_title,
        m4b_manifest_path=m4b_manifest_path,
    )


def regenerate_m4b_manifest(
    book_dir: Path,
    metadata: LoadedBookMetadata | None = None,
) -> Path | None:
    metadata = metadata or load_book_metadata(book_dir)
    if metadata is None:
        return None
    cover_path = metadata.cover_path
    if cover_path is None:
        for ext in _SUPPORTED_COVER_EXTS:
            candidate = book_dir / f"cover{ext}"
            if candidate.exists():
                cover_path = candidate
                break
    if cover_path is not None:
        ensure_cover_is_square(cover_path)
    chapters = sorted(
        metadata.chapters.items(),
        key=lambda item: (
            item[1].index if item[1].index is not None else 10**6,
            item[0],
        ),
    )
    if not chapters:
        txt_files = sorted(book_dir.glob("*.txt"))
        for idx, txt in enumerate(txt_files, start=1):
            metadata.chapters.setdefault(
                txt.name,
                ChapterMetadata(index=idx, title=txt.stem, original_title=None),
            )
        chapters = sorted(
            metadata.chapters.items(),
            key=lambda item: (
                item[1].index if item[1].index is not None else 10**6,
                item[0],
            ),
        )
    tracks: list[dict[str, object]] = []
    for filename, chapter_meta in chapters:
        mp3_name = Path(filename).with_suffix(".mp3").name
        chapter_title = (
            chapter_meta.original_title
            or chapter_meta.title
            or Path(filename).stem
        )
        index = chapter_meta.index
        tracks.append(
            {
                "file": mp3_name,
                "chapter": chapter_title,
                "index": index if index is not None else len(tracks) + 1,
            }
        )
    payload: dict[str, object] = {
        "name": metadata.title or book_dir.name,
        "album": metadata.title or book_dir.name,
        "artist": metadata.title or book_dir.name,
        "tracks": tracks,
        "version": 1,
    }
    if cover_path is not None and cover_path.exists():
        payload["cover"] = cover_path.name
    manifest_path = book_dir / M4B_MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def load_book_metadata(book_dir: Path) -> LoadedBookMetadata | None:
    metadata_path = book_dir / BOOK_METADATA_FILENAME
    if not metadata_path.exists():
        return None
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None

    title = payload.get("title")
    cover_name = payload.get("cover")
    cover_path = None
    if isinstance(cover_name, str):
        candidate = book_dir / cover_name
        if candidate.exists():
            ensure_cover_is_square(candidate)
            cover_path = candidate

    chapters_payload = payload.get("chapters")
    chapters: dict[str, ChapterMetadata] = {}
    if isinstance(chapters_payload, list):
        for entry in chapters_payload:
            if not isinstance(entry, dict):
                continue
            file_name = entry.get("file")
            if not isinstance(file_name, str):
                continue
            index_val = entry.get("index")
            index = None
            if isinstance(index_val, int):
                index = index_val
            elif isinstance(index_val, str) and index_val.isdigit():
                index = int(index_val)
            chapters[file_name] = ChapterMetadata(
                index=index,
                title=entry.get("title") if isinstance(entry.get("title"), str) else None,
                original_title=entry.get("original_title")
                if isinstance(entry.get("original_title"), str)
                else None,
            )

    return LoadedBookMetadata(
        title=title if isinstance(title, str) else None,
        cover_path=cover_path,
        chapters=chapters,
    )


__all__ = [
    "BookPackage",
    "ChapterFileRecord",
    "ChapterMetadata",
    "LoadedBookMetadata",
    "BOOK_METADATA_FILENAME",
    "M4B_MANIFEST_FILENAME",
    "ensure_cover_is_square",
    "regenerate_m4b_manifest",
    "load_book_metadata",
    "write_book_package",
]
