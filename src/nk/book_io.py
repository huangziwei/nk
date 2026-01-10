from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Mapping

try:
    from PIL import Image
except Exception:  # pragma: no cover - optional image padding
    Image = None  # type: ignore[assignment]

try:  # pragma: no cover - importlib backport for older Python
    from importlib import resources
except ImportError:  # pragma: no cover
    import importlib_resources as resources  # type: ignore

from .core import ChapterText, CoverImage
from .chunk_manifest import write_chunk_manifests
from .tokens import ChapterToken, deserialize_chapter_tokens, serialize_chapter_tokens

BOOK_METADATA_FILENAME = ".nk-book.json"
M4B_MANIFEST_FILENAME = "m4b.json"
RUBY_EVIDENCE_FILENAME = "ruby_evidence.json"
_SUPPORTED_COVER_EXTS = (".jpg", ".jpeg", ".png")
_CUSTOM_TOKEN_FILENAME = "custom_token.json"
_LEGACY_CUSTOM_PITCH_FILENAME = "custom_pitch.json"
_TOKEN_SUFFIX = ".token.json"
TOKEN_METADATA_VERSION = 2
_TEMPLATE_RESOURCE = "nk.data"
_TEMPLATE_FILENAME = "custom_token_template.json"
_LEGACY_TEMPLATE_PAYLOAD = {
    "overrides": [
        {
            "pattern": "CONTENTS",
            "reading": "コンテンツ",
            "accent": 3,
            "surface": "CONTENTS",
        },
    ]
}


def is_original_text_file(path: Path) -> bool:
    return path.name.endswith(".original.txt")


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
    book_author: str | None
    m4b_manifest_path: Path
    ruby_evidence_path: Path | None = None


@dataclass
class ChapterMetadata:
    index: int | None
    title: str | None
    original_title: str | None


@dataclass
class LoadedBookMetadata:
    title: str | None
    author: str | None
    cover_path: Path | None
    chapters: dict[str, ChapterMetadata]
    tts_defaults: "BookTTSDefaults | None"
    tts_voices: dict[str, "BookTTSDefaults"] | None
    source_epub: str | None = None


@dataclass
class BookTTSDefaults:
    speaker: int | None = None
    speed: float | None = None
    pitch: float | None = None
    intonation: float | None = None

    def as_payload(self) -> dict[str, float | int]:
        payload: dict[str, float | int] = {}
        if self.speaker is not None:
            payload["speaker"] = self.speaker
        if self.speed is not None:
            payload["speed"] = self.speed
        if self.pitch is not None:
            payload["pitch"] = self.pitch
        if self.intonation is not None:
            payload["intonation"] = self.intonation
        return payload

    def is_empty(self) -> bool:
        return not self.as_payload()

    @classmethod
    def from_payload(cls, payload: object) -> "BookTTSDefaults | None":
        if not isinstance(payload, Mapping):
            return None
        speaker = payload.get("speaker")
        speed = payload.get("speed")
        pitch = payload.get("pitch")
        intonation = payload.get("intonation")
        has_value = False
        speaker_value: int | None = None
        speed_value: float | None = None
        pitch_value: float | None = None
        intonation_value: float | None = None
        if isinstance(speaker, int):
            speaker_value = speaker
            has_value = True
        if isinstance(speed, (int, float)):
            speed_value = float(speed)
            has_value = True
        if isinstance(pitch, (int, float)):
            pitch_value = float(pitch)
            has_value = True
        if isinstance(intonation, (int, float)):
            intonation_value = float(intonation)
            has_value = True
        if not has_value:
            return None
        return cls(
            speaker=speaker_value,
            speed=speed_value,
            pitch=pitch_value,
            intonation=intonation_value,
        )


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


def _write_chapter_texts(
    output_dir: Path, chapters: Iterable[ChapterText]
) -> list[ChapterFileRecord]:
    output_dir.mkdir(parents=True, exist_ok=True)
    used_names: set[str] = set()
    records: list[ChapterFileRecord] = []
    for index, chapter in enumerate(chapters):
        basename = _chapter_basename(index, chapter, used_names)
        path = output_dir / f"{basename}.txt"
        path.write_text(chapter.text, encoding="utf-8")
        original_path = output_dir / f"{basename}.original.txt"
        if chapter.original_text is not None:
            original_path.write_text(chapter.original_text, encoding="utf-8")
        else:
            original_path.unlink(missing_ok=True)
        _maybe_write_token_metadata(path, chapter.text, chapter.tokens)
        legacy_partial_path = path.with_name(f"{path.stem}.partial.txt")
        legacy_partial_path.unlink(missing_ok=True)
        _token_metadata_path(legacy_partial_path).unlink(missing_ok=True)
        records.append(ChapterFileRecord(chapter=chapter, path=path, index=index + 1))
    return records


def _token_metadata_path(chapter_path: Path) -> Path:
    return chapter_path.with_name(chapter_path.name + _TOKEN_SUFFIX)


def _maybe_write_token_metadata(
    chapter_path: Path,
    text: str | None,
    tokens: list[ChapterToken] | None,
) -> None:
    token_path = _token_metadata_path(chapter_path)
    if not text or not tokens:
        token_path.unlink(missing_ok=True)
        return
    payload = {
        "version": TOKEN_METADATA_VERSION,
        "text_sha1": hashlib.sha1(text.encode("utf-8")).hexdigest(),
        "tokens": serialize_chapter_tokens(tokens),
    }
    token_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _resolve_book_title(
    chapters: Iterable[ChapterText], output_dir: Path
) -> str | None:
    for chapter in chapters:
        if chapter.book_title:
            return chapter.book_title
    return output_dir.name or None


def _resolve_book_author(chapters: Iterable[ChapterText]) -> str | None:
    for chapter in chapters:
        if chapter.book_author:
            return chapter.book_author
    return None


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


def _write_ruby_evidence(
    output_dir: Path, payload: list[dict[str, object]] | None
) -> Path | None:
    path = output_dir / RUBY_EVIDENCE_FILENAME
    if not payload:
        path.unlink(missing_ok=True)
        return None
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_m4b_manifest(
    output_dir: Path,
    book_title: str | None,
    book_author: str | None,
    records: list[ChapterFileRecord],
    cover_path: Path | None,
) -> Path:
    title = book_title or output_dir.name
    artist = book_author or title
    tracks: list[dict[str, object]] = []
    for record in records:
        mp3_name = record.path.with_suffix(".mp3").name
        chapter_title = (
            record.chapter.original_title or record.chapter.title or record.path.stem
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
        "artist": artist,
        "tracks": tracks,
        "version": 1,
    }
    if cover_path is not None and cover_path.exists():
        payload["cover"] = cover_path.name
    manifest_path = output_dir / M4B_MANIFEST_FILENAME
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
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
                img.resize((1, 1), resample=resample).convert("RGB").getpixel((0, 0))
            )
            canvas = Image.new("RGB", (size, size), dominant)
            offset = ((size - width) // 2, (size - height) // 2)
            canvas.paste(img, offset)
            save_kwargs = (
                {"quality": 92}
                if cover_path.suffix.lower() in {".jpg", ".jpeg"}
                else {}
            )
            canvas.save(cover_path, **save_kwargs)
    except Exception:  # pragma: no cover - best effort padding
        return


def _load_default_override_template() -> dict:
    """
    Load the packaged custom_token template JSON so the default can be edited
    without touching code. Falls back to the legacy hard-coded payload if the
    resource is missing.
    """
    try:
        template_path = resources.files(_TEMPLATE_RESOURCE).joinpath(_TEMPLATE_FILENAME)
        payload = json.loads(template_path.read_text("utf-8"))
        if isinstance(payload, dict):
            return payload
    except (FileNotFoundError, ModuleNotFoundError, OSError, json.JSONDecodeError):
        pass
    return _LEGACY_TEMPLATE_PAYLOAD


def _is_legacy_template(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    overrides = payload.get("overrides")
    if not isinstance(overrides, list) or len(overrides) != 1:
        return False
    entry = overrides[0]
    if not isinstance(entry, dict):
        return False
    return (
        entry.get("pattern") == "CONTENTS"
        and entry.get("reading") == "コンテンツ"
        and entry.get("surface") == "CONTENTS"
    )


def _normalize_overrides(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, dict):
        return []
    overrides = payload.get("overrides")
    if not isinstance(overrides, list):
        return []
    normalized: list[dict[str, object]] = []
    for entry in overrides:
        if isinstance(entry, dict):
            normalized.append(entry)
    return normalized


def _normalize_removals(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, dict):
        return []
    remove = payload.get("remove")
    if remove is None:
        remove = payload.get("removals")
    if not isinstance(remove, list):
        return []
    normalized: list[dict[str, object]] = []
    for entry in remove:
        if isinstance(entry, dict):
            normalized.append(entry)
    return normalized


def _merge_overrides(
    template_overrides: list[dict[str, object]],
    book_overrides: list[dict[str, object]],
) -> list[dict[str, object]]:
    def _key(entry: dict[str, object]) -> tuple[bool, str | None, str | None, str | None]:
        regex = bool(entry.get("regex"))
        pattern = entry.get("pattern") if isinstance(entry.get("pattern"), str) else None
        match_surface = entry.get("match_surface")
        surface = entry.get("surface")
        surface_key = (
            match_surface
            if isinstance(match_surface, str) and match_surface
            else surface
            if isinstance(surface, str)
            else None
        )
        source_val = entry.get("source")
        source = source_val if isinstance(source_val, str) else None
        return (regex, pattern, surface_key, source)

    merged: dict[tuple[bool, str | None, str | None], dict[str, object]] = {}
    for entry in template_overrides:
        if not isinstance(entry, dict):
            continue
        merged[_key(entry)] = entry
    for entry in book_overrides:
        if not isinstance(entry, dict):
            continue
        merged[_key(entry)] = entry  # book-specific rule wins on conflict
    return list(merged.values())


def _merge_removals(
    template_remove: list[dict[str, object]],
    book_remove: list[dict[str, object]],
) -> list[dict[str, object]]:
    def _key(entry: dict[str, object]) -> tuple[str | None, str | None]:
        reading = entry.get("reading") if isinstance(entry.get("reading"), str) else None
        surface = entry.get("surface") if isinstance(entry.get("surface"), str) else None
        return (reading, surface)

    merged: dict[tuple[str | None, str | None], dict[str, object]] = {}
    for entry in template_remove:
        if not isinstance(entry, dict):
            continue
        merged[_key(entry)] = entry
    for entry in book_remove:
        if not isinstance(entry, dict):
            continue
        merged[_key(entry)] = entry
    return list(merged.values())


def _ensure_custom_token_template(output_dir: Path) -> None:
    template_path = output_dir / _CUSTOM_TOKEN_FILENAME
    legacy_path = output_dir / _LEGACY_CUSTOM_PITCH_FILENAME
    if legacy_path.exists() and not template_path.exists():
        migrated = False
        try:
            legacy_path.replace(template_path)
            migrated = True
        except OSError:
            try:
                template_path.write_text(
                    legacy_path.read_text(encoding="utf-8"), encoding="utf-8"
                )
                legacy_path.unlink(missing_ok=True)
                migrated = True
            except OSError:
                pass
        if not migrated:
            return  # failed to migrate; leave legacy file untouched
    template = _load_default_override_template()
    template_overrides = _normalize_overrides(template)
    template_remove = _normalize_removals(template)
    book_overrides: list[dict[str, object]] = []
    book_remove: list[dict[str, object]] = []
    existing_payload: dict[str, object] | None = None
    if template_path.exists():
        try:
            existing_payload = json.loads(template_path.read_text("utf-8"))
        except (OSError, json.JSONDecodeError):
            return  # don't clobber a file the user may want to fix manually
        book_overrides = _normalize_overrides(existing_payload)
        book_remove = _normalize_removals(existing_payload)
        if not book_overrides and not _is_legacy_template(existing_payload):
            book_overrides = []
    merged_overrides = _merge_overrides(template_overrides, book_overrides)
    merged_remove = _merge_removals(template_remove, book_remove)
    merged_payload: dict[str, object] = {"overrides": merged_overrides}
    include_remove = (
        bool(template_remove)
        or bool(book_remove)
        or (existing_payload is not None and isinstance(existing_payload, dict) and "remove" in existing_payload)
    )
    if include_remove:
        merged_payload["remove"] = merged_remove
    if existing_payload is not None:
        existing_norm: dict[str, object] = {"overrides": book_overrides}
        if include_remove:
            existing_norm["remove"] = book_remove
        if merged_payload == existing_norm:
            return  # nothing new to add
    template_path.write_text(
        json.dumps(merged_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _build_metadata_payload(
    book_title: str | None,
    book_author: str | None,
    records: list[ChapterFileRecord],
    *,
    source_epub: Path | None,
    cover_path: Path | None,
    tts_defaults: BookTTSDefaults | None = None,
    tts_voices: dict[str, BookTTSDefaults] | None = None,
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
    if book_author:
        payload["author"] = book_author
    if cover_path is not None:
        payload["cover"] = cover_path.name
    if source_epub is not None:
        payload["epub"] = source_epub.name
    if tts_defaults:
        defaults_payload = tts_defaults.as_payload()
        if defaults_payload:
            payload["tts_defaults"] = defaults_payload
    if tts_voices:
        voices_payload: dict[str, dict[str, float | int]] = {}
        for name, voice in tts_voices.items():
            voice_payload = voice.as_payload()
            if voice_payload:
                voices_payload[name] = voice_payload
        if voices_payload:
            payload["tts_voices"] = voices_payload
    return payload


def write_book_package(
    output_dir: Path,
    chapters: list[ChapterText],
    *,
    source_epub: Path | None = None,
    cover_image: CoverImage | None = None,
    ruby_evidence: list[dict[str, object]] | None = None,
    apply_overrides: bool = True,
) -> BookPackage:
    previous_metadata = load_book_metadata(output_dir)
    records = _write_chapter_texts(output_dir, chapters)
    book_title = _resolve_book_title(chapters, output_dir)
    book_author = _resolve_book_author(chapters)
    cover_path = _write_cover_image(output_dir, cover_image) if cover_image else None
    previous_voices = previous_metadata.tts_voices if previous_metadata else None
    metadata_payload = _build_metadata_payload(
        book_title,
        book_author,
        records,
        source_epub=source_epub,
        cover_path=cover_path,
        tts_defaults=previous_metadata.tts_defaults if previous_metadata else None,
        tts_voices=previous_voices,
    )
    metadata_path = output_dir / BOOK_METADATA_FILENAME
    metadata_path.write_text(
        json.dumps(metadata_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    m4b_manifest_path = _write_m4b_manifest(
        output_dir,
        book_title,
        book_author,
        records,
        cover_path,
    )
    ruby_evidence_path = _write_ruby_evidence(output_dir, ruby_evidence)
    _ensure_custom_token_template(output_dir)
    if apply_overrides:
        from .refine import load_override_config, refine_book

        try:
            overrides = load_override_config(output_dir)
        except ValueError:
            overrides = []
        if overrides:
            try:
                refine_book(output_dir, overrides)
            except ValueError:
                # If overrides are invalid, leave the original text; user can fix and rerun refine.
                pass
    write_chunk_manifests(
        (record.path for record in records),
        default_speaker="narrator",
    )
    return BookPackage(
        output_dir=output_dir,
        chapter_records=records,
        metadata_path=metadata_path,
        cover_path=cover_path,
        book_title=book_title,
        book_author=book_author,
        m4b_manifest_path=m4b_manifest_path,
        ruby_evidence_path=ruby_evidence_path,
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
        txt_files = sorted(
            p
            for p in book_dir.glob("*.txt")
            if not p.name.endswith(".original.txt")
            and not p.name.endswith(".partial.txt")
        )
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
            chapter_meta.original_title or chapter_meta.title or Path(filename).stem
        )
        index = chapter_meta.index
        tracks.append(
            {
                "file": mp3_name,
                "chapter": chapter_title,
                "index": index if index is not None else len(tracks) + 1,
            }
        )
    title = metadata.title or book_dir.name
    artist = metadata.author or title
    payload: dict[str, object] = {
        "name": title,
        "album": title,
        "artist": artist,
        "tracks": tracks,
        "version": 1,
    }
    if cover_path is not None and cover_path.exists():
        payload["cover"] = cover_path.name
    manifest_path = book_dir / M4B_MANIFEST_FILENAME
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
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
    author = payload.get("author")
    cover_name = payload.get("cover")
    cover_path = None
    if isinstance(cover_name, str):
        candidate = book_dir / cover_name
        if candidate.exists():
            ensure_cover_is_square(candidate)
            cover_path = candidate

    epub_name = payload.get("epub")
    if not isinstance(epub_name, str):
        epub_name = None

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
                title=entry.get("title")
                if isinstance(entry.get("title"), str)
                else None,
                original_title=entry.get("original_title")
                if isinstance(entry.get("original_title"), str)
                else None,
            )

    tts_defaults = BookTTSDefaults.from_payload(payload.get("tts_defaults"))

    voices_payload = payload.get("tts_voices")
    tts_voices: dict[str, BookTTSDefaults] | None = None
    if isinstance(voices_payload, dict):
        for key, entry in voices_payload.items():
            if not isinstance(key, str):
                continue
            voice = BookTTSDefaults.from_payload(entry)
            if voice is None:
                continue
            if tts_voices is None:
                tts_voices = {}
            tts_voices[key] = voice

    return LoadedBookMetadata(
        title=title if isinstance(title, str) else None,
        author=author if isinstance(author, str) else None,
        cover_path=cover_path,
        chapters=chapters,
        tts_defaults=tts_defaults,
        tts_voices=tts_voices,
        source_epub=epub_name,
    )


def load_token_metadata(chapter_path: Path) -> ChapterTokenMetadata | None:
    token_path = _token_metadata_path(chapter_path)
    if not token_path.exists():
        return None
    try:
        payload = json.loads(token_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    tokens_payload = payload.get("tokens")
    tokens = (
        deserialize_chapter_tokens(tokens_payload)
        if isinstance(tokens_payload, list)
        else []
    )
    text_sha1 = payload.get("text_sha1")
    if not isinstance(text_sha1, str):
        text_sha1 = None
    return ChapterTokenMetadata(text_sha1=text_sha1, tokens=tokens)


def update_book_tts_defaults(
    book_dir: Path,
    updates: Mapping[str, float | int | None],
) -> bool:
    if not updates:
        return False
    metadata_path = book_dir / BOOK_METADATA_FILENAME
    if not metadata_path.exists():
        return False
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    existing_payload = payload.get("tts_defaults")
    current: dict[str, float | int] = {}
    if isinstance(existing_payload, dict):
        for key, value in existing_payload.items():
            if key in {"speaker", "speed", "pitch", "intonation"} and isinstance(
                value, (int, float)
            ):
                if key == "speaker" and not isinstance(value, int):
                    continue
                current[key] = int(value) if key == "speaker" else float(value)
    changed = False
    for key, value in updates.items():
        if key not in {"speaker", "speed", "pitch", "intonation"}:
            continue
        if value is None:
            if key in current:
                current.pop(key, None)
                changed = True
            continue
        normalized: float | int
        if key == "speaker":
            if not isinstance(value, int):
                continue
            normalized = value
        else:
            normalized = float(value)
        if current.get(key) != normalized:
            current[key] = normalized
            changed = True
    if not changed:
        return False
    if current:
        payload["tts_defaults"] = current
    else:
        payload.pop("tts_defaults", None)
    metadata_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return True


__all__ = [
    "BookPackage",
    "ChapterFileRecord",
    "ChapterMetadata",
    "LoadedBookMetadata",
    "BookTTSDefaults",
    "ChapterTokenMetadata",
    "BOOK_METADATA_FILENAME",
    "M4B_MANIFEST_FILENAME",
    "TOKEN_METADATA_VERSION",
    "is_original_text_file",
    "ensure_cover_is_square",
    "regenerate_m4b_manifest",
    "load_book_metadata",
    "load_token_metadata",
    "update_book_tts_defaults",
    "write_book_package",
]


@dataclass
class ChapterTokenMetadata:
    text_sha1: str | None
    tokens: list[ChapterToken]
