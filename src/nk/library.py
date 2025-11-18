from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .book_io import LoadedBookMetadata, load_book_metadata

BOOKMARKS_FILENAME = ".nk-player-bookmarks.json"


@dataclass(slots=True)
class BookListing:
    path: Path
    metadata: LoadedBookMetadata | None
    author: str | None
    title: str
    modified: float
    last_played: float


def list_books_sorted(root: Path, mode: str = "author") -> list[BookListing]:
    normalized_mode = mode.lower().strip()
    if normalized_mode not in {"author", "recent", "played"}:
        normalized_mode = "author"
    entries: list[tuple[tuple[object, ...], BookListing]] = []
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        metadata = load_book_metadata(entry)
        author = (metadata.author.strip() if metadata and metadata.author else None)
        title = (
            metadata.title.strip()
            if metadata and metadata.title
            else entry.name
        )
        normalized_author = author.casefold() if author else ""
        normalized_title = title.casefold()
        try:
            stat = entry.stat()
            modified = getattr(stat, "st_ctime", stat.st_mtime)
        except OSError:
            modified = 0.0
        last_played = _last_played_timestamp(entry)
        book = BookListing(
            path=entry,
            metadata=metadata,
            author=author,
            title=title,
            modified=modified,
            last_played=last_played,
        )
        if normalized_mode == "recent":
            sort_key = (
                -modified,
                0 if author else 1,
                normalized_author,
                normalized_title,
                entry.name.casefold(),
            )
        elif normalized_mode == "played":
            has_played = last_played > 0
            sort_key = (
                0 if has_played else 1,
                -last_played if has_played else 0,
                -modified,
                0 if author else 1,
                normalized_author,
                normalized_title,
                entry.name.casefold(),
            )
        else:
            sort_key = (
                0 if author else 1,
                normalized_author,
                normalized_title,
                entry.name.casefold(),
            )
        entries.append((sort_key, book))
    entries.sort(key=lambda item: item[0])
    return [book for _, book in entries]


def _last_played_timestamp(book_dir: Path) -> float:
    bookmark_path = book_dir / BOOKMARKS_FILENAME
    try:
        raw = json.loads(bookmark_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0.0
    if not isinstance(raw, dict):
        return 0.0
    entry = raw.get("last_played")
    if isinstance(entry, dict):
        updated = entry.get("updated_at")
        if isinstance(updated, (int, float)):
            return float(updated)
        time_value = entry.get("time")
        if isinstance(time_value, (int, float)):
            return float(time_value)
    return 0.0
