from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .book_io import LoadedBookMetadata, load_book_metadata


@dataclass(slots=True)
class BookListing:
    path: Path
    metadata: LoadedBookMetadata | None
    author: str | None
    title: str


def list_books_sorted(root: Path) -> list[BookListing]:
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
        book = BookListing(
            path=entry,
            metadata=metadata,
            author=author,
            title=title,
        )
        sort_key = (
            0 if author else 1,
            normalized_author,
            normalized_title,
            entry.name.casefold(),
        )
        entries.append((sort_key, book))
    entries.sort(key=lambda item: item[0])
    return [book for _, book in entries]

