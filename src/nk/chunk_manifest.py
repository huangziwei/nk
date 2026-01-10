from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .chunking import ChunkSpan, split_text_on_breaks_with_spans

CHUNK_MANIFEST_VERSION = 1
CHUNK_MANIFEST_SUFFIX = ".tts.json"


@dataclass
class ChunkManifest:
    text_sha1: str
    chunks: list[dict[str, object]]


def chunk_manifest_path(chapter_path: Path) -> Path:
    return chapter_path.with_name(chapter_path.name + CHUNK_MANIFEST_SUFFIX)


def _build_chunk_manifest_payload(
    text: str,
    spans: Iterable[ChunkSpan],
    *,
    default_speaker: str = "narrator",
) -> dict[str, object]:
    chunks: list[dict[str, object]] = []
    for index, span in enumerate(spans, start=1):
        entry: dict[str, object] = {
            "index": index,
            "start": span.start,
            "end": span.end,
            "text": span.text,
            "speaker": default_speaker,
        }
        chunks.append(entry)
    payload: dict[str, object] = {
        "version": CHUNK_MANIFEST_VERSION,
        "text_sha1": hashlib.sha1(text.encode("utf-8")).hexdigest(),
        "chunk_count": len(chunks),
        "chunks": chunks,
    }
    return payload


def write_chunk_manifest(
    chapter_path: Path,
    text: str,
    *,
    default_speaker: str = "narrator",
) -> Path | None:
    manifest_path = chunk_manifest_path(chapter_path)
    cleaned = text.strip()
    spans = split_text_on_breaks_with_spans(cleaned)
    if not spans:
        manifest_path.unlink(missing_ok=True)
        return None
    payload = _build_chunk_manifest_payload(
        cleaned,
        spans,
        default_speaker=default_speaker,
    )
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest_path


def write_chunk_manifest_from_path(
    chapter_path: Path,
    *,
    default_speaker: str = "narrator",
) -> Path | None:
    try:
        text = chapter_path.read_text(encoding="utf-8")
    except OSError:
        return None
    return write_chunk_manifest(
        chapter_path,
        text,
        default_speaker=default_speaker,
    )


def write_chunk_manifests(
    chapter_paths: Iterable[Path],
    *,
    default_speaker: str = "narrator",
) -> list[Path]:
    written: list[Path] = []
    for chapter_path in chapter_paths:
        manifest_path = write_chunk_manifest_from_path(
            chapter_path,
            default_speaker=default_speaker,
        )
        if manifest_path is not None:
            written.append(manifest_path)
    return written


__all__ = [
    "CHUNK_MANIFEST_SUFFIX",
    "CHUNK_MANIFEST_VERSION",
    "ChunkManifest",
    "chunk_manifest_path",
    "write_chunk_manifest",
    "write_chunk_manifest_from_path",
    "write_chunk_manifests",
]
