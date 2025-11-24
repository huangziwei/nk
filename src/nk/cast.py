from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .book_io import load_book_metadata
from .chunk_manifest import CHUNK_MANIFEST_SUFFIX


_QUOTE_CHARS = {"「", "『", "“", "”", '"'}
_MALE_MARKERS = {"俺", "おれ", "僕", "ぼく", "わし", "わい", "拙者"}
_FEMALE_MARKERS = {"あたし", "あたい", "わたしよ", "わたくし", "うち", "妾", "あたくし"}


@dataclass
class CastResult:
    manifest_path: Path
    updated_chunks: int
    total_chunks: int


def _load_manifest(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("version") is None or payload.get("chunks") is None:
        return None
    return payload


def _is_dialogue(text: str) -> bool:
    text = text.strip()
    if not text:
        return False
    return text[0] in _QUOTE_CHARS


def _gender_bucket(text: str) -> tuple[str, float]:
    for marker in _FEMALE_MARKERS:
        if marker in text:
            return "female", 0.7
    for marker in _MALE_MARKERS:
        if marker in text:
            return "male", 0.6
    return "character", 0.3


def _select_voice(speaker: str, voice_map: dict[str, int] | None) -> int | None:
    if not voice_map:
        return None
    if speaker in voice_map:
        return voice_map[speaker]
    return voice_map.get("narrator")


def annotate_manifest(
    manifest_path: Path,
    *,
    voice_map: dict[str, int] | None = None,
    force: bool = True,
    window: int = 5,
) -> CastResult:
    payload = _load_manifest(manifest_path)
    if payload is None:
        return CastResult(manifest_path=manifest_path, updated_chunks=0, total_chunks=0)
    chunks = payload.get("chunks") or []
    updated = 0
    total = 0
    working: list[dict[str, object]] = []

    # Pass 1: classify dialogue vs narration and capture explicit gender hints.
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        total += 1
        text = chunk.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        is_dialogue = _is_dialogue(text)
        speaker = "narrator"
        confidence = 1.0
        if is_dialogue:
            speaker, confidence = _gender_bucket(text)
        working.append(
            {
                "chunk": chunk,
                "is_dialogue": is_dialogue,
                "speaker": speaker,
                "confidence": confidence,
            }
        )

    # Pass 2: windowed inheritance for unresolved dialogue.
    for idx, entry in enumerate(working):
        if not entry["is_dialogue"]:
            continue
        if entry["speaker"] != "character":
            continue
        start = max(0, idx - window)
        end = min(len(working), idx + window + 1)
        neighbor_speakers: list[tuple[str, float]] = []
        for j in range(start, end):
            if j == idx:
                continue
            neighbor = working[j]
            if not neighbor["is_dialogue"]:
                continue
            if neighbor["speaker"] in {"male", "female"}:
                neighbor_speakers.append((neighbor["speaker"], neighbor["confidence"]))
        if neighbor_speakers:
            neighbor_speakers.sort(key=lambda x: x[1], reverse=True)
            inherited, conf = neighbor_speakers[0]
            entry["speaker"] = inherited
            entry["confidence"] = min(0.5, conf * 0.8)
        else:
            if voice_map and "male" in voice_map:
                entry["speaker"] = "male"
                entry["confidence"] = 0.2
            elif voice_map and "female" in voice_map:
                entry["speaker"] = "female"
                entry["confidence"] = 0.2
            else:
                entry["speaker"] = "narrator"
                entry["confidence"] = 0.1
        updated += 1

    # Pass 3: write back and assign voices.
    for entry in working:
        chunk = entry["chunk"]
        speaker = entry["speaker"]
        confidence = entry["confidence"]
        chunk["speaker"] = speaker
        chunk["confidence"] = confidence
        chunk["needs_review"] = confidence < 0.5
        voice_id = _select_voice(speaker, voice_map)
        if voice_id is not None:
            chunk["voice"] = voice_id

    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    # Count chunks we actually touched (ones with valid text).
    touched = sum(1 for entry in working if isinstance(entry.get("chunk"), dict))
    return CastResult(manifest_path=manifest_path, updated_chunks=touched, total_chunks=total)


def _manifest_paths(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    return sorted(root.rglob(f"*{CHUNK_MANIFEST_SUFFIX}"))


def cast_manifests(
    target: Path,
    *,
    force: bool = False,
) -> list[CastResult]:
    paths = _manifest_paths(target)
    if not paths:
        return []
    voice_map: dict[str, int] | None = None
    # Load voices from nearest .nk-book.json
    book_dir = target if target.is_dir() else target.parent
    metadata = load_book_metadata(book_dir)
    if metadata and metadata.tts_voices:
        voice_map = {name: voice.speaker for name, voice in metadata.tts_voices.items() if voice.speaker is not None}
    results: list[CastResult] = []
    for path in paths:
        results.append(annotate_manifest(path, voice_map=voice_map, force=force))
    return results


__all__ = ["cast_manifests", "annotate_manifest", "CastResult"]
