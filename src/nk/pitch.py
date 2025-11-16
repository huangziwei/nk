from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

__all__ = [
    "PitchToken",
    "ChapterPitchMetadata",
    "serialize_pitch_tokens",
    "deserialize_pitch_tokens",
]


@dataclass
class PitchToken:
    surface: str
    reading: str
    accent_type: int | None
    accent_connection: str | None = None
    pos: str | None = None
    start: int = 0
    end: int = 0
    sources: tuple[str, ...] | None = None
    original_start: int | None = None
    original_end: int | None = None

    def with_offsets(
        self,
        start: int,
        end: int,
        original_start: int | None = None,
        original_end: int | None = None,
    ) -> "PitchToken":
        token = PitchToken(
            surface=self.surface,
            reading=self.reading,
            accent_type=self.accent_type,
            accent_connection=self.accent_connection,
            pos=self.pos,
            start=start,
            end=end,
            sources=self.sources,
            original_start=self.original_start if original_start is None else original_start,
            original_end=self.original_end if original_end is None else original_end,
        )
        return token


@dataclass
class ChapterPitchMetadata:
    text_sha1: str | None
    tokens: list[PitchToken]


def _serialize_offset(transformed: int, original: int | None) -> dict[str, int | None]:
    return {
        "transformed": transformed,
        "original": original,
    }


def serialize_pitch_tokens(tokens: Iterable[PitchToken]) -> list[dict[str, object]]:
    serialized: list[dict[str, object]] = []
    for token in tokens:
        entry: dict[str, object] = {
            "surface": token.surface,
            "reading": token.reading,
            "accent": token.accent_type,
            "connection": token.accent_connection,
            "pos": token.pos,
            "start": _serialize_offset(token.start, token.original_start),
            "end": _serialize_offset(token.end, token.original_end),
            "sources": list(token.sources) if token.sources else [],
        }
        serialized.append(entry)
    return serialized


def _parse_offset(value: object) -> tuple[int, int | None]:
    transformed = 0
    original: int | None = None
    if isinstance(value, Mapping):
        transformed_val = value.get("transformed")
        original_val = value.get("original")
        if isinstance(transformed_val, int):
            transformed = transformed_val
        elif isinstance(transformed_val, str) and transformed_val.isdigit():
            transformed = int(transformed_val)
        if isinstance(original_val, int):
            original = original_val
        elif isinstance(original_val, str) and original_val.isdigit():
            original = int(original_val)
    elif isinstance(value, int):
        transformed = value
    elif isinstance(value, str) and value.isdigit():
        transformed = int(value)
    return transformed, original


def deserialize_pitch_tokens(data: Iterable[Mapping[str, object]]) -> list[PitchToken]:
    tokens: list[PitchToken] = []
    for entry in data:
        surface = entry.get("surface")
        reading = entry.get("reading")
        if not isinstance(surface, str) or not isinstance(reading, str):
            continue
        accent_val = entry.get("accent")
        accent_type = None
        if isinstance(accent_val, int):
            accent_type = accent_val
        elif isinstance(accent_val, str) and accent_val.isdigit():
            accent_type = int(accent_val)
        connection = entry.get("connection")
        if not isinstance(connection, str):
            connection = None
        pos = entry.get("pos")
        if not isinstance(pos, str):
            pos = None
        start_val = entry.get("start")
        end_val = entry.get("end")
        start, original_start = _parse_offset(start_val)
        end, original_end = _parse_offset(end_val)
        sources_val = entry.get("sources")
        sources: tuple[str, ...] | None = None
        if isinstance(sources_val, list):
            normalized_sources = [str(item) for item in sources_val if isinstance(item, str) and item]
            if normalized_sources:
                sources = tuple(normalized_sources)
        tokens.append(
            PitchToken(
                surface=surface,
                reading=reading,
                accent_type=accent_type,
                accent_connection=connection,
                pos=pos,
                start=start,
                end=end,
                sources=sources,
                original_start=original_start,
                original_end=original_end,
            )
        )
    return tokens
