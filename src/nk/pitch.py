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

    def with_offsets(self, start: int, end: int) -> "PitchToken":
        token = PitchToken(
            surface=self.surface,
            reading=self.reading,
            accent_type=self.accent_type,
            accent_connection=self.accent_connection,
            pos=self.pos,
            start=start,
            end=end,
            sources=self.sources,
        )
        return token


@dataclass
class ChapterPitchMetadata:
    text_sha1: str | None
    tokens: list[PitchToken]


def serialize_pitch_tokens(tokens: Iterable[PitchToken]) -> list[dict[str, object]]:
    serialized: list[dict[str, object]] = []
    for token in tokens:
        entry: dict[str, object] = {
            "surface": token.surface,
            "reading": token.reading,
            "accent": token.accent_type,
            "connection": token.accent_connection,
            "pos": token.pos,
            "start": token.start,
            "end": token.end,
            "sources": list(token.sources) if token.sources else [],
        }
        serialized.append(entry)
    return serialized


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
        if isinstance(start_val, int) and isinstance(end_val, int):
            start = start_val
            end = end_val
        else:
            start = 0
            end = 0
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
            )
        )
    return tokens
