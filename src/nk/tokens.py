from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from .pitch import PitchToken

__all__ = [
    "ChapterToken",
    "serialize_chapter_tokens",
    "deserialize_chapter_tokens",
    "tokens_to_pitch_tokens",
]


@dataclass
class ChapterToken:
    """
    Canonical representation of a kanji/ruby span inside the original text.

    Each token tracks original offsets, optional transformed offsets (filled
    once the katakana text is generated), and provenance for the selected
    reading so downstream tools can inspect how a pronunciation was chosen.
    """

    surface: str
    start: int
    end: int
    reading: str | None = None
    reading_source: str | None = None
    fallback_reading: str | None = None
    context_prefix: str = ""
    context_suffix: str = ""
    accent_type: int | None = None
    accent_connection: str | None = None
    pos: str | None = None
    transformed_start: int | None = None
    transformed_end: int | None = None

    def to_pitch_token(self) -> PitchToken:
        sources: tuple[str, ...] | None = None
        if self.reading_source:
            sources = (self.reading_source,)
        return PitchToken(
            surface=self.surface,
            reading=self.reading or self.fallback_reading or self.surface,
            accent_type=self.accent_type,
            accent_connection=self.accent_connection,
            pos=self.pos,
            start=self.transformed_start or 0,
            end=self.transformed_end or (self.transformed_start or 0),
            sources=sources,
            original_start=self.start,
            original_end=self.end,
        )


def serialize_chapter_tokens(tokens: Iterable[ChapterToken]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for token in tokens:
        entry: dict[str, object] = {
            "surface": token.surface,
            "start": token.start,
            "end": token.end,
            "reading": token.reading,
            "reading_source": token.reading_source,
            "fallback_reading": token.fallback_reading,
            "context_prefix": token.context_prefix,
            "context_suffix": token.context_suffix,
            "accent": token.accent_type,
            "connection": token.accent_connection,
            "pos": token.pos,
            "transformed_start": token.transformed_start,
            "transformed_end": token.transformed_end,
        }
        payload.append(entry)
    return payload


def deserialize_chapter_tokens(data: Iterable[Mapping[str, object]]) -> list[ChapterToken]:
    tokens: list[ChapterToken] = []
    for entry in data:
        if not isinstance(entry, Mapping):
            continue
        surface = entry.get("surface")
        start = entry.get("start")
        end = entry.get("end")
        if not isinstance(surface, str) or not isinstance(start, int) or not isinstance(end, int):
            continue
        reading = entry.get("reading")
        if not isinstance(reading, str):
            reading = None
        fallback = entry.get("fallback_reading")
        if not isinstance(fallback, str):
            fallback = None
        reading_source = entry.get("reading_source")
        if not isinstance(reading_source, str):
            reading_source = None
        context_prefix = entry.get("context_prefix")
        if not isinstance(context_prefix, str):
            context_prefix = ""
        context_suffix = entry.get("context_suffix")
        if not isinstance(context_suffix, str):
            context_suffix = ""
        accent_val = entry.get("accent")
        accent_type = accent_val if isinstance(accent_val, int) else None
        connection = entry.get("connection")
        if not isinstance(connection, str):
            connection = None
        pos = entry.get("pos")
        if not isinstance(pos, str):
            pos = None
        transformed_start = entry.get("transformed_start")
        if not isinstance(transformed_start, int):
            transformed_start = None
        transformed_end = entry.get("transformed_end")
        if not isinstance(transformed_end, int):
            transformed_end = None
        tokens.append(
            ChapterToken(
                surface=surface,
                start=start,
                end=end,
                reading=reading,
                reading_source=reading_source,
                fallback_reading=fallback,
                context_prefix=context_prefix,
                context_suffix=context_suffix,
                accent_type=accent_type,
                accent_connection=connection,
                pos=pos,
                transformed_start=transformed_start,
                transformed_end=transformed_end,
            )
        )
    return tokens


def tokens_to_pitch_tokens(tokens: Iterable[ChapterToken]) -> list[PitchToken]:
    return [token.to_pitch_token() for token in tokens]
