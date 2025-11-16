from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "PitchToken",
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

