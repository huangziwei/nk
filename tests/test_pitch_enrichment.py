from collections.abc import Iterable

import pytest

from nk.core import _fill_missing_pitch_from_surface, _finalize_segment_text
from nk.pitch import PitchToken


class DummyBackend:
    def __init__(self, responses: dict[str, tuple[str, Iterable[PitchToken]]]):
        self._responses = responses
        self.calls: list[str] = []

    def to_reading_with_pitch(self, surface: str) -> tuple[str, list[PitchToken]]:
        self.calls.append(surface)
        reading, tokens = self._responses.get(surface, ("", []))
        return reading, list(tokens)


def _pitch_token(
    surface: str,
    reading: str,
    accent: int | None,
    connection: str | None = None,
    pos: str | None = None,
) -> PitchToken:
    return PitchToken(
        surface=surface,
        reading=reading,
        accent_type=accent,
        accent_connection=connection,
        pos=pos,
        start=0,
        end=len(reading),
        sources=("unidic",),
    )


def test_fill_missing_pitch_assigns_accents_when_reading_matches() -> None:
    backend = DummyBackend(
        {
            "自分": ("ジブン", [_pitch_token("自分", "ジブン", 2, "C3", "名詞")]),
            "子供": ("コドモ", [_pitch_token("子供", "コドモ", None, None, "名詞")]),
            "別": ("ベツ", [_pitch_token("別", "ベツ", 1, "C1", "名詞")]),
        }
    )
    tokens = [
        PitchToken(surface="自分", reading="ジブン", accent_type=None, sources=("nhk",)),
        PitchToken(surface="子供", reading="コドモ", accent_type=None, sources=("propagation",)),
        PitchToken(surface="別", reading="ベツ", accent_type=None, sources=("ruby",)),
    ]
    _fill_missing_pitch_from_surface(tokens, backend)
    assert tokens[0].accent_type == 2
    assert tokens[0].accent_connection == "C3"
    assert tokens[0].pos == "名詞"
    # Accent unavailable from backend -> stay None
    assert tokens[1].accent_type is None
    # Ruby/propagation tokens should now also get accent when available
    assert tokens[2].accent_type == 1
    assert tokens[2].accent_connection == "C1"
    assert backend.calls == ["自分", "子供", "別"]


def test_fill_missing_pitch_reuses_cached_surface_lookups() -> None:
    backend = DummyBackend(
        {
            "自分": ("ジブン", [_pitch_token("自分", "ジブン", 3, "C2", "名詞")]),
        }
    )
    tokens = [
        PitchToken(surface="自分", reading="ジブン", accent_type=None, sources=("nhk",)),
        PitchToken(surface="自分", reading="ジブン", accent_type=None, sources=("ruby",)),
    ]
    _fill_missing_pitch_from_surface(tokens, backend)
    assert tokens[0].accent_type == 3
    assert tokens[1].accent_type == 3
    assert backend.calls == ["自分"]


def test_fill_missing_pitch_skips_unidic_sources() -> None:
    backend = DummyBackend(
        {
            "既": ("キ", [_pitch_token("既", "キ", 2, "C1", "名詞")]),
        }
    )
    tokens = [
        PitchToken(surface="既", reading="キ", accent_type=None, sources=("unidic",)),
    ]
    _fill_missing_pitch_from_surface(tokens, backend)
    assert tokens[0].accent_type is None
    assert backend.calls == []


def test_finalize_segment_text_raises_on_alignment_failure() -> None:
    class MisalignedBackend:
        def to_reading_with_pitch(self, text: str) -> tuple[str, list[PitchToken]]:
            return "サシ", [
                PitchToken(
                    surface="仮",
                    reading="テン",
                    accent_type=None,
                    start=0,
                    end=1,
                    sources=("unidic",),
                )
            ]

    with pytest.raises(ValueError):
        _finalize_segment_text("仮", MisalignedBackend())
