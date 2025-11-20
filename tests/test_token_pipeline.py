from __future__ import annotations

from dataclasses import dataclass

from nk.core import _RubySpan, _build_chapter_tokens_from_original, _render_text_from_tokens
from nk.tokens import ChapterToken


@dataclass
class _StubToken:
    surface: str
    reading: str
    start: int
    end: int
    accent_type: int | None = None
    accent_connection: str | None = None
    pos: str | None = None


class _StubBackend:
    def tokenize(self, text: str) -> list[_StubToken]:
        readings = {
            "馬": "ウマ",
            "締": "シメ",
            "光": "ヒカ",
            "也": "ナリ",
        }
        tokens: list[_StubToken] = []
        for idx, ch in enumerate(text):
            reading = readings.get(ch, ch)
            tokens.append(
                _StubToken(
                    surface=ch,
                    reading=reading,
                    start=idx,
                    end=idx + 1,
                    pos="名詞",
                )
            )
        return tokens


class _GikunBackend(_StubBackend):
    def to_reading_text(self, text: str) -> str:
        mapping = {
            "Aランク": "エーランク",
            "Fランク": "エフランク",
        }
        return mapping.get(text, text)

    def to_reading_with_pitch(self, surface: str):  # type: ignore[override]
        reading = self.to_reading_text(surface)
        token = _StubToken(
            surface=surface,
            reading=reading,
            start=0,
            end=len(surface),
            accent_type=0,
            accent_connection="C2",
            pos="名詞",
        )
        return reading, [token]


def test_propagation_tokens_precede_unidic() -> None:
    backend = _StubBackend()
    text = "馬締光也の顔"
    tokens = _build_chapter_tokens_from_original(
        text,
        backend,
        ruby_spans=[],
        unique_mapping={"馬締光也": "マジメミツヤ", "馬締": "マジメ"},
        common_mapping={},
        unique_sources={"馬締光也": "propagation", "馬締": "propagation"},
        common_sources={},
        context_rules={},
    )
    assert tokens
    assert tokens[0].surface == "馬締光也"
    assert tokens[0].reading == "マジメミツヤ"
    assert tokens[0].reading_source == "propagation"


def test_translation_ruby_preserves_gikun_reading() -> None:
    backend = _GikunBackend()
    text = "AランクとFランク"
    a_start = text.index("Aランク")
    f_start = text.index("Fランク")
    ruby_spans = [
        _RubySpan(start=a_start, end=a_start + len("Aランク"), base="Aランク", reading="化け物"),
        _RubySpan(start=f_start, end=f_start + len("Fランク"), base="Fランク", reading="誰か"),
    ]
    tokens = _build_chapter_tokens_from_original(
        text,
        backend,
        ruby_spans=ruby_spans,
        unique_mapping={},
        common_mapping={},
        unique_sources={},
        common_sources={},
        context_rules={},
    )
    assert tokens
    a_token = next(token for token in tokens if token.surface == "Aランク")
    f_token = next(token for token in tokens if token.surface == "Fランク")
    assert a_token.reading == "化け物"
    assert f_token.reading == "誰か"
    assert a_token.fallback_reading == "エーランク"
    assert f_token.fallback_reading == "エフランク"

    rendered, _ = _render_text_from_tokens(text, tokens, preserve_unambiguous=True)
    assert "化け物" in rendered
    assert "誰か" in rendered


def test_ambiguous_unidic_surfaces_are_forced_to_katakana() -> None:
    text = "守衛さんと守衛室から"
    tokens = [
        ChapterToken(
            surface="守衛",
            start=text.index("守衛"),
            end=text.index("守衛") + 2,
            reading="モリエ",
            reading_source="unidic",
        ),
        ChapterToken(
            surface="守衛",
            start=text.rindex("守衛"),
            end=text.rindex("守衛") + 2,
            reading="シュエイ",
            reading_source="unidic",
        ),
    ]
    rendered, _ = _render_text_from_tokens(text, tokens, preserve_unambiguous=True)
    assert "守衛" not in rendered
    assert "モリエ" in rendered
    assert "シュエイ" in rendered


def test_conflict_between_ruby_and_unidic_blocks_surface_preservation() -> None:
    text = "鳥や虫や蛙の鳴き声と蛙の鳴き声"
    first = text.index("蛙")
    second = text.rindex("蛙")
    tokens = [
        ChapterToken(
            surface="蛙",
            start=first,
            end=first + 1,
            reading="カエル",
            reading_source="ruby",
        ),
        ChapterToken(
            surface="蛙",
            start=second,
            end=second + 1,
            reading="カワズ",
            reading_source="unidic",
        ),
    ]
    rendered, _ = _render_text_from_tokens(text, tokens, preserve_unambiguous=True)
    assert "蛙" not in rendered
    assert "カエル" in rendered
    assert "カワズ" in rendered
