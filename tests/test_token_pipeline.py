from __future__ import annotations

from dataclasses import dataclass

from nk.core import _build_chapter_tokens_from_original


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
