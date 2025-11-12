
from __future__ import annotations

from bs4 import BeautifulSoup

from nk.core import _collect_reading_counts_from_soup, _select_reading_mapping

import pytest


class _MockNLP:
    def reading_variants(self, text: str) -> set[str]:
        # Pretend the dictionary prefers オンスイ for 温水.
        if text == "温水":
            return {"オンスイ"}
        return set()


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def test_adjacent_single_kanji_ruby_merges_into_compound() -> None:
    html = """
    <p>
      <ruby><rb>温</rb><rt>ぬく</rt></ruby><ruby><rb>水</rb><rt>みず</rt></ruby>がいる。
      <ruby><rb>温</rb><rt>ぬく</rt></ruby><ruby><rb>水</rb><rt>みず</rt></ruby>も来た。
      <ruby><rb>温</rb><rt>ぬく</rt></ruby><ruby><rb>水</rb><rt>みず</rt></ruby>だった。
    </p>
    """
    soup = _soup(html)
    accumulators = _collect_reading_counts_from_soup(soup)
    combined = accumulators.get("温水")
    assert combined is not None
    # Three occurrences of the compound name should be recorded.
    assert combined.total == 3
    assert combined.counts["ヌクミズ"] == 3
    # Ensure downstream selection keeps the propagated reading even when the
    # dictionary says otherwise.
    unique, _ = _select_reading_mapping(accumulators, mode="advanced", nlp=_MockNLP())
    assert unique.get("温水") == "ヌクミズ"


def test_nlp_backend_normalizes_cross_token_small_kana() -> None:
    pytest.importorskip("fugashi")
    from nk.nlp import NLPBackend

    backend = NLPBackend()
    reading = backend.to_reading_text("行事予算")
    assert "ジヨ" not in reading
    assert "ギョウジョサン" in reading


def test_nlp_backend_prefers_hoka_for_independent_ta() -> None:
    pytest.importorskip("fugashi")
    from nk.nlp import NLPBackend

    backend = NLPBackend()
    assert backend.to_reading_text("他は") == "ホカは"
    assert backend.to_reading_text("他にも") == "ホカにも"
    assert backend.to_reading_text("他、質問は？") == "ホカ、シツモンは?"
    assert backend.to_reading_text("その他") == "そのホカ"
    assert backend.to_reading_text("他人") == "タニン"
    assert backend.to_reading_text("他界") == "タカイ"


def test_nlp_backend_handles_parent_titles_and_oyaji() -> None:
    pytest.importorskip("fugashi")
    from nk.nlp import NLPBackend

    backend = NLPBackend()
    assert backend.to_reading_text("父上") == "チチウエ"
    assert backend.to_reading_text("お父上") == "おチチウエ"
    assert backend.to_reading_text("お父上様") == "おチチウエサマ"
    assert backend.to_reading_text("父親") == "オヤジ"
    assert backend.to_reading_text("父親は") == "オヤジは"
    assert backend.to_reading_text("母上") == "ハハウエ"
    assert backend.to_reading_text("お母上") == "おハハウエ"
    assert backend.to_reading_text("お母上様") == "おハハウエサマ"
