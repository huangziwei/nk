
from __future__ import annotations

from bs4 import BeautifulSoup
from types import SimpleNamespace

from nk.core import _collect_reading_counts_from_soup, _select_reading_mapping

import pytest


class _MockNLP:
    def reading_variants(self, text: str) -> set[str]:
        # Pretend the dictionary prefers オンスイ for 温水.
        if text == "温水":
            return {"オンスイ"}
        return set()

    def to_reading_text(self, text: str) -> str:
        return text


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
    unique, _, _ = _select_reading_mapping(accumulators, mode="advanced", nlp=_MockNLP())
    assert unique.get("温水") == "ヌクミズ"


def test_name_ruby_with_dict_mismatch_is_preserved() -> None:
    class _NameMock:
        def reading_variants(self, text: str) -> set[str]:
            if text == "馬締":
                return {"ウマシメ"}
            return set()

        def to_reading_text(self, text: str) -> str:
            return text

    html = """
    <p>
      <ruby><rb>馬締</rb><rt>まじめ</rt></ruby>が来た。
    </p>
    """
    soup = _soup(html)
    accumulators = _collect_reading_counts_from_soup(soup)
    unique, _, _ = _select_reading_mapping(accumulators, mode="advanced", nlp=_NameMock())
    assert unique.get("馬締") == "マジメ"


def test_small_kana_alignment_prefers_dictionary_variant() -> None:
    class _SmallMock:
        def reading_variants(self, text: str) -> set[str]:
            if text == "宮崎":
                return {"ミャザキ"}
            return set()

        def to_reading_text(self, text: str) -> str:
            return text

    html = """
    <p>
      <ruby><rb>宮崎</rb><rt>みやざき</rt></ruby>に会った。
    </p>
    """
    soup = _soup(html)
    accumulators = _collect_reading_counts_from_soup(soup)
    unique, _, _ = _select_reading_mapping(accumulators, mode="advanced", nlp=_SmallMock())
    assert unique.get("宮崎") == "ミャザキ"


def test_suffix_context_allows_nlp_confirmation() -> None:
    class _SuffixMock:
        def reading_variants(self, text: str) -> set[str]:
            if text == "東京":
                return {"トウキョウ"}
            return set()

        def to_reading_text(self, text: str) -> str:
            if text == "東京は":
                return "アズマキョウハ"
            return text

    html = """
    <p>
      <ruby><rb>東</rb><rt>あずま</rt></ruby><ruby><rb>京</rb><rt>きょう</rt></ruby>は。
    </p>
    """
    soup = _soup(html)
    accumulators = _collect_reading_counts_from_soup(soup)
    unique, _, _ = _select_reading_mapping(accumulators, mode="advanced", nlp=_SuffixMock())
    assert unique.get("東京") == "アズマキョウ"


def test_nlp_backend_preserves_and_reports_kana_forms() -> None:
    pytest.importorskip("fugashi")
    from nk.nlp import NLPBackend

    backend = NLPBackend()
    assert backend.to_reading_text("宮") == "ミヤ"
    reading = backend.to_reading_text("行事予算")
    assert "ギョウジヨサン" in reading


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


def test_cli_convert_command_outputs_kana(capsys) -> None:
    pytest.importorskip("fugashi")
    from nk.cli import _run_convert

    args = SimpleNamespace(text=["他は"])
    assert _run_convert(args) == 0
    captured = capsys.readouterr()
    assert "ホカは" in captured.out
