from bs4 import BeautifulSoup

from nk.core import (
    _TransformationTracker,
    _apply_mapping_to_plain_text,
    _collapse_ruby_to_readings,
    _replace_outside_ruby_with_readings,
    _strip_html_to_text,
)


def test_plain_text_mapping_skips_existing_wrapped_segments() -> None:
    tracker = _TransformationTracker()
    existing = tracker.wrap("百合", "ユリ", ("ruby",))
    text = f"先{existing}後 百合"
    mapping = {"百合": "ユリ"}
    result = _apply_mapping_to_plain_text(
        text,
        mapping,
        context_rules=None,
        tracker=tracker,
        source_labels={"百合": "propagation"},
    )
    fragment = tracker.extract(result)
    assert fragment.text == "先ユリ後 ユリ"
    assert len(fragment.tokens) == 2
    assert fragment.tokens[0].sources == ("ruby",)
    assert fragment.tokens[1].sources == ("propagation",)


def test_replacement_skips_ruby_bases() -> None:
    html = "<div><ruby><rb>体</rb><rt>カラダ</rt></ruby>体</div>"
    soup = BeautifulSoup(html, "html.parser")
    tracker = _TransformationTracker()
    mapping = {"体": "カラダ"}
    _replace_outside_ruby_with_readings(soup, mapping, tracker=tracker)
    assert "[[NKT" not in str(soup.find("rb"))
    _collapse_ruby_to_readings(soup, tracker=tracker)
    text = _strip_html_to_text(soup)
    fragment = tracker.extract(text)
    ruby_tokens = [token for token in fragment.tokens if token.sources == ("ruby",)]
    assert ruby_tokens and ruby_tokens[0].surface == "体"
