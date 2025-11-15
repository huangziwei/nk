from nk.core import _TransformationTracker, _apply_mapping_to_plain_text


def test_numeric_mapping_skips_longer_numbers() -> None:
    tracker = _TransformationTracker()
    mapping = {"20": "ハタチ"}
    text = "2025年"
    result = _apply_mapping_to_plain_text(
        text,
        mapping,
        context_rules=None,
        tracker=tracker,
        source_labels={"20": "nhk"},
    )
    fragment = tracker.extract(result)
    assert fragment.text == "2025年"
    assert not fragment.tokens


def test_numeric_mapping_applies_with_non_digit_boundary() -> None:
    tracker = _TransformationTracker()
    mapping = {"20": "ハタチ"}
    text = "20歳"
    result = _apply_mapping_to_plain_text(
        text,
        mapping,
        context_rules=None,
        tracker=tracker,
        source_labels={"20": "nhk"},
    )
    fragment = tracker.extract(result)
    assert fragment.text == "ハタチ歳"
    assert len(fragment.tokens) == 1
    assert fragment.tokens[0].reading == "ハタチ"


def test_digit_prefixed_base_skips_when_part_of_larger_number() -> None:
    tracker = _TransformationTracker()
    mapping = {"7日": "ナノカ"}
    text = "17日"
    result = _apply_mapping_to_plain_text(
        text,
        mapping,
        context_rules=None,
        tracker=tracker,
        source_labels={"7日": "nhk"},
    )
    fragment = tracker.extract(result)
    assert fragment.text == "17日"
    assert not fragment.tokens
