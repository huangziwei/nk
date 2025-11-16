from nk.core import _apply_mapping_to_plain_text


def test_numeric_mapping_skips_longer_numbers() -> None:
    mapping = {"20": "ハタチ"}
    text = "2025年"
    result = _apply_mapping_to_plain_text(text, mapping, context_rules=None, source_labels={"20": "nhk"})
    assert result == "2025年"


def test_numeric_mapping_applies_with_non_digit_boundary() -> None:
    mapping = {"20": "ハタチ"}
    text = "20歳"
    result = _apply_mapping_to_plain_text(text, mapping, context_rules=None, source_labels={"20": "nhk"})
    assert result == "ハタチ歳"


def test_digit_prefixed_base_skips_when_part_of_larger_number() -> None:
    mapping = {"7日": "ナノカ"}
    text = "17日"
    result = _apply_mapping_to_plain_text(text, mapping, context_rules=None, source_labels={"7日": "nhk"})
    assert result == "17日"
