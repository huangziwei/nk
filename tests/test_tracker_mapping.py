from nk.core import _TransformationTracker, _apply_mapping_to_plain_text


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
