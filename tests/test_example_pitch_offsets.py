from __future__ import annotations

import json
from pathlib import Path

import pytest


EXAMPLE_PITCH_FILES = sorted(Path("example").rglob("*.pitch.json"))
EXAMPLE_TRANSFORMED_TEXTS = [
    path
    for path in Path("example").rglob("*.txt")
    if not path.name.endswith(".original.txt") and not path.name.endswith(".pitch.json") and not path.name.endswith(".token.json")
]
BOOK_TRANSFORMED_TEXTS = [
    path
    for path in Path("books/フィクション/舟を編む").rglob("*.txt")
    if not path.name.endswith(".original.txt") and not path.name.endswith(".pitch.json") and not path.name.endswith(".token.json")
]

if not EXAMPLE_PITCH_FILES:
    pytest.skip("example pitch fixtures not found", allow_module_level=True)


def _assert_index_order(tokens: list[dict], key: str, *, pitch_path: Path) -> None:
    """
    Ensure indexes monotonically increase without overlap for the given key.
    """
    prev_end: int | None = None
    for idx, token in enumerate(tokens):
        start = token["start"].get(key)
        end = token["end"].get(key)
        if start is None or end is None:
            continue
        assert start <= end, f"{pitch_path} token #{idx} has {key} start > end"
        if prev_end is not None:
            assert (
                start >= prev_end
            ), f"{pitch_path} token #{idx} has {key} overlap (start={start}, prev_end={prev_end})"
        prev_end = end


def _contains_cjk(text: str) -> bool:
    for ch in text:
        code = ord(ch)
        if (
            0x4E00 <= code <= 0x9FFF
            or 0x3400 <= code <= 0x4DBF
            or 0x20000 <= code <= 0x2A6DF
            or 0x2A700 <= code <= 0x2B73F
            or 0x2B740 <= code <= 0x2B81F
            or 0x2B820 <= code <= 0x2CEAF
            or 0xF900 <= code <= 0xFAFF
        ):
            return True
    return False


@pytest.mark.parametrize("pitch_path", EXAMPLE_PITCH_FILES)
def test_example_pitch_original_indexes_are_ordered(pitch_path: Path) -> None:
    data = json.loads(pitch_path.read_text())
    _assert_index_order(data["tokens"], "original", pitch_path=pitch_path)


@pytest.mark.parametrize("pitch_path", EXAMPLE_PITCH_FILES)
def test_example_pitch_transformed_indexes_are_ordered(pitch_path: Path) -> None:
    data = json.loads(pitch_path.read_text())
    _assert_index_order(data["tokens"], "transformed", pitch_path=pitch_path)


@pytest.mark.parametrize("token_path", sorted(Path("example").rglob("*.txt.token.json")))
def test_example_tokens_match_text(token_path: Path) -> None:
    text_path = token_path.with_suffix("").with_suffix("")
    original_path = text_path.with_name(text_path.stem + ".original.txt")
    transformed_text = text_path.read_text(encoding="utf-8")
    original_text = original_path.read_text(encoding="utf-8")
    data = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = data.get("tokens", [])
    for idx, token in enumerate(tokens):
        surface = token.get("surface")
        reading = token.get("reading")
        start = token.get("start")
        end = token.get("end")
        t_start = token.get("transformed_start")
        t_end = token.get("transformed_end")
        assert isinstance(surface, str), f"{token_path} token #{idx} missing surface"
        assert isinstance(reading, str), f"{token_path} token #{idx} missing reading"
        assert isinstance(start, int) and isinstance(end, int), f"{token_path} token #{idx} missing original offsets"
        assert isinstance(t_start, int) and isinstance(t_end, int), f"{token_path} token #{idx} missing transformed offsets"
        assert original_text[start:end] == surface, (
            f"{token_path} token #{idx} original slice mismatch "
            f"(expected '{surface}', got '{original_text[start:end]}')"
        )
        assert transformed_text[t_start:t_end] == reading, (
            f"{token_path} token #{idx} transformed slice mismatch "
            f"(expected '{reading}', got '{transformed_text[t_start:t_end]}')"
        )


@pytest.mark.parametrize("token_path", sorted(Path("books/フィクション/舟を編む").rglob("*.txt.token.json")))
def test_book_tokens_match_text(token_path: Path) -> None:
    text_path = token_path.with_suffix("").with_suffix("")
    original_path = text_path.with_name(text_path.stem + ".original.txt")
    transformed_text = text_path.read_text(encoding="utf-8")
    original_text = original_path.read_text(encoding="utf-8")
    data = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = data.get("tokens", [])
    for idx, token in enumerate(tokens):
        surface = token.get("surface")
        reading = token.get("reading")
        start = token.get("start")
        end = token.get("end")
        t_start = token.get("transformed_start")
        t_end = token.get("transformed_end")
        assert isinstance(surface, str), f"{token_path} token #{idx} missing surface"
        assert isinstance(reading, str), f"{token_path} token #{idx} missing reading"
        assert isinstance(start, int) and isinstance(end, int), f"{token_path} token #{idx} missing original offsets"
        assert isinstance(t_start, int) and isinstance(t_end, int), f"{token_path} token #{idx} missing transformed offsets"
        assert original_text[start:end] == surface, (
            f"{token_path} token #{idx} original slice mismatch "
            f"(expected '{surface}', got '{original_text[start:end]}')"
        )
        assert transformed_text[t_start:t_end] == reading, (
            f"{token_path} token #{idx} transformed slice mismatch "
            f"(expected '{reading}', got '{transformed_text[t_start:t_end]}')"
        )


@pytest.mark.parametrize("text_path", EXAMPLE_TRANSFORMED_TEXTS)
def test_example_transformed_text_has_no_kanji(text_path: Path) -> None:
    transformed_text = text_path.read_text(encoding="utf-8")
    assert not _contains_cjk(transformed_text), f"{text_path} still contains kanji"


@pytest.mark.parametrize("text_path", BOOK_TRANSFORMED_TEXTS)
def test_book_transformed_text_has_no_kanji(text_path: Path) -> None:
    transformed_text = text_path.read_text(encoding="utf-8")
    assert not _contains_cjk(transformed_text), f"{text_path} still contains kanji"
