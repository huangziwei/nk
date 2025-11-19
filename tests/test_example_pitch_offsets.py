from __future__ import annotations

import json
from pathlib import Path

import pytest

EXAMPLE_TOKEN_FILES = sorted(Path("example").rglob("*.txt.token.json"))
if not EXAMPLE_TOKEN_FILES:
    pytest.skip("example token fixtures not found", allow_module_level=True)


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


@pytest.mark.parametrize("token_path", EXAMPLE_TOKEN_FILES)
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
        assert isinstance(start, int) and isinstance(end, int), (
            f"{token_path} token #{idx} missing original offsets"
        )
        assert isinstance(t_start, int) and isinstance(t_end, int), (
            f"{token_path} token #{idx} missing transformed offsets"
        )
        assert original_text[start:end] == surface, (
            f"{token_path} token #{idx} original slice mismatch "
            f"(expected '{surface}', got '{original_text[start:end]}')"
        )
        transformed_slice = transformed_text[t_start:t_end]
        if transformed_slice != reading:
            assert transformed_slice == surface, (
                f"{token_path} token #{idx} transformed slice mismatch "
                f"(expected '{reading}' or surface '{surface}', got '{transformed_slice}')"
            )


@pytest.mark.parametrize(
    "token_path",
    sorted(Path("books/フィクション/大衆文学/舟を編む").rglob("*.txt.token.json")),
)
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
        assert isinstance(start, int) and isinstance(end, int), (
            f"{token_path} token #{idx} missing original offsets"
        )
        assert isinstance(t_start, int) and isinstance(t_end, int), (
            f"{token_path} token #{idx} missing transformed offsets"
        )
        assert original_text[start:end] == surface, (
            f"{token_path} token #{idx} original slice mismatch "
            f"(expected '{surface}', got '{original_text[start:end]}')"
        )
        transformed_slice = transformed_text[t_start:t_end]
        if transformed_slice != reading:
            assert transformed_slice == surface, (
                f"{token_path} token #{idx} transformed slice mismatch "
                f"(expected '{reading}' or surface '{surface}', got '{transformed_slice}')"
            )


@pytest.mark.parametrize("token_path", EXAMPLE_TOKEN_FILES)
def test_example_readings_are_kana(token_path: Path) -> None:
    data = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = data.get("tokens", [])
    for idx, token in enumerate(tokens):
        reading = token.get("reading")
        assert isinstance(reading, str), f"{token_path} token #{idx} missing reading"
        assert not _contains_cjk(reading), (
            f"{token_path} token #{idx} reading '{reading}' still contains kanji"
        )


@pytest.mark.parametrize(
    "token_path", sorted(Path("books/フィクション/大衆文学/舟を編む").rglob("*.txt.token.json"))
)
def test_book_readings_are_kana(token_path: Path) -> None:
    data = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = data.get("tokens", [])
    for idx, token in enumerate(tokens):
        reading = token.get("reading")
        assert isinstance(reading, str), f"{token_path} token #{idx} missing reading"
        assert not _contains_cjk(reading), (
            f"{token_path} token #{idx} reading '{reading}' still contains kanji"
        )
