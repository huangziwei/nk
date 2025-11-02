from __future__ import annotations

import unicodedata
from dataclasses import dataclass

__all__ = [
    "NLPBackend",
    "NLPBackendUnavailableError",
]


class NLPBackendUnavailableError(RuntimeError):
    """Raised when the optional NLP backend cannot be initialized."""


def _is_cjk_char(ch: str) -> bool:
    if not ch:
        return False
    code = ord(ch)
    return (
        0x4E00 <= code <= 0x9FFF
        or 0x3400 <= code <= 0x4DBF
        or 0x20000 <= code <= 0x2A6DF
        or 0x2A700 <= code <= 0x2B73F
        or 0x2B740 <= code <= 0x2B81F
        or 0x2B820 <= code <= 0x2CEAF
        or 0x2CEB0 <= code <= 0x2EBEF
        or 0x30000 <= code <= 0x3134F
        or 0xF900 <= code <= 0xFAFF
        or 0x2F800 <= code <= 0x2FA1F
        or ch in "々〆ヵヶ"
    )


def _hiragana_to_katakana(text: str) -> str:
    result = []
    for ch in text:
        code = ord(ch)
        if 0x3041 <= code <= 0x3096:
            result.append(chr(code + 0x60))
        elif ch == "ゝ":
            result.append("ヽ")
        elif ch == "ゞ":
            result.append("ヾ")
        elif ch == "ゟ":
            result.append("ヿ")
        else:
            result.append(ch)
    return "".join(result)


def _normalize_katakana(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("ヂ", "ジ").replace("ヅ", "ズ")
    text = text.replace("ヮ", "ワ").replace("ヵ", "カ").replace("ヶ", "ケ")
    text = text.replace("ゕ", "カ").replace("ゖ", "ケ")
    small_map = {"ヤ": "ャ", "ユ": "ュ", "ヨ": "ョ"}
    digraph_bases = {
        "キ",
        "ギ",
        "シ",
        "ジ",
        "チ",
        "ヂ",
        "ニ",
        "ヒ",
        "ビ",
        "ピ",
        "ミ",
        "リ",
    }
    chars: list[str] = []
    idx = 0
    while idx < len(text):
        ch = text[idx]
        if idx + 1 < len(text) and ch in digraph_bases:
            nxt = text[idx + 1]
            if nxt in small_map:
                chars.append(ch)
                chars.append(small_map[nxt])
                idx += 2
                continue
        chars.append(ch)
        idx += 1
    return "".join(chars)


@dataclass
class _Token:
    surface: str
    reading: str
    start: int
    end: int


class NLPBackend:
    """Wrapper around SudachiPy (or compatible) for reading verification."""

    def __init__(self) -> None:
        try:
            from sudachipy import dictionary, tokenizer as tk  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional dep
            raise NLPBackendUnavailableError(
                "Advanced mode requires 'sudachipy' and 'sudachidict_core' to be installed."
            ) from exc

        try:
            self._tokenizer = dictionary.Dictionary().create()
        except Exception as exc:  # pragma: no cover - optional dep
            raise NLPBackendUnavailableError(
                "SudachiPy dictionary load failed. Ensure 'sudachidict_core' is installed."
            ) from exc

        self._split_mode = tk.Tokenizer.SplitMode.C

    def reading_variants(self, text: str) -> set[str]:
        tokens = self._tokenize(text)
        if not tokens:
            return set()
        reading = "".join(token.reading for token in tokens if token.reading)
        return {reading}

    def to_reading_text(self, text: str) -> str:
        tokens = self._tokenize(text)
        if not tokens:
            return text
        pieces: list[str] = []
        last = 0
        for token in tokens:
            if token.start > last:
                pieces.append(text[last:token.start])
            if any(_is_cjk_char(ch) for ch in token.surface):
                pieces.append(token.reading or token.surface)
            else:
                pieces.append(token.surface)
            last = token.end
        if last < len(text):
            pieces.append(text[last:])
        return "".join(pieces)

    def _tokenize(self, text: str) -> list[_Token]:
        if not text:
            return []
        try:
            sudachi_tokens = self._tokenizer.tokenize(text, self._split_mode)
        except Exception:  # pragma: no cover - Sudachi errors surface upstream
            return []

        tokens: list[_Token] = []
        running_index = 0
        for tk in sudachi_tokens:
            surface = tk.surface()
            reading_form = tk.reading_form()
            if reading_form == "*":
                reading_form = surface
            reading = _normalize_katakana(_hiragana_to_katakana(reading_form))

            # SudachiPy 0.6 provides begin/end, but fall back to sequential offsets otherwise.
            begin_attr = getattr(tk, "begin", None)
            if callable(begin_attr):
                start = begin_attr()
            else:
                start = running_index

            end_attr = getattr(tk, "end", None)
            if callable(end_attr):
                end = end_attr()
            else:
                end = start + len(surface)
            running_index = end

            tokens.append(
                _Token(
                    surface=surface,
                    reading=reading,
                    start=start,
                    end=end,
                )
            )
        return tokens
