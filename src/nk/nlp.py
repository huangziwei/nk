from __future__ import annotations

import unicodedata
import warnings
from dataclasses import dataclass
from typing import Callable, Optional

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


def _contains_cjk(text: str) -> bool:
    return any(_is_cjk_char(ch) for ch in text)


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
    """Fugashi-based backend for reading verification and kana conversion."""

    def __init__(self) -> None:
        try:
            from fugashi import Tagger  # type: ignore
        except ImportError as exc:
            raise NLPBackendUnavailableError(
                "Advanced mode requires 'fugashi' (MeCab) to be installed."
            ) from exc

        self._tagger = Tagger()
        self._kakasi_converter = self._build_kakasi_converter()

    def reading_variants(self, text: str) -> set[str]:
        tokens = self._tokenize(text)
        if not tokens:
            return set()
        pieces: list[str] = []
        for token in tokens:
            if _contains_cjk(token.surface):
                pieces.append(token.reading)
            else:
                pieces.append(token.surface)
        return {"".join(pieces)}

    def to_reading_text(self, text: str) -> str:
        tokens = self._tokenize(text)
        if not tokens:
            return text
        pieces: list[str] = []
        pos = 0
        for token in tokens:
            if token.start > pos:
                pieces.append(text[pos:token.start])
            if _contains_cjk(token.surface):
                pieces.append(token.reading)
            else:
                pieces.append(token.surface)
            pos = token.end
        if pos < len(text):
            pieces.append(text[pos:])
        return "".join(pieces)

    def _tokenize(self, text: str) -> list[_Token]:
        tokens: list[_Token] = []
        if not text:
            return tokens
        pos = 0
        previous_reading = ""
        for raw in self._tagger(text):
            surface = raw.surface
            if not surface:
                continue
            start = text.find(surface, pos)
            if start == -1:
                start = pos
            if start > pos:
                pos = start
            reading = self._reading_for_token(raw, surface, previous_reading)
            end = start + len(surface)
            tokens.append(_Token(surface=surface, reading=reading, start=start, end=end))
            pos = end
            if _contains_cjk(surface) and reading:
                previous_reading = reading
            else:
                previous_reading = ""
        return tokens

    def _reading_for_token(self, token, surface: str, previous_reading: str) -> str:
        reading = self._extract_reading(token)
        if reading and not _contains_cjk(reading):
            return reading
        # Fallback: break surface into characters and resolve individually.
        chars: list[str] = []
        for ch in surface:
            if _is_cjk_char(ch):
                if ch == "々" and previous_reading:
                    chars.append(previous_reading)
                else:
                    chars.append(self._reading_for_char(ch))
            else:
                chars.append(ch)
        return "".join(chars)

    def _reading_for_char(self, ch: str) -> str:
        # Try re-tokenizing the single character to get dictionary reading.
        for raw in self._tagger(ch):
            reading = self._extract_reading(raw)
            if reading and not _contains_cjk(reading):
                return reading
        if self._kakasi_converter is not None:
            converted = self._kakasi_converter(ch)
            if converted:
                return _normalize_katakana(_hiragana_to_katakana(converted))
        return ch

    def _extract_reading(self, token) -> str:
        feature = getattr(token, "feature", None)
        value: Optional[str] = None
        for attr in ("reading", "reading_form", "kana", "pron", "pronunciation"):
            if feature is None:
                break
            attr_val = None
            if hasattr(feature, attr):
                attr_val = getattr(feature, attr)
            else:
                try:
                    attr_val = feature[attr]
                except Exception:  # pragma: no cover - feature object may not be subscriptable
                    attr_val = None
            if attr_val and attr_val != "*":
                value = attr_val
                break
        if not value:
            return ""
        return _normalize_katakana(_hiragana_to_katakana(str(value)))

    def _build_kakasi_converter(self) -> Optional[Callable[[str], str]]:
        try:
            from pykakasi import kakasi  # type: ignore
        except ImportError as exc:
            raise NLPBackendUnavailableError(
                "Advanced mode requires 'pykakasi' for fallback readings."
            ) from exc

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            kk = kakasi()
            kk.setMode("J", "K")
            kk.setMode("H", "K")
            kk.setMode("K", "K")

        def _convert(text: str) -> str:
            try:
                result = kk.convert(text)
            except Exception:  # pragma: no cover - kakasi errors are rare
                return text
            if isinstance(result, list):
                converted = "".join(item.get("kana") or item.get("orig", "") for item in result)
                return converted or text
            return str(result)

        return _convert
