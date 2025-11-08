from __future__ import annotations

import unicodedata
import warnings
from dataclasses import dataclass
from typing import Callable, Optional

from .tools import get_unidic_dicdir
from .pitch import PitchToken

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


HONORIFIC_PREFIX_SET = {"お", "御", "ご"}
HONORIFIC_SUFFIX_SET = {
    "さん",
    "さま",
    "様",
    "ちゃん",
    "ちゃん。",
    "ちゃん、",
    "殿",
    "どの",
    "氏",
    "君",
}

HONORIFIC_OVERRIDES = {
    "父": "トウ",
    "母": "カア",
    "祖父": "ソフ",
    "祖母": "ソボ",
    "兄": "ニイ",
    "姉": "ネエ",
    "弟": "トウト",
    "妹": "イモウト",
    "伯父": "オジ",
    "叔父": "オジ",
    "伯母": "オバ",
    "叔母": "オバ",
    "爺": "ジイ",
    "婆": "バア",
    "義父": "ギフ",
    "義母": "ギボ",
    "客": "キャク",
    "医者": "イシャ",
}

HONORIFIC_SUFFIX_REPLACEMENTS = {
    "君": "ギミ",
}


@dataclass
class _Token:
    surface: str
    reading: str
    start: int
    end: int
    accent_type: int | None
    accent_connection: str | None
    pos: str | None


class NLPBackend:
    """Fugashi-based backend for reading verification and kana conversion."""

    def __init__(self) -> None:
        try:
            from fugashi import Tagger  # type: ignore
        except ImportError as exc:
            raise NLPBackendUnavailableError(
                "Advanced mode requires 'fugashi' (MeCab) to be installed."
            ) from exc

        dicdir = get_unidic_dicdir()
        if dicdir:
            self._tagger = Tagger(f"-d {dicdir}")
        else:
            warnings.warn(
                "UniDic 3.1.1 not detected; falling back to the default MeCab dictionary.",
                RuntimeWarning,
                stacklevel=2,
            )
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
        reading = "".join(pieces)
        return {_normalize_katakana(reading)}

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
        result = "".join(pieces)
        return _normalize_katakana(result)

    def _tokenize(self, text: str) -> list[_Token]:
        tokens: list[_Token] = []
        if not text:
            return tokens
        raw_tokens = list(self._tagger(text))
        pos = 0
        previous_surface = ""
        previous_reading = ""
        previous_lemma = ""
        for idx, raw in enumerate(raw_tokens):
            surface = raw.surface
            if not surface:
                continue
            start = text.find(surface, pos)
            if start == -1:
                start = pos
            if start > pos:
                pos = start
            next_surface = ""
            if idx + 1 < len(raw_tokens):
                next_surface = raw_tokens[idx + 1].surface
            reading = self._reading_for_token(
                raw,
                surface,
                previous_surface,
                previous_lemma,
                next_surface,
                previous_reading,
            )
            end = start + len(surface)
            accent_type = self._extract_accent_type(raw)
            accent_connection = self._extract_accent_connection(raw)
            pos_label = self._extract_pos(raw)
            tokens.append(
                _Token(
                    surface=surface,
                    reading=reading,
                    start=start,
                    end=end,
                    accent_type=accent_type,
                    accent_connection=accent_connection,
                    pos=pos_label,
                )
            )
            pos = end
            if _contains_cjk(surface) and reading:
                previous_surface = surface
                previous_reading = reading
                previous_lemma = self._extract_lemma(raw) or surface
            else:
                previous_surface = surface
                previous_reading = ""
                previous_lemma = self._extract_lemma(raw) or surface
        return tokens

    def to_reading_with_pitch(self, text: str) -> tuple[str, list[PitchToken]]:
        tokens = self._tokenize(text)
        if not tokens:
            return text, []
        pieces: list[str] = []
        pitch_tokens: list[PitchToken] = []
        pos = 0
        for token in tokens:
            if token.start > pos:
                pieces.append(text[pos:token.start])
            segment = token.reading if _contains_cjk(token.surface) else token.surface
            segment = _normalize_katakana(segment)
            if segment:
                pieces.append(segment)
            if _contains_cjk(token.surface) and token.reading:
                pitch_tokens.append(
                    PitchToken(
                        surface=token.surface,
                        reading=segment,
                        accent_type=token.accent_type,
                        accent_connection=token.accent_connection,
                        pos=token.pos,
                    )
                )
            pos = token.end
        if pos < len(text):
            pieces.append(text[pos:])
        reading = _normalize_katakana("".join(pieces))
        return reading, pitch_tokens

    def _reading_for_token(
        self,
        token,
        surface: str,
        previous_surface: str,
        previous_lemma: str,
        next_surface: str,
        previous_reading: str,
    ) -> str:
        lemma = self._extract_lemma(token)
        cleaned_surface = surface.strip()
        base = lemma or cleaned_surface
        if base in HONORIFIC_OVERRIDES and next_surface in HONORIFIC_SUFFIX_SET:
            return HONORIFIC_OVERRIDES[base]
        if (
            previous_surface in HONORIFIC_PREFIX_SET
            and next_surface in HONORIFIC_SUFFIX_SET
            and base in HONORIFIC_OVERRIDES
        ):
            return HONORIFIC_OVERRIDES[base]
        if (
            previous_lemma in HONORIFIC_OVERRIDES
            and cleaned_surface in HONORIFIC_SUFFIX_REPLACEMENTS
        ):
            return HONORIFIC_SUFFIX_REPLACEMENTS[cleaned_surface]
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

    def _extract_lemma(self, token) -> str | None:
        feature = getattr(token, "feature", None)
        if feature is None:
            return None
        if hasattr(feature, "lemma"):
            return getattr(feature, "lemma") or None
        try:
            return feature["lemma"]  # type: ignore[index]
        except Exception:
            return None

    def _extract_pos(self, token) -> str | None:
        feature = getattr(token, "feature", None)
        if feature is None:
            return None
        for attr in ("pos1", "pos"):
            if hasattr(feature, attr):
                value = getattr(feature, attr)
                if value and value != "*":
                    return str(value)
            else:
                try:
                    value = feature[attr]
                except Exception:
                    value = None
                if value and value != "*":
                    return str(value)
        return None

    def _extract_accent_type(self, token) -> int | None:
        feature = getattr(token, "feature", None)
        if feature is None:
            return None
        for attr in ("aType", "accentType", "pitchAccentType"):
            if hasattr(feature, attr):
                value = getattr(feature, attr)
            else:
                try:
                    value = feature[attr]
                except Exception:
                    value = None
            if not value or value == "*":
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
        return None

    def _extract_accent_connection(self, token) -> str | None:
        feature = getattr(token, "feature", None)
        if feature is None:
            return None
        for attr in ("aConType", "accentConnection", "pitchAccentConnection"):
            if hasattr(feature, attr):
                value = getattr(feature, attr)
            else:
                try:
                    value = feature[attr]
                except Exception:
                    value = None
            if value and value != "*":
                return str(value)
        return None

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
