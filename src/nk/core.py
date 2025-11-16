from __future__ import annotations

import json
import re
import warnings
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
import json
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Mapping
from urllib.parse import unquote
try:
    from importlib import resources
except ImportError:  # pragma: no cover
    import importlib_resources as resources  # type: ignore

from bs4 import (
    BeautifulSoup,
    Doctype,
    FeatureNotFound,
    NavigableString,
    Tag,
    XMLParsedAsHTMLWarning,
)  # type: ignore

from .pitch import PitchToken
from .tokens import ChapterToken, tokens_to_pitch_tokens

if TYPE_CHECKING:
    from .nlp import NLPBackend

HTML_EXTS = (".xhtml", ".html", ".htm")

# Block elements that should start on a new line when collapsing to text.
BLOCK_LEVEL_TAGS = {
    "address",
    "article",
    "aside",
    "blockquote",
    "dd",
    "div",
    "dl",
    "dt",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hgroup",
    "hr",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "ul",
    "tr",
}
# Tags that should force a break even when nested inside another block.
FORCE_BREAK_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6", "li", "p", "dt", "dd", "tr"}

CHAPTER_MARKER_PREFIX = "[[NKCHAP:"
CHAPTER_MARKER_SUFFIX = "]]"
CHAPTER_MARKER_PATTERN = re.compile(r"\[\[NKCHAP:(\d+)\]\]")


def _chapter_marker_spans(text: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    search_pos = 0
    while True:
        start = text.find(CHAPTER_MARKER_PREFIX, search_pos)
        if start == -1:
            break
        end_marker = text.find(CHAPTER_MARKER_SUFFIX, start + len(CHAPTER_MARKER_PREFIX))
        if end_marker == -1:
            break
        end = end_marker + len(CHAPTER_MARKER_SUFFIX)
        spans.append((start, end))
        search_pos = end
    return spans

SMALL_KANA_BASE_MAP = {
    "ァ": "ア",
    "ィ": "イ",
    "ゥ": "ウ",
    "ェ": "エ",
    "ォ": "オ",
    "ャ": "ヤ",
    "ュ": "ユ",
    "ョ": "ヨ",
    "ッ": "ツ",
    "ヮ": "ワ",
    "ヵ": "カ",
    "ヶ": "ケ",
    "ぁ": "ア",
    "ぃ": "イ",
    "ぅ": "ウ",
    "ぇ": "エ",
    "ぉ": "オ",
    "ゃ": "ヤ",
    "ゅ": "ユ",
    "ょ": "ヨ",
    "っ": "ツ",
    "ゎ": "ワ",
}
SMALL_KANA_SET = set(SMALL_KANA_BASE_MAP.keys())

@dataclass
class _ReadingFlags:
    has_hiragana: bool = False
    has_latin: bool = False
    has_middle_dot: bool = False
    has_long_mark: bool = False


@dataclass
class _ReadingAccumulator:
    counts: Counter[str] = field(default_factory=Counter)
    flags: dict[str, _ReadingFlags] = field(default_factory=dict)
    total: int = 0
    single_kanji_only: bool | None = None
    suffix_counts: Counter[str] = field(default_factory=Counter)
    suffix_samples: list[str] = field(default_factory=list)
    prefix_counts: Counter[str] = field(default_factory=Counter)
    prefix_samples: list[str] = field(default_factory=list)

    def register(
        self,
        base: str,
        reading: str,
        raw_reading: str,
        has_hiragana: bool,
        suffix: str,
        prefix: str,
    ) -> None:
        self.total += 1
        self.counts[reading] += 1
        flags = self.flags.setdefault(reading, _ReadingFlags())
        flags.has_hiragana = flags.has_hiragana or has_hiragana
        flags.has_latin = flags.has_latin or any(
            "LATIN" in unicodedata.name(ch, "") for ch in raw_reading
        )
        flags.has_middle_dot = flags.has_middle_dot or ("・" in raw_reading)
        flags.has_long_mark = flags.has_long_mark or ("ー" in raw_reading)
        if suffix:
            self.suffix_counts[suffix] += 1
            if len(self.suffix_samples) < _MAX_SUFFIX_SAMPLES:
                self.suffix_samples.append(suffix)
        if prefix:
            self.prefix_counts[prefix] += 1
            if len(self.prefix_samples) < _MAX_PREFIX_SAMPLES:
                self.prefix_samples.append(prefix)
        single_occurrence = _is_single_kanji_base(base)
        if self.single_kanji_only is None:
            self.single_kanji_only = single_occurrence
        else:
            self.single_kanji_only = self.single_kanji_only and single_occurrence

    def merge_from(self, other: _ReadingAccumulator) -> None:
        self.counts.update(other.counts)
        self.total += other.total
        if other.single_kanji_only is not None:
            if self.single_kanji_only is None:
                self.single_kanji_only = other.single_kanji_only
            else:
                self.single_kanji_only = self.single_kanji_only and other.single_kanji_only
        for reading, other_flags in other.flags.items():
            flags = self.flags.setdefault(reading, _ReadingFlags())
            flags.has_hiragana = flags.has_hiragana or other_flags.has_hiragana
            flags.has_latin = flags.has_latin or other_flags.has_latin
            flags.has_middle_dot = flags.has_middle_dot or other_flags.has_middle_dot
            flags.has_long_mark = flags.has_long_mark or other_flags.has_long_mark
        self.suffix_counts.update(other.suffix_counts)
        if other.suffix_samples and len(self.suffix_samples) < _MAX_SUFFIX_SAMPLES:
            remaining = _MAX_SUFFIX_SAMPLES - len(self.suffix_samples)
            self.suffix_samples.extend(other.suffix_samples[:remaining])
        self.prefix_counts.update(other.prefix_counts)
        if other.prefix_samples and len(self.prefix_samples) < _MAX_PREFIX_SAMPLES:
            remaining = _MAX_PREFIX_SAMPLES - len(self.prefix_samples)
            self.prefix_samples.extend(other.prefix_samples[:remaining])


_CORPUS_READING_CACHE: dict[str, _ReadingAccumulator] | None = None
_MAX_SUFFIX_SAMPLES = 12
_MAX_SUFFIX_CONTEXTS = 8
_MAX_PREFIX_SAMPLES = 12
_MAX_PREFIX_CONTEXTS = 6
_NUMERIC_PREFIX_CHARS = set("0123456789０１２３４５６７８９一二三四五六七八九十百千〇零")
_SURFACE_PITCH_CACHE_ATTR = "_nk_surface_pitch_cache"
_SURFACE_PITCH_SKIP_SOURCES = {"unidic"}


@dataclass
class _ContextRule:
    prefixes: tuple[str, ...] = ()
    max_prefix: int = 0


def _normalize_numeric_prefix(text: str) -> str:
    return unicodedata.normalize("NFKC", text or "")


def _context_rule_for_accumulator(base: str, accumulator: _ReadingAccumulator) -> _ContextRule | None:
    if not _is_single_kanji_base(base):
        return None
    if not accumulator.prefix_counts:
        return None
    total = sum(accumulator.prefix_counts.values())
    if total <= 0:
        return None
    prefixes: list[str] = []
    for value, count in accumulator.prefix_counts.most_common(_MAX_PREFIX_CONTEXTS):
        if not value:
            continue
        share = count / total
        if share < 0.1:
            break
        normalized = _normalize_numeric_prefix(value)
        if normalized not in prefixes:
            prefixes.append(normalized)
    if not prefixes:
        return None
    max_prefix = max(len(prefix) for prefix in prefixes)
    return _ContextRule(prefixes=tuple(prefixes), max_prefix=max_prefix)


def _extract_numeric_prefix_from_text(text: str, start: int, max_prefix: int) -> str:
    if start <= 0 or max_prefix <= 0:
        return ""
    chars: list[str] = []
    idx = start - 1
    while idx >= 0 and len(chars) < max_prefix:
        ch = text[idx]
        if ch.isspace():
            if chars:
                break
            idx -= 1
            continue
        if ch not in _NUMERIC_PREFIX_CHARS:
            break
        chars.append(ch)
        idx -= 1
    if not chars:
        return ""
    return _normalize_numeric_prefix("".join(reversed(chars)))


@dataclass
class ChapterText:
    source: str
    title: str | None
    text: str
    original_text: str | None = None
    original_title: str | None = None
    book_title: str | None = None
    pitch_data: list[PitchToken] | None = None
    book_author: str | None = None
    tokens: list[ChapterToken] | None = None


@dataclass
class CoverImage:
    path: str
    media_type: str | None
    data: bytes


@dataclass
class _NavPoint:
    order: int
    path: str
    spine_index: int
    fragment: str | None
    title: str | None
    local_order: int = 0


@dataclass
class _FallbackSegment:
    spine_index: int
    order: int
    sequence: int
    source: str
    fragment: "_TextFragment"
    raw_original: str


@dataclass
class _PendingChapter:
    sort_key: tuple[int, int, int]
    source: str
    raw_text: str
    raw_original: str
    title_hint: str | None
    tokens: list[PitchToken] = field(default_factory=list)
    ruby_spans: list[_RubySpan] = field(default_factory=list)


@dataclass
class _TextFragment:
    text: str
    tokens: list[PitchToken]
    ruby_spans: list[_RubySpan] | None = None


@dataclass
class _TrackedTokenRecord:
    surface: str
    reading: str
    sources: tuple[str, ...]


class _TransformationTracker:
    _START_PREFIX = "[[NKT:"
    _CLOSE_PREFIX = "[[/NKT:"
    _SUFFIX = "]]"

    def __init__(self) -> None:
        self._next_id = 1
        self._records: dict[str, _TrackedTokenRecord] = {}

    def wrap(self, surface: str, reading: str, sources: tuple[str, ...]) -> str:
        if not reading:
            return reading
        token_id = str(self._next_id)
        self._next_id += 1
        normalized_sources: tuple[str, ...] = tuple(dict.fromkeys(sources)) if sources else tuple()
        self._records[token_id] = _TrackedTokenRecord(
            surface=surface,
            reading=reading,
            sources=normalized_sources,
        )
        return (
            f"{self._START_PREFIX}{token_id}{self._SUFFIX}"
            f"{reading}"
            f"{self._CLOSE_PREFIX}{token_id}{self._SUFFIX}"
        )

    def extract(self, text: str) -> _TextFragment:
        if not text or self._START_PREFIX not in text:
            return _TextFragment(text=text, tokens=[])
        result_chars: list[str] = []
        tokens: list[PitchToken] = []
        idx = 0
        length = len(text)
        while idx < length:
            if text.startswith(self._START_PREFIX, idx):
                id_start = idx + len(self._START_PREFIX)
                id_end = text.find(self._SUFFIX, id_start)
                if id_end == -1:
                    result_chars.append(text[idx])
                    idx += 1
                    continue
                token_id = text[id_start:id_end]
                close_marker = f"{self._CLOSE_PREFIX}{token_id}{self._SUFFIX}"
                idx = id_end + len(self._SUFFIX)
                start_pos = len(result_chars)
                reading_chars: list[str] = []
                while idx < length:
                    if text.startswith(close_marker, idx):
                        idx += len(close_marker)
                        break
                    reading_chars.append(text[idx])
                    result_chars.append(text[idx])
                    idx += 1
                end_pos = len(result_chars)
                record = self._records.get(token_id)
                if record is None:
                    continue
                reading_text = "".join(reading_chars) or record.reading
                tokens.append(
                    PitchToken(
                        surface=record.surface,
                        reading=reading_text,
                        accent_type=None,
                        start=start_pos,
                        end=end_pos,
                        sources=record.sources,
                    )
                )
            else:
                result_chars.append(text[idx])
                idx += 1
        return _TextFragment(text="".join(result_chars), tokens=tokens)


def _tracker_marker_spans(text: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    prefix = _TransformationTracker._START_PREFIX
    close_prefix = _TransformationTracker._CLOSE_PREFIX
    suffix = _TransformationTracker._SUFFIX
    idx = 0
    length = len(text)
    while idx < length:
        start_idx = text.find(prefix, idx)
        if start_idx == -1:
            break
        id_start = start_idx + len(prefix)
        id_end = text.find(suffix, id_start)
        if id_end == -1:
            break
        token_id = text[id_start:id_end]
        close_marker = f"{close_prefix}{token_id}{suffix}"
        close_idx = text.find(close_marker, id_end + len(suffix))
        if close_idx == -1:
            idx = id_end + len(suffix)
            continue
        span_end = close_idx + len(close_marker)
        spans.append((start_idx, span_end))
        idx = span_end
    return spans


@dataclass
class _RubySpanRecord:
    base: str
    reading: str


@dataclass
class _RubySpan:
    start: int
    end: int
    base: str
    reading: str


class _RubySpanTracker:
    _START_PREFIX = "[[NKR:"
    _END_PREFIX = "[[/NKR:"
    _SUFFIX = "]]"

    def __init__(self) -> None:
        self._next_id = 1
        self._records: dict[str, _RubySpanRecord] = {}

    def wrap_ruby(self, ruby: Tag) -> None:
        token_id = str(self._next_id)
        self._next_id += 1
        base_raw = _normalize_ws(_ruby_base_text(ruby))
        reading_raw = _normalize_ws(_ruby_reading_text(ruby))
        if not base_raw or not reading_raw:
            return
        base_norm = unicodedata.normalize("NFKC", base_raw)
        reading_norm = _normalize_katakana(_hiragana_to_katakana(reading_raw))
        if not reading_norm:
            return
        self._records[token_id] = _RubySpanRecord(base=base_norm, reading=reading_norm)
        start_marker = NavigableString(f"{self._START_PREFIX}{token_id}{self._SUFFIX}")
        end_marker = NavigableString(f"{self._END_PREFIX}{token_id}{self._SUFFIX}")
        ruby.insert_before(start_marker)
        ruby.insert_after(end_marker)

    def mark_soup(self, soup: BeautifulSoup) -> None:
        for ruby in list(soup.find_all("ruby")):
            self.wrap_ruby(ruby)

    def extract(self, text: str) -> tuple[str, list[_RubySpan]]:
        if not text:
            return "", []
        result_chars: list[str] = []
        spans: list[_RubySpan] = []
        idx = 0
        length = len(text)
        while idx < length:
            if text.startswith(self._START_PREFIX, idx):
                id_start = idx + len(self._START_PREFIX)
                id_end = text.find(self._SUFFIX, id_start)
                if id_end == -1:
                    result_chars.append(text[idx])
                    idx += 1
                    continue
                token_id = text[id_start:id_end]
                idx = id_end + len(self._SUFFIX)
                start_pos = len(result_chars)
                close_marker = f"{self._END_PREFIX}{token_id}{self._SUFFIX}"
                close_idx = text.find(close_marker, idx)
                if close_idx == -1:
                    # Marker not closed; re-emit the literal marker text.
                    literal = f"{self._START_PREFIX}{token_id}{self._SUFFIX}"
                    result_chars.append(literal)
                    continue
                # Consume everything between markers.
                while idx < close_idx:
                    result_chars.append(text[idx])
                    idx += 1
                end_pos = len(result_chars)
                idx += len(close_marker)
                record = self._records.get(token_id)
                if record is None:
                    continue
                spans.append(
                    _RubySpan(
                        start=start_pos,
                        end=end_pos,
                        base=record.base,
                        reading=record.reading,
                    )
                )
            else:
                result_chars.append(text[idx])
                idx += 1
        return "".join(result_chars), spans


def _is_cjk_char(ch: str) -> bool:
    if not ch:
        return False
    code = ord(ch)
    return (
        0x4E00 <= code <= 0x9FFF  # CJK Unified Ideographs
        or 0x3400 <= code <= 0x4DBF  # Extension A
        or 0x20000 <= code <= 0x2A6DF  # Extension B
        or 0x2A700 <= code <= 0x2B73F  # Extension C
        or 0x2B740 <= code <= 0x2B81F  # Extension D
        or 0x2B820 <= code <= 0x2CEAF  # Extension E
        or 0x2CEB0 <= code <= 0x2EBEF  # Extension F
        or 0x30000 <= code <= 0x3134F  # Extension G
        or 0xF900 <= code <= 0xFAFF  # Compatibility Ideographs
        or 0x2F800 <= code <= 0x2FA1F  # Compatibility Supplement
        or ch in "々〆ヵヶ"
    )


def _is_numeric_string(value: str) -> bool:
    if not value:
        return False
    return all(ch.isdigit() for ch in value)


def _build_mapping_pattern(mapping: dict[str, str]) -> re.Pattern[str] | None:
    if not mapping:
        return None
    keys = sorted(mapping.keys(), key=len, reverse=True)
    if not keys:
        return None
    return re.compile("|".join(re.escape(k) for k in keys))


def _extract_numeric_prefix_context(text: str, start: int, max_len: int) -> str:
    if max_len <= 0 or start <= 0:
        return ""
    chars: list[str] = []
    idx = start - 1
    while idx >= 0 and len(chars) < max_len:
        ch = text[idx]
        if ch.isspace():
            if chars:
                break
            idx -= 1
            continue
        if ch not in _NUMERIC_PREFIX_CHARS:
            break
        chars.append(ch)
        idx -= 1
    if not chars:
        return ""
    return _normalize_numeric_prefix("".join(reversed(chars)))


def _apply_mapping_with_pattern(
    text: str,
    mapping: dict[str, str],
    pattern: re.Pattern[str],
    tracker: _TransformationTracker | None = None,
    source_labels: dict[str, str] | None = None,
    context_rules: dict[str, _ContextRule] | None = None,
    skip_chapter_markers: bool = False,
) -> str:
    if pattern is None:
        return text
    protected_spans: list[tuple[int, int]] = []
    if skip_chapter_markers:
        protected_spans = _chapter_marker_spans(text)
    if tracker is not None:
        protected_spans.extend(_tracker_marker_spans(text))
    protected_spans = sorted(protected_spans)
    protected_starts: list[int] = [span[0] for span in protected_spans] if protected_spans else []

    def repl(match: re.Match[str]) -> str:
        base = match.group(0)
        start, end = match.span()
        if protected_spans:
            idx = bisect_right(protected_starts, start) - 1
            if idx >= 0:
                span_start, span_end = protected_spans[idx]
                if span_start <= start < span_end:
                    return base
        prev_ch = text[start - 1] if start > 0 else ""
        next_ch = text[end] if end < len(text) else ""
        if base:
            if base[0].isdigit() and prev_ch and prev_ch.isdigit():
                return base
            if base[-1].isdigit() and next_ch and next_ch.isdigit():
                return base
        if len(base) == 1:
            if (_is_cjk_char(prev_ch) and prev_ch != "\n") or _is_cjk_char(next_ch):
                return base
            if base.isascii() and base.isalnum():
                if (prev_ch.isascii() and prev_ch.isalnum()) or (
                    next_ch.isascii() and next_ch.isalnum()
                ):
                    return base
        elif _is_numeric_string(base):
            if (prev_ch and prev_ch.isdigit()) or (next_ch and next_ch.isdigit()):
                return base
        rule = context_rules.get(base) if context_rules else None
        if rule:
            prefix = _extract_numeric_prefix_context(text, start, rule.max_prefix)
            if not prefix or prefix not in rule.prefixes:
                return base
        replacement = mapping[base]
        if tracker and replacement:
            source = (source_labels.get(base) if source_labels else None) or "propagation"
            replacement = tracker.wrap(base, replacement, (source,))
        return replacement

    return pattern.sub(repl, text)


def _mapping_match_allowed(text: str, start: int, end: int, base: str) -> bool:
    prev_ch = text[start - 1] if start > 0 else ""
    next_ch = text[end] if end < len(text) else ""
    if base:
        if base[0].isdigit() and prev_ch.isdigit():
            return False
        if base[-1].isdigit() and next_ch.isdigit():
            return False
    if len(base) == 1:
        if (_is_cjk_char(prev_ch) and prev_ch != "\n") or _is_cjk_char(next_ch):
            return False
        if base.isascii() and base.isalnum():
            if (prev_ch.isascii() and prev_ch.isalnum()) or (next_ch.isascii() and next_ch.isalnum()):
                return False
    elif _is_numeric_string(base):
        if (prev_ch and prev_ch.isdigit()) or (next_ch and next_ch.isdigit()):
            return False
    return True


def _iter_mapping_matches(
    text: str,
    mapping: Mapping[str, str],
    context_rules: Mapping[str, _ContextRule] | None = None,
) -> list[tuple[int, int, str, str]]:
    pattern = _build_mapping_pattern(mapping)
    if pattern is None:
        return []
    matches: list[tuple[int, int, str, str]] = []
    for match in pattern.finditer(text):
        start, end = match.span()
        base = match.group(0)
        reading = mapping.get(base)
        if not reading:
            continue
        if not _mapping_match_allowed(text, start, end, base):
            continue
        rule = context_rules.get(base) if context_rules else None
        if rule:
            prefix = _extract_numeric_prefix_from_text(text, start, rule.max_prefix)
            if not prefix or prefix not in rule.prefixes:
                continue
        matches.append((start, end, base, reading))
    return matches


def _get_book_title(zf: zipfile.ZipFile) -> str | None:
    try:
        opf_path = _find_opf_path(zf)
        opf_xml = _zip_read_text(zf, opf_path)
        root = ET.fromstring(opf_xml)
        for title_el in root.findall(".//{http://purl.org/dc/elements/1.1/}title"):
            title_text = "".join(title_el.itertext()).strip()
            if title_text:
                return unicodedata.normalize("NFKC", title_text)
    except Exception:
        return None
    return None


def _get_book_author(zf: zipfile.ZipFile) -> str | None:
    try:
        opf_path = _find_opf_path(zf)
        opf_xml = _zip_read_text(zf, opf_path)
        root = ET.fromstring(opf_xml)
        authors: list[str] = []
        for creator_el in root.findall(".//{http://purl.org/dc/elements/1.1/}creator"):
            raw_name = "".join(creator_el.itertext()).strip()
            if not raw_name:
                continue
            normalized = unicodedata.normalize("NFKC", raw_name).strip()
            if not normalized:
                continue
            role = _get_attr(creator_el, "role")
            if role:
                role_lower = role.lower()
                if role_lower not in {"aut", "author"}:
                    continue
            if normalized not in authors:
                authors.append(normalized)
        if not authors:
            return None
        if len(authors) == 1:
            return authors[0]
        ascii_only = all(_looks_like_ascii_word(name) for name in authors)
        separator = ", " if ascii_only else "・"
        return separator.join(authors)
    except Exception:
        return None


def _apply_mapping_to_plain_text(
    text: str,
    mapping: dict[str, str],
    context_rules: dict[str, _ContextRule] | None = None,
    tracker: _TransformationTracker | None = None,
    source_labels: dict[str, str] | None = None,
) -> str:
    pattern = _build_mapping_pattern(mapping)
    if pattern is None:
        return text
    return _apply_mapping_with_pattern(
        text,
        mapping,
        pattern,
        tracker=tracker,
        source_labels=source_labels,
        context_rules=context_rules,
        skip_chapter_markers=True,
    )


def _lookup_surface_pitch(
    surface: str,
    backend: "NLPBackend",
) -> tuple[str, PitchToken] | None:
    if not surface:
        return None
    try:
        reading_text, tokens = backend.to_reading_with_pitch(surface)
    except Exception:
        return None
    if len(tokens) != 1:
        return None
    token = tokens[0]
    if token.accent_type is None or not token.reading:
        return None
    normalized_token_reading = _normalize_katakana(token.reading)
    normalized_surface_reading = _normalize_katakana(reading_text.strip())
    if not normalized_surface_reading:
        return None
    if normalized_token_reading != normalized_surface_reading:
        return None
    return normalized_surface_reading, token


def _fill_missing_pitch_from_surface(tokens: list[PitchToken], backend: "NLPBackend") -> None:
    if not tokens:
        return
    cache = getattr(backend, _SURFACE_PITCH_CACHE_ATTR, None)
    if cache is None:
        cache = {}
        setattr(backend, _SURFACE_PITCH_CACHE_ATTR, cache)
    sentinel = object()
    for token in tokens:
        if token.accent_type is not None or not token.reading or not token.surface:
            continue
        sources = token.sources or ()
        if any((source or "").lower() in _SURFACE_PITCH_SKIP_SOURCES for source in sources):
            continue
        normalized_reading = _normalize_katakana(token.reading)
        if not normalized_reading:
            continue
        cache_key = unicodedata.normalize("NFKC", token.surface)
        cached = cache.get(cache_key, sentinel)
        if cached is sentinel:
            cached = _lookup_surface_pitch(token.surface, backend)
            cache[cache_key] = cached
        if not cached:
            continue
        cached_reading, source_token = cached
        if cached_reading != normalized_reading:
            continue
        if source_token.accent_type is None:
            continue
        token.accent_type = source_token.accent_type
        token.accent_connection = source_token.accent_connection
        if source_token.pos:
            token.pos = source_token.pos


def _align_pitch_tokens(text: str, tokens: list[PitchToken]) -> list[PitchToken]:
    if not text or not tokens:
        return []
    aligned: list[PitchToken] = []
    cursor = 0
    text_len = len(text)
    for token in tokens:
        reading = token.reading
        if not reading:
            continue
        hint = max(0, min(token.start, text_len))
        idx = hint
        if not text.startswith(reading, idx):
            window = max(len(reading), 16)
            window_start = max(0, hint - window)
            window_end = min(text_len, hint + window + len(reading))
            window_idx = text.find(reading, window_start, window_end)
            if window_idx != -1:
                idx = window_idx
            else:
                sequential_idx = text.find(reading, cursor)
                if sequential_idx != -1:
                    idx = sequential_idx
                else:
                    fallback_idx = text.find(reading)
                    if fallback_idx != -1:
                        idx = fallback_idx
        idx = max(0, min(idx, text_len))
        if not text.startswith(reading, idx):
            raise ValueError(f"Unable to align token '{token.surface}' with reading '{reading}'")
        end = min(text_len, idx + len(reading))
        aligned.append(replace(token, start=idx, end=end))
        cursor = end
    return aligned


def _remap_tokens_to_text(text: str, tokens: list[PitchToken]) -> None:
    if not text or not tokens:
        return
    cursor = 0
    text_len = len(text)
    for token in tokens:
        reading = token.reading
        if not reading:
            token.start = cursor
            token.end = cursor
            continue
        hint = max(0, min(token.start, text_len))
        idx = hint
        if not text.startswith(reading, idx):
            window = max(len(reading), 16)
            window_start = max(0, hint - window)
            window_end = min(text_len, hint + window + len(reading))
            window_idx = text.find(reading, window_start, window_end)
            if window_idx != -1:
                idx = window_idx
            else:
                sequential_idx = text.find(reading, cursor)
                if sequential_idx != -1:
                    idx = sequential_idx
                else:
                    fallback_idx = text.find(reading)
                    if fallback_idx != -1:
                        idx = fallback_idx
        idx = max(0, min(idx, text_len))
        if not text.startswith(reading, idx):
            raise ValueError(f"Unable to remap token '{token.surface}' with reading '{reading}'")
        end = min(text_len, idx + len(reading))
        token.start = idx
        token.end = end
        cursor = end


def _find_surface_positions(text: str, surface: str) -> list[int]:
    positions: list[int] = []
    if not text or not surface:
        return positions
    start = 0
    while True:
        idx = text.find(surface, start)
        if idx == -1:
            break
        positions.append(idx)
        start = idx + max(1, len(surface))
    return positions


def _range_is_free(ranges: list[tuple[int, int]], start: int, end: int) -> bool:
    for existing_start, existing_end in ranges:
        if end <= existing_start:
            continue
        if start >= existing_end:
            continue
        return False
    return True


def _add_coverage_range(ranges: list[tuple[int, int]], start: int, end: int) -> None:
    ranges.append((start, end))
    ranges.sort(key=lambda item: item[0])


def _slice_ruby_spans(spans: list[_RubySpan], start: int, end: int) -> list[_RubySpan]:
    if not spans or end <= start:
        return []
    sliced: list[_RubySpan] = []
    for span in spans:
        if span.end <= start or span.start >= end:
            continue
        sliced.append(
            _RubySpan(
                start=max(0, span.start - start),
                end=min(end, span.end) - start,
                base=span.base,
                reading=span.reading,
            )
        )
    return sliced


def _align_tokens_to_original_text(original_text: str | None, tokens: list[PitchToken] | None) -> None:
    if not original_text or not tokens:
        return
    cursor = 0
    text_len = len(original_text)
    surface_positions: dict[str, list[int]] = {}
    surface_indices: dict[str, int] = {}
    for token in tokens:
        surface = (token.surface or "").strip()
        if not surface or surface in surface_positions:
            continue
        surface_positions[surface] = _find_surface_positions(original_text, surface)
        surface_indices[surface] = 0
    for token in tokens:
        token.original_start = None
        token.original_end = None
        surface = (token.surface or "").strip()
        if not surface:
            continue
        positions = surface_positions.get(surface)
        if positions:
            pos_idx = surface_indices.get(surface, 0)
            if pos_idx < len(positions):
                idx = positions[pos_idx]
                surface_indices[surface] = pos_idx + 1
            else:
                idx = -1
        else:
            idx = -1
        if idx == -1:
            idx = original_text.find(surface, cursor)
            if idx == -1 and cursor > 0:
                window_start = max(0, cursor - len(surface) - 8)
                idx = original_text.find(surface, window_start, cursor)
        if idx == -1:
            continue
        start = idx
        end = min(text_len, idx + len(surface))
        token.original_start = start
        token.original_end = end
        cursor = max(cursor, end)


def _normalize_token_order(piece_text: str, tokens: list[PitchToken]) -> None:
    if not piece_text or not tokens:
        return
    if not any(token.original_start is not None for token in tokens):
        return
    tokens.sort(
        key=lambda token: (
            token.original_start if token.original_start is not None else token.start,
            token.start,
        )
    )


def _realign_tokens_to_text(
    piece_text: str,
    tokens: list[PitchToken] | None,
    original_text: str | None,
) -> list[PitchToken] | None:
    if not piece_text or not tokens:
        return tokens
    try:
        realigned = _align_pitch_tokens(piece_text, tokens)
    except ValueError:
        return tokens
    if not realigned:
        return None
    _align_tokens_to_original_text(original_text, realigned)
    _normalize_token_order(piece_text, realigned)
    return realigned


def _build_chapter_tokens_from_original(
    text: str,
    backend: "NLPBackend",
    ruby_spans: list[_RubySpan] | None,
    unique_mapping: Mapping[str, str],
    common_mapping: Mapping[str, str],
    unique_sources: Mapping[str, str],
    common_sources: Mapping[str, str],
    context_rules: Mapping[str, _ContextRule],
) -> list[ChapterToken]:
    coverage: list[tuple[int, int]] = []
    tokens: list[ChapterToken] = []

    def _append_token(
        start: int,
        end: int,
        reading: str,
        source: str,
        fallback: str | None = None,
        accent_type: int | None = None,
        accent_connection: str | None = None,
        pos: str | None = None,
    ) -> None:
        surface = text[start:end]
        if not surface:
            return
        token = ChapterToken(
            surface=surface,
            start=start,
            end=end,
            reading=_normalize_katakana(reading),
            reading_source=source,
            fallback_reading=_normalize_katakana(fallback or reading),
            context_prefix=text[max(0, start - 3) : start],
            context_suffix=text[end : end + 3],
            accent_type=accent_type,
            accent_connection=accent_connection,
            pos=pos,
        )
        tokens.append(token)
        _add_coverage_range(coverage, start, end)

    if ruby_spans:
        for span in ruby_spans:
            start = max(0, min(len(text), span.start))
            end = max(start, min(len(text), span.end))
            if not _range_is_free(coverage, start, end):
                continue
            _append_token(start, end, span.reading, "ruby")

    for mapping, sources in (
        (unique_mapping, unique_sources),
        (common_mapping, common_sources),
    ):
        if not mapping:
            continue
        matches = _iter_mapping_matches(text, mapping, context_rules)
        if not matches:
            continue
        matches.sort(key=lambda item: (item[0], -(item[1] - item[0])))
        for start, end, base, reading in matches:
            if not _range_is_free(coverage, start, end):
                continue
            source_label = sources.get(base, "propagation")
            _append_token(start, end, reading, source_label)

    raw_tokens = backend.tokenize(text)
    for raw in raw_tokens:
        start = raw.start
        end = raw.end
        if not _range_is_free(coverage, start, end):
            continue
        surface = raw.surface
        if not surface or not _contains_cjk(surface):
            continue
        reading = _normalize_katakana(raw.reading)
        if not reading:
            continue
        _append_token(
            start,
            end,
            reading,
            "unidic",
            fallback=reading,
            accent_type=raw.accent_type,
            accent_connection=raw.accent_connection,
            pos=raw.pos,
        )

    tokens.sort(key=lambda token: (token.start, token.end))
    return tokens


def _render_text_from_tokens(text: str, tokens: list[ChapterToken]) -> tuple[str, list[ChapterToken]]:
    if not text:
        return "", []
    if not tokens:
        normalized = _normalize_katakana(_hiragana_to_katakana(text))
        normalized = _normalize_ellipsis(normalized)
        return normalized, []
    output: list[str] = []
    cursor = 0
    out_pos = 0
    for token in sorted(tokens, key=lambda t: (t.start, t.end)):
        if token.start > cursor:
            chunk = text[cursor : token.start]
            normalized_chunk = _normalize_katakana(_hiragana_to_katakana(chunk))
            if normalized_chunk:
                output.append(normalized_chunk)
                out_pos += len(normalized_chunk)
        reading = token.reading or token.fallback_reading or token.surface
        normalized_reading = _normalize_katakana(reading)
        token.reading = normalized_reading
        token.transformed_start = out_pos
        output.append(normalized_reading)
        out_pos += len(normalized_reading)
        token.transformed_end = out_pos
        cursor = token.end
    if cursor < len(text):
        chunk = text[cursor:]
        normalized_chunk = _normalize_katakana(_hiragana_to_katakana(chunk))
        if normalized_chunk:
            output.append(normalized_chunk)
    rendered = _normalize_ellipsis("".join(output))
    return rendered, tokens


def _hiragana_to_katakana(text: str) -> str:
    result_chars: list[str] = []
    for ch in text:
        code = ord(ch)
        if 0x3041 <= code <= 0x3096:
            result_chars.append(chr(code + 0x60))
        elif ch == "ゝ":
            result_chars.append("ヽ")
        elif ch == "ゞ":
            result_chars.append("ヾ")
        elif ch == "ゟ":
            result_chars.append("ヿ")
        else:
            result_chars.append(ch)
    return "".join(result_chars)


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


def _zip_read_text(zf: zipfile.ZipFile, name: str) -> str:
    raw = zf.read(name)
    for enc in ("utf-8", "utf-16", "cp932", "shift_jis", "euc_jp"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _zip_read_bytes(zf: zipfile.ZipFile, name: str) -> bytes:
    with zf.open(name, "r") as handle:
        return handle.read()


def _find_opf_path(zf: zipfile.ZipFile) -> str:
    # Per spec: META-INF/container.xml -> rootfiles/rootfile@full-path
    try:
        container = _zip_read_text(zf, "META-INF/container.xml")
        root = ET.fromstring(container)
        ns = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
        for rf in root.findall(".//c:rootfile", ns):
            full = rf.attrib.get("full-path")
            if full:
                return full
    except Exception:
        pass
    # Fallback: first *.opf found
    for n in zf.namelist():
        if n.lower().endswith(".opf"):
            return n
    raise FileNotFoundError("OPF file not found in EPUB")


def _spine_items(zf: zipfile.ZipFile) -> list[str]:
    opf_path = _find_opf_path(zf)
    opf_xml = _zip_read_text(zf, opf_path)
    root = ET.fromstring(opf_xml)
    # Resolve namespaces loosely
    nsmap = {"opf": root.tag.split("}")[0].strip("{")}
    # manifest id -> href
    manifest = {}
    for it in root.findall(".//opf:manifest/opf:item", nsmap):
        iid = it.attrib.get("id")
        href = it.attrib.get("href")
        if iid and href:
            manifest[iid] = href
    # spine order
    items = []
    for ir in root.findall(".//opf:spine/opf:itemref", nsmap):
        iid = ir.attrib.get("idref")
        if iid in manifest:
            items.append(manifest[iid])
    # Make hrefs absolute relative to OPF directory
    base = str(PurePosixPath(opf_path).parent)
    fixed = []
    for href in items:
        p = str(PurePosixPath(base) / href) if base not in ("", ".", "/") else href
        fixed.append(str(PurePosixPath(p).as_posix()))
    # If spine is empty, fall back to all HTML files in zip order
    if not fixed:
        fixed = [n for n in zf.namelist() if n.lower().endswith(HTML_EXTS)]
    return fixed


def _strip_tag(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _get_attr(elem: ET.Element, name: str) -> str | None:
    for attr, value in elem.attrib.items():
        if _strip_tag(attr) == name:
            return value
    return None


def _resolve_opf_href(opf_path: str, href: str) -> str:
    return _resolve_relative_path(opf_path, href)


def _resolve_relative_path(base_file: str, href: str) -> str:
    base = str(PurePosixPath(base_file).parent)
    if base not in ("", ".", "/"):
        combined = PurePosixPath(base) / href
    else:
        combined = PurePosixPath(href)
    return str(combined.as_posix())


def _normalize_zip_path(path: str) -> str:
    return str(PurePosixPath(path).as_posix())


def _split_href_fragment(href: str) -> tuple[str, str | None]:
    if "#" in href:
        base, frag = href.split("#", 1)
        return base, unquote(frag)
    return href, None


def _parse_nav_document(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    nav_tags = []
    for nav in soup.find_all("nav"):
        nav_type = (nav.get("epub:type") or "").lower()
        role = (nav.get("role") or "").lower()
        if "toc" in nav_type or role == "doc-toc":
            nav_tags.append(nav)
    if not nav_tags:
        nav_tags = soup.find_all("nav")
    if not nav_tags:
        return []
    entries: list[tuple[str, str]] = []
    for nav in nav_tags:
        for anchor in nav.find_all("a"):
            href = anchor.get("href")
            if not href:
                continue
            text = anchor.get_text(strip=True)
            entries.append((href, text))
    return entries


def _parse_ncx_document(xml_text: str) -> list[tuple[str, str]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    ns = {"ncx": root.tag.split("}")[0].strip("{")}

    def _collect_points(elem: ET.Element, acc: list[tuple[str, str]]) -> None:
        for nav_point in elem.findall("ncx:navPoint", ns):
            label_elem = nav_point.find(".//ncx:text", ns)
            content_elem = nav_point.find("ncx:content", ns)
            if content_elem is None:
                continue
            href = content_elem.attrib.get("src")
            if not href:
                continue
            text = ""
            if label_elem is not None:
                text = "".join(label_elem.itertext()).strip()
            acc.append((href, text))
            _collect_points(nav_point, acc)

    entries: list[tuple[str, str]] = []
    nav_map = root.find("ncx:navMap", ns)
    if nav_map is None:
        return entries
    _collect_points(nav_map, entries)
    return entries


def _toc_nav_points(zf: zipfile.ZipFile, spine: list[str]) -> list[_NavPoint]:
    try:
        opf_path = _find_opf_path(zf)
        opf_xml = _zip_read_text(zf, opf_path)
        root = ET.fromstring(opf_xml)
    except Exception:
        return []
    ns = {"opf": root.tag.split("}")[0].strip("{")}
    manifest: dict[str, dict[str, str | None]] = {}
    for item in root.findall(".//opf:manifest/opf:item", ns):
        item_id = item.attrib.get("id")
        if not item_id:
            continue
        manifest[item_id] = {
            "href": item.attrib.get("href"),
            "media_type": item.attrib.get("media-type"),
            "properties": item.attrib.get("properties"),
        }
    nav_candidates: list[str] = []
    ncx_candidates: list[str] = []
    for info in manifest.values():
        href = info.get("href")
        if not href:
            continue
        resolved = _resolve_opf_href(opf_path, href)
        properties = (info.get("properties") or "").lower()
        media_type = (info.get("media_type") or "").lower()
        if "nav" in properties:
            nav_candidates.append(resolved)
        if media_type == "application/x-dtbncx+xml":
            ncx_candidates.append(resolved)
    entries: list[tuple[str, str]] = []
    for nav_path in nav_candidates:
        try:
            html = _zip_read_text(zf, nav_path)
        except KeyError:
            continue
        entries = _parse_nav_document(html)
        if entries:
            base = nav_path
            normalized: list[tuple[str, str]] = []
            for href, title in entries:
                normalized.append((_resolve_relative_path(base, href), title))
            entries = normalized
            break
    if not entries:
        for ncx_path in ncx_candidates:
            try:
                xml_text = _zip_read_text(zf, ncx_path)
            except KeyError:
                continue
            entries = _parse_ncx_document(xml_text)
            if entries:
                base = ncx_path
                normalized = []
                for href, title in entries:
                    normalized.append((_resolve_relative_path(base, href), title))
                entries = normalized
                break
    if not entries:
        return []
    spine_map: dict[str, int] = {}
    for idx, path in enumerate(spine):
        norm = _normalize_zip_path(path)
        if norm not in spine_map:
            spine_map[norm] = idx
    nav_points: list[_NavPoint] = []
    for order, (href, title) in enumerate(entries):
        base_href, fragment = _split_href_fragment(href)
        norm = _normalize_zip_path(base_href)
        spine_index = spine_map.get(norm)
        if spine_index is None:
            continue
        nav_points.append(
            _NavPoint(
                order=order,
                path=spine[spine_index],
                spine_index=spine_index,
                fragment=fragment,
                title=title or None,
            )
        )
    return nav_points


def _insert_nav_markers(soup: BeautifulSoup, entries: list[_NavPoint]) -> None:
    if not entries:
        return
    for entry in entries:
        marker_text = f"\n{CHAPTER_MARKER_PREFIX}{entry.order}{CHAPTER_MARKER_SUFFIX}\n"
        marker = NavigableString(marker_text)
        target = None
        if entry.fragment:
            target = soup.find(id=entry.fragment)
            if target is None:
                target = soup.find(attrs={"name": entry.fragment})
        if target is None:
            body = soup.find("body")
            if body and body.contents:
                target = body.contents[0]
            else:
                target = soup
        target.insert_before(marker)


def _split_text_by_markers(text: str) -> tuple[str, list[tuple[int, str, int, int]]]:
    if not text:
        return "", []
    segments: list[tuple[int, str, int, int]] = []
    cursor = 0
    current_marker: int | None = None
    leading = ""
    for match in CHAPTER_MARKER_PATTERN.finditer(text):
        if current_marker is None:
            leading = text[: match.start()]
        else:
            segment_start = cursor
            segment = text[segment_start : match.start()]
            segments.append((current_marker, segment, segment_start, match.start()))
        current_marker = int(match.group(1))
        cursor = match.end()
    if current_marker is not None:
        segments.append((current_marker, text[cursor:], cursor, len(text)))
    else:
        leading = text
    return leading, segments


def _fragment_from_tracker(text: str, tracker: _TransformationTracker | None) -> _TextFragment:
    if tracker is None:
        return _TextFragment(text=text, tokens=[])
    return tracker.extract(text)


def _strip_fragment_newlines(fragment: _TextFragment) -> tuple[str, list[PitchToken], list[_RubySpan]]:
    raw_text = fragment.text
    if not raw_text:
        return "", [], []
    leading_trim = len(raw_text) - len(raw_text.lstrip("\n"))
    trailing_trim = len(raw_text) - len(raw_text.rstrip("\n"))
    end_idx = len(raw_text) - trailing_trim if trailing_trim else len(raw_text)
    trimmed = raw_text[leading_trim:end_idx]
    if not trimmed:
        return "", [], []
    adjusted: list[PitchToken] = []
    for token in fragment.tokens:
        start = token.start - leading_trim
        end = token.end - leading_trim
        if end <= 0 or start >= len(trimmed):
            continue
        adjusted.append(replace(token, start=start, end=end))
    span_results: list[_RubySpan] = []
    if fragment.ruby_spans:
        for span in fragment.ruby_spans:
            start = span.start - leading_trim
            end = span.end - leading_trim
            if end <= 0 or start >= len(trimmed):
                continue
            span_results.append(
                _RubySpan(
                    start=max(0, start),
                    end=min(len(trimmed), end),
                    base=span.base,
                    reading=span.reading,
                )
            )
    return trimmed, adjusted, span_results


def _trim_text_and_tokens(
    text: str,
    tokens: list[PitchToken] | None,
) -> tuple[str, list[PitchToken] | None]:
    if not text:
        return "", tokens
    left = 0
    right = len(text)
    while left < right and text[left].isspace():
        left += 1
    while right > left and text[right - 1].isspace():
        right -= 1
    if left == 0 and right == len(text):
        return text, tokens
    trimmed = text[left:right]
    if not tokens:
        return trimmed, tokens
    adjusted: list[PitchToken] = []
    for token in tokens:
        start = max(left, min(token.start, right))
        end = max(left, min(token.end, right))
        new_start = max(0, start - left)
        new_end = max(new_start, end - left)
        adjusted.append(replace(token, start=new_start, end=new_end))
    return trimmed, adjusted


def _trim_transformed_text_and_tokens(
    text: str,
    tokens: list[ChapterToken] | None,
) -> tuple[str, list[ChapterToken] | None]:
    if not text:
        return "", tokens
    left = 0
    right = len(text)
    while left < right and text[left].isspace():
        left += 1
    while right > left and text[right - 1].isspace():
        right -= 1
    if left == 0 and right == len(text):
        return text, tokens
    trimmed = text[left:right]
    if not tokens:
        return trimmed, tokens
    for token in tokens:
        start = token.transformed_start if token.transformed_start is not None else 0
        end = token.transformed_end if token.transformed_end is not None else start
        start = max(left, min(start, right))
        end = max(start, min(end, right))
        token.transformed_start = max(0, start - left)
        token.transformed_end = max(token.transformed_start, end - left)
    return trimmed, tokens


def _combine_text_fragments(fragments: list[_TextFragment]) -> tuple[str, list[PitchToken], list[_RubySpan]]:
    if not fragments:
        return "", [], []
    filtered = [fragment for fragment in fragments if fragment.text.strip()]
    if not filtered:
        return "", [], []
    text_parts: list[str] = []
    tokens: list[PitchToken] = []
    spans: list[_RubySpan] = []
    cursor = 0
    for fragment in filtered:
        trimmed_text, fragment_tokens, fragment_spans = _strip_fragment_newlines(fragment)
        if not trimmed_text:
            continue
        if text_parts:
            cursor += 2
        text_parts.append(trimmed_text)
        for token in fragment_tokens:
            tokens.append(replace(token, start=token.start + cursor, end=token.end + cursor))
        for span in fragment_spans:
            spans.append(
                _RubySpan(
                    start=span.start + cursor,
                    end=span.end + cursor,
                    base=span.base,
                    reading=span.reading,
                )
            )
        cursor += len(trimmed_text)
    combined_text = "\n\n".join(text_parts)
    return combined_text, tokens, spans


def _first_non_blank_line(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return None


def _line_looks_like_title_or_author(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if len(stripped) > 32:
        return False
    if any(ch in "。！？!?…" for ch in stripped):
        return False
    allowed_punct = {
        "・",
        "-",
        "‐",
        " ",
        "　",
        "／",
        "(",
        ")",
        "（",
        "）",
        "［",
        "］",
        "[",
        "]",
        "『",
        "』",
        "〈",
        "〉",
        "＝",
    }
    for ch in stripped:
        if ch in allowed_punct:
            continue
        code = ord(ch)
        if _is_cjk_char(ch):
            continue
        if 0x3040 <= code <= 0x30FF:
            continue
        if ch.isascii() and (ch.isalpha() or ch.isdigit()):
            continue
        return False
    return True


def _ensure_title_author_break(text: str) -> str:
    lines = text.splitlines()
    first_idx = second_idx = -1
    for idx, line in enumerate(lines):
        if line.strip():
            if first_idx == -1:
                first_idx = idx
            else:
                second_idx = idx
                break
    if first_idx == -1 or second_idx == -1:
        return text
    if not (_line_looks_like_title_or_author(lines[first_idx]) and _line_looks_like_title_or_author(lines[second_idx])):
        return text
    has_blank = any(not lines[idx].strip() for idx in range(first_idx + 1, second_idx))
    if has_blank:
        return text
    lines.insert(second_idx, "")
    return "\n".join(lines)


def _finalize_segment_text(
    raw_text: str,
    backend: "NLPBackend" | None,
    preset_tokens: list[PitchToken] | None = None,
    original_text: str | None = None,
    ruby_spans: list[_RubySpan] | None = None,
    unique_mapping: Mapping[str, str] | None = None,
    common_mapping: Mapping[str, str] | None = None,
    unique_sources: Mapping[str, str] | None = None,
    common_sources: Mapping[str, str] | None = None,
    context_rules: Mapping[str, _ContextRule] | None = None,
) -> tuple[str, list[PitchToken] | None, list[ChapterToken] | None]:
    del preset_tokens  # legacy parameter
    token_basis = original_text if original_text is not None else raw_text
    if not token_basis:
        return "", None, None
    if backend is None:
        normalized = _normalize_ellipsis(_normalize_katakana(_hiragana_to_katakana(token_basis)))
        return normalized, None, None
    tokens = _build_chapter_tokens_from_original(
        token_basis,
        backend,
        ruby_spans or [],
        unique_mapping or {},
        common_mapping or {},
        unique_sources or {},
        common_sources or {},
        context_rules or {},
    )
    rendered_text, finalized_tokens = _render_text_from_tokens(token_basis, tokens)
    rendered_text, finalized_tokens = _trim_transformed_text_and_tokens(rendered_text, finalized_tokens)
    rendered_text = _normalize_ellipsis(rendered_text)
    pitch_tokens = tokens_to_pitch_tokens(finalized_tokens)
    return rendered_text, pitch_tokens, finalized_tokens


def _extract_cover_image(zf: zipfile.ZipFile) -> CoverImage | None:
    try:
        opf_path = _find_opf_path(zf)
    except FileNotFoundError:
        return None
    try:
        opf_xml = _zip_read_text(zf, opf_path)
        root = ET.fromstring(opf_xml)
    except Exception:
        return None

    manifest: dict[str, dict[str, str | None]] = {}
    for elem in root.iter():
        if _strip_tag(elem.tag) != "manifest":
            continue
        for child in elem:
            if _strip_tag(child.tag) != "item":
                continue
            item_id = _get_attr(child, "id")
            href = _get_attr(child, "href")
            if not item_id or not href:
                continue
            manifest[item_id] = {
                "href": href,
                "media_type": _get_attr(child, "media-type"),
                "properties": _get_attr(child, "properties"),
            }

    def _candidate_from_id(item_id: str) -> dict[str, str | None] | None:
        return manifest.get(item_id)

    cover_candidates: list[dict[str, str | None]] = []
    cover_id: str | None = None
    for elem in root.iter():
        if _strip_tag(elem.tag) != "meta":
            continue
        name = _get_attr(elem, "name")
        content = _get_attr(elem, "content")
        if name and name.lower() == "cover" and content:
            cover_id = content.strip()
            break

    if cover_id:
        manifest_entry = _candidate_from_id(cover_id)
        if manifest_entry:
            cover_candidates.append(manifest_entry)

    for item in manifest.values():
        properties = (item.get("properties") or "").lower()
        media_type = (item.get("media_type") or "").lower()
        if "cover-image" in properties and media_type.startswith("image/"):
            cover_candidates.append(item)

    for item_id, item in manifest.items():
        href = (item.get("href") or "").lower()
        media_type = (item.get("media_type") or "").lower()
        if ("cover" in item_id.lower() or "cover" in href) and media_type.startswith("image/"):
            cover_candidates.append(item)

    if not cover_candidates:
        for item in manifest.values():
            media_type = (item.get("media_type") or "").lower()
            if media_type.startswith("image/"):
                cover_candidates.append(item)
                break

    seen: set[str] = set()
    for candidate in cover_candidates:
        href = candidate.get("href")
        if not href:
            continue
        resolved = _resolve_opf_href(opf_path, href)
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved not in zf.namelist():
            continue
        try:
            data = _zip_read_bytes(zf, resolved)
        except KeyError:
            continue
        return CoverImage(
            path=resolved,
            media_type=candidate.get("media_type"),
            data=data,
        )
    return None


def get_epub_cover(inp_epub: str) -> CoverImage | None:
    """
    Extract the declared cover image from an EPUB, if present.
    """
    with zipfile.ZipFile(inp_epub, "r") as zf:
        return _extract_cover_image(zf)


def _normalize_ws(s: str) -> str:
    # Collapse all whitespace; keep punctuation/kanji intact
    return "".join(s.split())


def _normalize_ellipsis(text: str) -> str:
    if not text:
        return text
    text = re.sub(r"\.{3,}", "…", text)
    text = re.sub(r"…{2,}", "…", text)
    return text


def _contains_cjk(s: str) -> bool:
    # Heuristic: any Han or iteration mark suggests kanji content
    for ch in s:
        code = ord(ch)
        if (0x4E00 <= code <= 0x9FFF) or ch in "々〆ヵヶ":
            return True
    return False


def _looks_like_ascii_word(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    has_alpha = False
    for ch in stripped:
        if ch.isalpha() and ch.isascii():
            has_alpha = True
        elif ch.isdigit() and ch.isascii():
            continue
        elif ch in {"-", "_", "'", "’", "・"}:
            continue
        else:
            return False
    return has_alpha


def _is_single_kanji_base(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    cjk_chars = [ch for ch in stripped if _is_cjk_char(ch)]
    return len(cjk_chars) == 1 and len(stripped) == len(cjk_chars)


def _soup_from_html(html: str) -> BeautifulSoup:
    stripped = html.lstrip()
    lower_head = stripped[:200].lower()
    xmlish = stripped.startswith("<?xml") or (
        "<html" in lower_head and "xmlns" in lower_head
    )

    if xmlish:
        for parser in ("lxml-xml", "xml"):
            try:
                return BeautifulSoup(html, parser)
            except FeatureNotFound:
                continue
            except Exception:
                continue

    for parser in ("html5lib", "lxml", "html.parser", "lxml-xml"):
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
                return BeautifulSoup(html, parser)
        except FeatureNotFound:
            continue
        except Exception:
            continue

    # Last resort without suppression; if this raises, propagate upstream.
    return BeautifulSoup(html, "lxml-xml")


def _ruby_base_text(ruby: Tag) -> str:
    """
    Extract base text from <ruby>, ignoring <rt>/<rp>. Supports legacy and <rb>.
    """
    # Prefer segmented <rb>
    rbs = ruby.find_all("rb", recursive=False)
    if rbs:
        base = "".join("".join(rb.stripped_strings) for rb in rbs)
        return base
    parts = []
    for child in ruby.children:
        if isinstance(child, NavigableString):
            parts.append(str(child))
        elif isinstance(child, Tag) and child.name not in ("rt", "rp"):
            parts.append("".join(child.stripped_strings))
    return "".join(parts)


def _ruby_reading_text(ruby: Tag) -> str:
    """
    Concatenate direct <rt> readings. If none, fallback to ruby text.
    """
    rts = ruby.find_all("rt", recursive=False)
    if rts:
        return "".join("".join(rt.stripped_strings) for rt in rts)
    # Rare fallback
    return "".join(ruby.stripped_strings)


def _is_hiragana_or_katakana(ch: str) -> bool:
    if not ch:
        return False
    code = ord(ch)
    return (
        0x3041 <= code <= 0x309F  # Hiragana
        or 0x30A1 <= code <= 0x30FF  # Katakana
        or ch in "ー"
    )


def _is_kana_string(text: str) -> bool:
    for ch in text:
        if ch.isspace():
            continue
        if _is_hiragana_or_katakana(ch) or ch in {"・"}:
            continue
        return False
    return True


def _collect_kana_suffix(ruby: Tag) -> str:
    """
    Capture contiguous kana characters immediately following a <ruby>.
    Used to provide additional context (okurigana) when validating readings.
    """
    suffix_chars: list[str] = []
    node = ruby.next_sibling
    while node is not None:
        if isinstance(node, NavigableString):
            text = str(node)
        elif isinstance(node, Tag):
            if node.name in ("rt", "rp"):
                node = node.next_sibling
                continue
            if node.name == "ruby":
                break
            text = "".join(node.stripped_strings)
        else:
            break
        idx = 0
        while idx < len(text):
            ch = text[idx]
            if ch.isspace():
                if suffix_chars:
                    return "".join(suffix_chars)
                idx += 1
                continue
            if _is_hiragana_or_katakana(ch):
                suffix_chars.append(ch)
                idx += 1
                continue
            return "".join(suffix_chars)
        node = node.next_sibling
    return "".join(suffix_chars)


def _load_corpus_reading_accumulators() -> dict[str, _ReadingAccumulator]:
    global _CORPUS_READING_CACHE
    if _CORPUS_READING_CACHE is not None:
        return _CORPUS_READING_CACHE
    cache: dict[str, _ReadingAccumulator] = {}
    try:
        data_path = resources.files("nk.data").joinpath("nhk_easy_readings.json")
        data = json.loads(data_path.read_text("utf-8"))
    except (FileNotFoundError, ModuleNotFoundError, OSError, json.JSONDecodeError):
        data = []
    for entry in data:
        base_raw = entry.get("base", "")
        reading_raw = entry.get("reading", "")
        suffix_raw = entry.get("suffix", "")
        count = entry.get("count", 0)
        try:
            count = int(count)
        except (TypeError, ValueError):
            count = 0
        if not base_raw or not reading_raw or count <= 0:
            continue
        base_norm = unicodedata.normalize("NFKC", base_raw)
        reading_norm = _normalize_katakana(_hiragana_to_katakana(reading_raw))
        if not reading_norm:
            continue
        suffix_norm = _normalize_katakana(_hiragana_to_katakana(suffix_raw or ""))
        has_hira = any(0x3040 <= ord(ch) <= 0x309F for ch in reading_raw)
        flags = _ReadingFlags(
            has_hiragana=has_hira,
            has_latin=any("LATIN" in unicodedata.name(ch, "") for ch in reading_raw),
            has_middle_dot="・" in reading_raw,
            has_long_mark="ー" in reading_raw,
        )
        accumulator = _ReadingAccumulator()
        accumulator.counts[reading_norm] = count
        accumulator.total = count
        accumulator.flags[reading_norm] = flags
        suffix_entries = entry.get("suffixes")
        if isinstance(suffix_entries, list):
            for suffix_entry in suffix_entries:
                if not isinstance(suffix_entry, dict):
                    continue
                suffix_value = suffix_entry.get("value")
                suffix_count = suffix_entry.get("count")
                if not isinstance(suffix_value, str):
                    continue
                try:
                    suffix_count_int = int(suffix_count)
                except (TypeError, ValueError):
                    suffix_count_int = 0
                if suffix_count_int <= 0:
                    continue
                normalized_suffix = _normalize_katakana(_hiragana_to_katakana(suffix_value))
                accumulator.suffix_counts[normalized_suffix] += suffix_count_int
                if (
                    normalized_suffix
                    and normalized_suffix not in accumulator.suffix_samples
                    and len(accumulator.suffix_samples) < _MAX_SUFFIX_SAMPLES
                ):
                    accumulator.suffix_samples.append(normalized_suffix)
        elif suffix_norm:
            accumulator.suffix_counts[suffix_norm] = count
            if suffix_norm not in accumulator.suffix_samples:
                accumulator.suffix_samples.append(suffix_norm)
        prefix_entries = entry.get("prefixes")
        if isinstance(prefix_entries, list):
            for prefix_entry in prefix_entries:
                if not isinstance(prefix_entry, dict):
                    continue
                prefix_value = prefix_entry.get("value")
                prefix_count = prefix_entry.get("count")
                if not isinstance(prefix_value, str):
                    continue
                try:
                    prefix_count_int = int(prefix_count)
                except (TypeError, ValueError):
                    prefix_count_int = 0
                if prefix_count_int <= 0:
                    continue
                prefix_norm = _normalize_numeric_prefix(prefix_value)
                if not prefix_norm:
                    continue
                accumulator.prefix_counts[prefix_norm] += prefix_count_int
                if (
                    prefix_norm not in accumulator.prefix_samples
                    and len(accumulator.prefix_samples) < _MAX_PREFIX_SAMPLES
                ):
                    accumulator.prefix_samples.append(prefix_norm)
        accumulator.single_kanji_only = _is_single_kanji_base(base_norm)
        cache[base_norm] = accumulator
    _CORPUS_READING_CACHE = cache
    return cache


def _collect_reading_counts_from_soup(soup: BeautifulSoup) -> dict[str, _ReadingAccumulator]:
    accumulators: dict[str, _ReadingAccumulator] = defaultdict(_ReadingAccumulator)
    def _previous_significant_sibling(tag: Tag):
        prev = tag.previous_sibling
        while isinstance(prev, NavigableString) and not prev.strip():
            prev = prev.previous_sibling
        return prev

    def _next_significant_sibling(tag: Tag):
        nxt = tag.next_sibling
        while isinstance(nxt, NavigableString) and not nxt.strip():
            nxt = nxt.next_sibling
        return nxt

    for ruby in soup.find_all("ruby"):
        base_raw = _normalize_ws(_ruby_base_text(ruby))
        if not base_raw:
            continue
        base_norm = unicodedata.normalize("NFKC", base_raw)
        if not (_contains_cjk(base_norm) or _looks_like_ascii_word(base_norm)):
            continue
        reading_raw = _ruby_reading_text(ruby)
        reading_norm = _normalize_ws(reading_raw)
        reading_norm = unicodedata.normalize("NFKC", reading_norm)
        reading_norm = _hiragana_to_katakana(reading_norm)
        reading_norm = _normalize_katakana(reading_norm)
        if not reading_norm or not _is_kana_string(reading_norm):
            continue
        has_hira = any(0x3040 <= ord(ch) <= 0x309F for ch in reading_raw)
        suffix = _collect_kana_suffix(ruby)
        accumulator = accumulators[base_norm]
        accumulator.register(base_norm, reading_norm, reading_raw, has_hira, suffix, "")

        if not _is_single_kanji_base(base_norm):
            continue

        prev = _previous_significant_sibling(ruby)
        if isinstance(prev, Tag) and prev.name == "ruby":
            prev_base = _normalize_ws(_ruby_base_text(prev))
            prev_base_norm = unicodedata.normalize("NFKC", prev_base)
            if prev_base and _is_single_kanji_base(prev_base_norm):
                continue

        group: list[Tag] = [ruby]
        next_tag = _next_significant_sibling(ruby)
        while isinstance(next_tag, Tag) and next_tag.name == "ruby":
            next_base_raw = _normalize_ws(_ruby_base_text(next_tag))
            if not next_base_raw:
                break
            next_base_norm = unicodedata.normalize("NFKC", next_base_raw)
            if not _is_single_kanji_base(next_base_norm):
                break
            group.append(next_tag)
            next_tag = _next_significant_sibling(next_tag)

        if len(group) <= 1:
            continue

        combined_base_raw = "".join(_normalize_ws(_ruby_base_text(tag)) for tag in group)
        combined_base_norm = unicodedata.normalize("NFKC", combined_base_raw)
        combined_reading_raw = "".join(_ruby_reading_text(tag) for tag in group)
        combined_reading_norm = _normalize_ws(combined_reading_raw)
        combined_reading_norm = unicodedata.normalize("NFKC", combined_reading_norm)
        combined_reading_norm = _hiragana_to_katakana(combined_reading_norm)
        combined_reading_norm = _normalize_katakana(combined_reading_norm)
        if not combined_reading_norm or not _is_kana_string(combined_reading_norm):
            continue
        combined_has_hira = any(
            any(0x3040 <= ord(ch) <= 0x309F for ch in _ruby_reading_text(tag))
            for tag in group
        )
        suffix = _collect_kana_suffix(group[-1])
        compound_acc = accumulators[combined_base_norm]
        compound_acc.register(
            combined_base_norm,
            combined_reading_norm,
            combined_reading_raw,
            combined_has_hira,
            suffix,
            "",
        )
    return accumulators


def _looks_like_translation(flags: _ReadingFlags, reading: str) -> bool:
    if flags.has_latin or flags.has_middle_dot:
        return True
    if not flags.has_hiragana and flags.has_long_mark and len(reading) >= 4:
        return True
    return False


def _is_likely_name_candidate(base: str, flags: _ReadingFlags) -> bool:
    stripped = base.strip()
    if not stripped:
        return False
    if flags.has_latin or flags.has_middle_dot:
        return False
    cjk_chars = [ch for ch in stripped if _is_cjk_char(ch)]
    if len(cjk_chars) < 2 or len(cjk_chars) > 4:
        return False
    if len(cjk_chars) != len(stripped):
        return False
    if not flags.has_hiragana and not flags.has_long_mark:
        return False
    return True


def _reading_matches(candidate: str, variants: set[str]) -> bool:
    if not variants:
        return False
    target = _normalize_katakana(candidate)
    for variant in variants:
        if target == _normalize_katakana(variant):
            return True
    return False


def _strip_small_kana_variants(text: str) -> str:
    return "".join(SMALL_KANA_BASE_MAP.get(ch, ch) for ch in text)


def _differs_only_by_small_kana(a: str, b: str) -> bool:
    if len(a) != len(b):
        return False
    if _strip_small_kana_variants(a) != _strip_small_kana_variants(b):
        return False
    for ch_a, ch_b in zip(a, b):
        if ch_a == ch_b:
            continue
        if ch_a not in SMALL_KANA_SET and ch_b not in SMALL_KANA_SET:
            return False
    return True


def _aligned_variant_for_small_kana(reading: str, variants: set[str]) -> str | None:
    for variant in variants:
        if _differs_only_by_small_kana(reading, variant):
            return variant
    return None


def _reading_variants_for_base(
    base: str,
    accumulator: _ReadingAccumulator,
    nlp: "NLPBackend",
) -> set[str]:
    variants: set[str] = set()
    if hasattr(nlp, "reading_variants"):
        try:
            raw_variants = nlp.reading_variants(base)
        except Exception:
            raw_variants = set()
        if raw_variants:
            variants.update(raw_variants)
    if not hasattr(nlp, "to_reading_text"):
        return variants
    suffix_entries: list[str] = []
    if accumulator.suffix_samples:
        for suffix in accumulator.suffix_samples:
            if not suffix or suffix in suffix_entries:
                continue
            suffix_entries.append(suffix)
            if len(suffix_entries) >= _MAX_SUFFIX_CONTEXTS:
                break
    if not suffix_entries:
        suffix_entries = [
            suffix for suffix, _ in accumulator.suffix_counts.most_common(5) if suffix
        ]
    seen_suffixes: set[str] = set()
    for suffix in suffix_entries:
        if suffix in seen_suffixes:
            continue
        seen_suffixes.add(suffix)
        combined = f"{base}{suffix}"
        reading = nlp.to_reading_text(combined)
        reading_norm = _normalize_katakana(_hiragana_to_katakana(reading))
        suffix_norm = _normalize_katakana(_hiragana_to_katakana(suffix))
        if suffix_norm and reading_norm.endswith(suffix_norm):
            reading_norm = reading_norm[: -len(suffix_norm)]
        reading_norm = reading_norm.strip()
        if reading_norm:
            variants.add(reading_norm)
    return variants


def _select_reading_mapping(
    accumulators: dict[str, _ReadingAccumulator],
    nlp: "NLPBackend",
) -> tuple[dict[str, str], dict[str, str], dict[str, _ContextRule]]:
    tier3: dict[str, str] = {}
    tier2: dict[str, str] = {}
    context_rules: dict[str, _ContextRule] = {}

    def _maybe_register_rule(base: str, accumulator: _ReadingAccumulator) -> None:
        if base in context_rules:
            return
        rule = _context_rule_for_accumulator(base, accumulator)
        if rule:
            context_rules[base] = rule

    for base, accumulator in accumulators.items():
        if not accumulator.counts:
            continue
        if accumulator.single_kanji_only:
            continue
        top_reading, top_count = accumulator.counts.most_common(1)[0]
        total = accumulator.total or top_count
        share = top_count / total
        alt_share = max(
            (count / total for reading, count in accumulator.counts.items() if reading != top_reading),
            default=0.0,
        )
        flags = accumulator.flags.get(top_reading, _ReadingFlags())
        if _looks_like_ascii_word(base):
            tier3[base] = top_reading
            _maybe_register_rule(base, accumulator)
            continue
        if _looks_like_translation(flags, top_reading):
            continue
        if alt_share >= 0.3:
            continue

        variants = _reading_variants_for_base(base, accumulator, nlp)
        if _reading_matches(top_reading, variants):
            tier3[base] = top_reading
            _maybe_register_rule(base, accumulator)
            continue
        aligned_variant = _aligned_variant_for_small_kana(top_reading, variants)
        if aligned_variant:
            tier3[base] = aligned_variant
            _maybe_register_rule(base, accumulator)
            continue
        if share >= 0.9 and _is_likely_name_candidate(base, flags):
            tier3[base] = top_reading
            _maybe_register_rule(base, accumulator)
            continue
        if total >= 3 and share >= 0.95:
            tier3[base] = top_reading
            _maybe_register_rule(base, accumulator)

    return tier3, tier2, context_rules


def _build_book_mapping(
    zf: zipfile.ZipFile,
    nlp: "NLPBackend",
) -> tuple[
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, _ContextRule],
    list[dict[str, object]],
]:
    accumulators: dict[str, _ReadingAccumulator] = defaultdict(_ReadingAccumulator)
    base_sources: dict[str, str] = {}
    for name in zf.namelist():
        if not name.lower().endswith(HTML_EXTS):
            continue
        html = _zip_read_text(zf, name)
        original_plain_text = _strip_html_to_text(_soup_from_html(html))
        soup = _soup_from_html(html)
        partial = _collect_reading_counts_from_soup(soup)
        for base, partial_acc in partial.items():
            accumulators[base].merge_from(partial_acc)
            base_sources.setdefault(base, "propagation")
    corpus_accumulators = _load_corpus_reading_accumulators()
    if corpus_accumulators:
        for base, corpus_acc in corpus_accumulators.items():
            existing = accumulators.get(base)
            if existing is None:
                existing = accumulators[base]
            if existing.total == 0:
                existing.merge_from(corpus_acc)
                base_sources.setdefault(base, "nhk")
    tier3, tier2, context_rules = _select_reading_mapping(accumulators, nlp)
    tier3_sources = {base: base_sources.get(base, "propagation") for base in tier3}
    tier2_sources = {base: base_sources.get(base, "propagation") for base in tier2}
    evidence_payload = _serialize_ruby_evidence(accumulators)
    return tier3, tier2, tier3_sources, tier2_sources, context_rules, evidence_payload


def _serialize_ruby_evidence(accumulators: Mapping[str, _ReadingAccumulator]) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for base, accumulator in accumulators.items():
        if not accumulator.counts:
            continue
        top_reading, top_count = accumulator.counts.most_common(1)[0]
        suffixes: list[dict[str, object]] = []
        for value, count in accumulator.suffix_counts.most_common(_MAX_SUFFIX_CONTEXTS):
            suffixes.append({"value": value, "count": count})
        prefixes: list[dict[str, object]] = []
        for value, count in accumulator.prefix_counts.most_common(_MAX_PREFIX_CONTEXTS):
            prefixes.append({"value": value, "count": count})
        entry: dict[str, object] = {
            "base": base,
            "reading": top_reading,
            "count": top_count,
            "suffix": suffixes[0]["value"] if suffixes else "",
            "suffixes": suffixes,
        }
        if prefixes:
            entry["prefixes"] = prefixes
        entries.append(entry)
    entries.sort(key=lambda item: (-int(item.get("count", 0)), item.get("base", "")))
    return entries


def _replace_outside_ruby_with_readings(
    soup: BeautifulSoup,
    mapping: dict[str, str],
    tracker: _TransformationTracker | None = None,
    source_labels: dict[str, str] | None = None,
    context_rules: dict[str, _ContextRule] | None = None,
) -> None:
    """
    Replace text nodes NOT inside ruby/rt/rp/script/style using {base->reading}.
    Longest-match-first to avoid swallowing shorter substrings.
    """
    pat = _build_mapping_pattern(mapping)
    if pat is None:
        return
    for node in list(soup.find_all(string=True)):
        parent = node.parent if isinstance(node.parent, Tag) else None
        if parent and parent.name in {"script", "style"}:
            continue
        if hasattr(node, "find_parent") and node.find_parent("ruby") is not None:
            continue
        if parent and parent.name in {"rt", "rp"}:
            continue
        text = unicodedata.normalize("NFKC", str(node))
        if not text.strip():
            continue
        new_text = _apply_mapping_with_pattern(
            text,
            mapping,
            pat,
            tracker=tracker,
            source_labels=source_labels,
            context_rules=context_rules,
        )
        if new_text != text:
            node.replace_with(new_text)


def _collapse_ruby_to_readings(
    soup: BeautifulSoup,
    tracker: _TransformationTracker | None = None,
) -> None:
    """
    Replace each <ruby> with its reading only (concat of direct <rt> contents).
    """
    for ruby in list(soup.find_all("ruby")):
        reading = _hiragana_to_katakana(_ruby_reading_text(ruby))
        reading = _normalize_katakana(reading)
        replacement = reading
        if tracker:
            base_raw = _normalize_ws(_ruby_base_text(ruby))
            base_norm = unicodedata.normalize("NFKC", base_raw)
            replacement = tracker.wrap(base_norm, reading, ("ruby",))
        ruby.replace_with(replacement)


def _strip_html_to_text(soup: BeautifulSoup) -> str:
    for node in list(soup.contents):
        if isinstance(node, Doctype):
            node.extract()
        elif isinstance(node, NavigableString):
            stripped = str(node).strip()
            if stripped and stripped.upper().startswith("HTML PUBLIC"):
                node.extract()
    # Remove rp/script/style
    for t in soup.find_all(["rp", "script", "style"]):
        t.decompose()
    for t in soup.find_all("title"):
        t.decompose()
    # Convert <br> to explicit newlines so they survive text extraction.
    for br in soup.find_all("br"):
        br.replace_with("\n")
    # Ensure block-level elements start on a new line, but avoid double-
    # counting nested blocks except for the small set that should always break.
    for tag in soup.find_all(BLOCK_LEVEL_TAGS):
        if tag.name in FORCE_BREAK_TAGS or not tag.find_parent(BLOCK_LEVEL_TAGS):
            tag.insert_before("\n")
    # Generate plain text without inserting extra separators between inline nodes.
    txt = soup.get_text(separator="")
    txt = unicodedata.normalize("NFKC", txt)
    txt = re.sub(r"[ \t]+\n", "\n", txt)
    txt = txt.replace("〝", '"').replace("〟", '"')
    txt = _normalize_ellipsis(txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt


def epub_to_chapter_texts(
    inp_epub: str,
    nlp: "NLPBackend" | None = None,
) -> tuple[list[ChapterText], list[dict[str, object]]]:
    """
    Convert an EPUB into chapterized text segments with ruby expansion.

    Returns the processed spine items in order as ChapterText objects.
    """
    backend = nlp
    if backend is None:
        from .nlp import NLPBackend  # Local import to avoid costly dependency during module import.

        backend = NLPBackend()
    with zipfile.ZipFile(inp_epub, "r") as zf:
        (
            unique_mapping,
            common_mapping,
            unique_sources,
            common_sources,
            context_rules,
            ruby_evidence,
        ) = _build_book_mapping(zf, backend)
        spine = _spine_items(zf)
        nav_points = _toc_nav_points(zf, spine)
        nav_buckets: dict[int, dict[str, list[object]]] = {
            entry.order: {"text_fragments": [], "original_parts": []} for entry in nav_points
        }
        nav_by_spine: dict[int, list[_NavPoint]] = defaultdict(list)
        for entry in nav_points:
            entry.local_order = len(nav_by_spine[entry.spine_index])
            nav_by_spine[entry.spine_index].append(entry)
        fallback_segments: list[_FallbackSegment] = []
        fallback_sequence = 0
        book_title = _get_book_title(zf)
        book_author = _get_book_author(zf)
        title_candidates: list[str] = []
        if book_title:
            normalized_title = unicodedata.normalize("NFKC", book_title).strip()
            if normalized_title:
                title_candidates.append(normalized_title)
                title_variant = unicodedata.normalize(
                    "NFKC",
                    _apply_mapping_to_plain_text(normalized_title, unique_mapping, context_rules),
                )
                title_variant = _apply_mapping_to_plain_text(title_variant, common_mapping, context_rules)
                variant_stripped = title_variant.strip()
                if variant_stripped and variant_stripped not in title_candidates:
                    title_candidates.append(variant_stripped)
                if backend is not None:
                    candidates = {
                        backend.to_reading_text(normalized_title).strip(),
                        backend.to_reading_text(variant_stripped or normalized_title).strip(),
                    }
                    for cand in candidates:
                        if cand and cand not in title_candidates:
                            title_candidates.append(cand)
        title_seen = False

        for spine_index, name in enumerate(spine):
            if name not in zf.namelist():
                # Some spines use relative paths; try to resolve simply
                candidates = [
                    n for n in zf.namelist() if n.endswith("/" + name) or n.endswith(name)
                ]
                if candidates:
                    name = candidates[0]
                else:
                    continue
            if not name.lower().endswith(HTML_EXTS):
                continue
            html = _zip_read_text(zf, name)
            nav_entries_for_file = nav_by_spine.get(spine_index, [])
            original_soup = _soup_from_html(html)
            ruby_tracker = _RubySpanTracker()
            ruby_tracker.mark_soup(original_soup)
            for rt in original_soup.find_all("rt"):
                rt.decompose()
            _insert_nav_markers(original_soup, nav_entries_for_file)
            original_marked_text = _strip_html_to_text(original_soup)
            original_plain_text, ruby_spans = ruby_tracker.extract(original_marked_text)
            soup = _soup_from_html(html)
            tracker = _TransformationTracker()
            # 1) propagate: replace base outside ruby using the global mapping
            _replace_outside_ruby_with_readings(
                soup,
                unique_mapping,
                tracker=tracker,
                source_labels=unique_sources,
                context_rules=context_rules,
            )
            _replace_outside_ruby_with_readings(
                soup,
                common_mapping,
                tracker=tracker,
                source_labels=common_sources,
                context_rules=context_rules,
            )
            _insert_nav_markers(soup, nav_entries_for_file)
            # 2) drop bases inside ruby, keep only readings
            _collapse_ruby_to_readings(soup, tracker=tracker)
            # 3) strip remaining html to text
            piece = _strip_html_to_text(soup)
            piece = _apply_mapping_to_plain_text(
                piece,
                unique_mapping,
                context_rules,
                tracker=tracker,
                source_labels=unique_sources,
            )
            piece = _apply_mapping_to_plain_text(
                piece,
                common_mapping,
                context_rules,
                tracker=tracker,
                source_labels=common_sources,
            )
            filtered_lines: list[str] = []
            skip_blank_after_title = False
            for line in piece.splitlines():
                stripped_line = line.strip()
                if not stripped_line:
                    if skip_blank_after_title:
                        skip_blank_after_title = False
                        continue
                    filtered_lines.append(line)
                    continue

                normalized_line = stripped_line.replace("\u3000", " ")
                is_title_line = bool(title_candidates) and (
                    stripped_line in title_candidates
                    or normalized_line in title_candidates
                )

                if is_title_line:
                    if title_seen:
                        skip_blank_after_title = True
                        continue
                    title_seen = True
                    skip_blank_after_title = True
                else:
                    skip_blank_after_title = False

                filtered_lines.append(line)

            raw_piece_text = "\n".join(filtered_lines)
            raw_piece_text = re.sub(r"\n{3,}", "\n\n", raw_piece_text)
            raw_piece_text = raw_piece_text.strip()
            if not raw_piece_text:
                continue
            leading_piece, piece_segments = _split_text_by_markers(raw_piece_text)
            leading_fragment = _fragment_from_tracker(leading_piece, tracker)
            leading_original, original_segments = _split_text_by_markers(original_plain_text)
            original_segment_map = {marker: (segment, start, end) for marker, segment, start, end in original_segments}
            for marker_id, segment_text, _, _ in piece_segments:
                bucket = nav_buckets.get(marker_id)
                if bucket is None:
                    continue
                if not segment_text.strip():
                    continue
                fragment = _fragment_from_tracker(segment_text, tracker)
                original_segment, seg_start, seg_end = original_segment_map.get(marker_id, ("", 0, 0))
                fragment.ruby_spans = _slice_ruby_spans(ruby_spans, seg_start, seg_end)
                bucket["text_fragments"].append(fragment)
                bucket["original_parts"].append(original_segment)
            if leading_fragment.text.strip():
                order = -1 if nav_entries_for_file else 0
                leading_span_list = _slice_ruby_spans(ruby_spans, 0, len(leading_original))
                leading_fragment.ruby_spans = leading_span_list
                fallback_segments.append(
                    _FallbackSegment(
                        spine_index=spine_index,
                        order=order,
                        sequence=fallback_sequence,
                        source=name,
                        fragment=leading_fragment,
                        raw_original=leading_original or leading_fragment.text,
                    )
                )
                fallback_sequence += 1

        pending_outputs: list[_PendingChapter] = []
        for segment in fallback_segments:
            fragment_text = segment.fragment.text
            if not fragment_text.strip():
                continue
            pending_outputs.append(
                _PendingChapter(
                    sort_key=(segment.spine_index, segment.order, segment.sequence),
                    source=segment.source,
                    raw_text=fragment_text,
                    raw_original=segment.raw_original,
                    title_hint=None,
                    tokens=list(segment.fragment.tokens),
                    ruby_spans=list(segment.fragment.ruby_spans or []),
                )
            )
        for entry in nav_points:
            bucket = nav_buckets.get(entry.order)
            if not bucket:
                continue
            fragments = bucket.get("text_fragments", [])
            raw_text, fragment_tokens, fragment_spans = _combine_text_fragments(fragments)
            if not raw_text.strip():
                continue
            original_parts = [part for part in bucket["original_parts"] if part.strip()]
            raw_original = "\n\n".join(part.strip("\n") for part in original_parts) if original_parts else raw_text
            pending_outputs.append(
                _PendingChapter(
                    sort_key=(entry.spine_index, entry.local_order + 1, entry.order),
                    source=entry.path,
                    raw_text=raw_text,
                    raw_original=raw_original,
                    title_hint=entry.title.strip() if entry.title else None,
                    tokens=fragment_tokens,
                    ruby_spans=fragment_spans,
                )
            )

        chapters: list[ChapterText] = []
        for pending in sorted(pending_outputs, key=lambda item: item.sort_key):
            original_basis = pending.raw_original if pending.raw_original is not None else pending.raw_text
            processing_basis = original_basis
            if not chapters:
                processing_basis = _ensure_title_author_break(processing_basis)
            finalized_text, pitch_tokens, chapter_tokens = _finalize_segment_text(
                pending.raw_text,
                backend,
                preset_tokens=pending.tokens,
                original_text=processing_basis,
                ruby_spans=pending.ruby_spans,
                unique_mapping=unique_mapping,
                common_mapping=common_mapping,
                unique_sources=unique_sources,
                common_sources=common_sources,
                context_rules=context_rules,
            )
            if not finalized_text:
                continue
            original_title = _first_non_blank_line(original_basis)
            processed_title = _first_non_blank_line(finalized_text)
            title = processed_title or pending.title_hint
            original_title = original_title or pending.title_hint or title
            chapters.append(
                ChapterText(
                    source=pending.source,
                    title=title,
                    text=finalized_text,
                    original_text=original_basis,
                    original_title=original_title,
                    book_title=book_title,
                    book_author=book_author,
                    pitch_data=pitch_tokens,
                    tokens=chapter_tokens,
                )
            )

        return chapters, ruby_evidence


def epub_to_txt(
    inp_epub: str,
    nlp: "NLPBackend" | None = None,
) -> str:
    """
    Convert an EPUB into plain text with ruby expansion.

    Advanced mode verifies ruby readings with an NLP backend, keeps the ones
    that match or dominate in-book evidence, and fills remaining kanji with
    dictionary readings.
    """
    chapters, _ = epub_to_chapter_texts(inp_epub, nlp=nlp)
    combined = "\n\n".join(chapter.text for chapter in chapters).strip()
    return combined


__all__ = ["ChapterText", "CoverImage", "epub_to_chapter_texts", "epub_to_txt", "get_epub_cover"]
