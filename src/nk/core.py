from __future__ import annotations

import re
import warnings
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Literal

from bs4 import (
    BeautifulSoup,
    FeatureNotFound,
    NavigableString,
    Tag,
    XMLParsedAsHTMLWarning,
)  # type: ignore

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

PropagationMode = Literal["fast", "advanced"]


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

    def register(self, base: str, reading: str, raw_reading: str, has_hiragana: bool) -> None:
        self.total += 1
        self.counts[reading] += 1
        flags = self.flags.setdefault(reading, _ReadingFlags())
        flags.has_hiragana = flags.has_hiragana or has_hiragana
        flags.has_latin = flags.has_latin or any(
            "LATIN" in unicodedata.name(ch, "") for ch in raw_reading
        )
        flags.has_middle_dot = flags.has_middle_dot or ("・" in raw_reading)
        flags.has_long_mark = flags.has_long_mark or ("ー" in raw_reading)
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


@dataclass
class ChapterText:
    source: str
    title: str | None
    text: str
    original_title: str | None = None
    book_title: str | None = None


@dataclass
class CoverImage:
    path: str
    media_type: str | None
    data: bytes


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


def _build_mapping_pattern(mapping: dict[str, str]) -> re.Pattern[str] | None:
    if not mapping:
        return None
    keys = sorted(mapping.keys(), key=len, reverse=True)
    if not keys:
        return None
    return re.compile("|".join(re.escape(k) for k in keys))


def _apply_mapping_with_pattern(
    text: str, mapping: dict[str, str], pattern: re.Pattern[str]
) -> str:
    if pattern is None:
        return text

    def repl(match: re.Match[str]) -> str:
        base = match.group(0)
        if len(base) == 1:
            start, end = match.span()
            prev_ch = text[start - 1] if start > 0 else ""
            next_ch = text[end] if end < len(text) else ""
            if (_is_cjk_char(prev_ch) and prev_ch != "\n") or _is_cjk_char(next_ch):
                return base
            if base.isascii() and base.isalnum():
                if (prev_ch.isascii() and prev_ch.isalnum()) or (
                    next_ch.isascii() and next_ch.isalnum()
                ):
                    return base
        return mapping[base]

    return pattern.sub(repl, text)


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


def _apply_mapping_to_plain_text(text: str, mapping: dict[str, str]) -> str:
    pattern = _build_mapping_pattern(mapping)
    if pattern is None:
        return text
    return _apply_mapping_with_pattern(text, mapping, pattern)


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
    base = str(PurePosixPath(opf_path).parent)
    if base not in ("", ".", "/"):
        combined = PurePosixPath(base) / href
    else:
        combined = PurePosixPath(href)
    return str(combined.as_posix())


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
        accumulator = accumulators[base_norm]
        accumulator.register(base_norm, reading_norm, reading_raw, has_hira)

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
        compound_acc = accumulators[combined_base_norm]
        compound_acc.register(
            combined_base_norm,
            combined_reading_norm,
            combined_reading_raw,
            combined_has_hira,
        )
    return accumulators


def _looks_like_translation(flags: _ReadingFlags, reading: str) -> bool:
    if flags.has_latin or flags.has_middle_dot:
        return True
    if not flags.has_hiragana and flags.has_long_mark and len(reading) >= 4:
        return True
    return False


def _reading_matches(candidate: str, variants: set[str]) -> bool:
    if not variants:
        return False
    target = _normalize_katakana(candidate)
    for variant in variants:
        if target == _normalize_katakana(variant):
            return True
    return False


def _select_reading_mapping(
    accumulators: dict[str, _ReadingAccumulator],
    mode: PropagationMode,
    nlp: "NLPBackend" | None,
) -> tuple[dict[str, str], dict[str, str]]:
    tier3: dict[str, str] = {}
    tier2: dict[str, str] = {}

    for base, accumulator in accumulators.items():
        if not accumulator.counts or accumulator.total < 2:
            if mode != "advanced":
                continue
        if accumulator.single_kanji_only:
            continue
        top_reading, top_count = accumulator.counts.most_common(1)[0]
        total = accumulator.total
        share = top_count / total
        alt_share = max(
            (count / total for reading, count in accumulator.counts.items() if reading != top_reading),
            default=0.0,
        )
        flags = accumulator.flags.get(top_reading, _ReadingFlags())
        if _looks_like_ascii_word(base):
            tier3[base] = top_reading
            continue
        if _looks_like_translation(flags, top_reading):
            continue
        if alt_share >= 0.3:
            continue

        if mode == "fast":
            if total < 2:
                continue
            if share >= 0.95:
                tier3[base] = top_reading
            elif share >= 0.9 and total >= 3:
                tier2[base] = top_reading
        else:  # advanced
            if nlp is None:
                continue
            variants = nlp.reading_variants(base)
            if _reading_matches(top_reading, variants):
                tier3[base] = top_reading
                continue
            if total >= 3 and share >= 0.95:
                tier3[base] = top_reading

    return tier3, tier2


def _build_book_mapping(
    zf: zipfile.ZipFile,
    mode: PropagationMode,
    nlp: "NLPBackend" | None,
) -> tuple[dict[str, str], dict[str, str]]:
    accumulators: dict[str, _ReadingAccumulator] = defaultdict(_ReadingAccumulator)
    for name in zf.namelist():
        if not name.lower().endswith(HTML_EXTS):
            continue
        html = _zip_read_text(zf, name)
        original_plain_text = _strip_html_to_text(_soup_from_html(html))
        soup = _soup_from_html(html)
        partial = _collect_reading_counts_from_soup(soup)
        for base, partial_acc in partial.items():
            accumulators[base].merge_from(partial_acc)
    return _select_reading_mapping(accumulators, mode, nlp)


def _replace_outside_ruby_with_readings(soup: BeautifulSoup, mapping: dict[str, str]) -> None:
    """
    Replace text nodes NOT inside ruby/rt/rp/script/style using {base->reading}.
    Longest-match-first to avoid swallowing shorter substrings.
    """
    pat = _build_mapping_pattern(mapping)
    if pat is None:
        return
    for node in list(soup.find_all(string=True)):
        parent = node.parent.name if isinstance(node.parent, Tag) else None
        if parent in ("script", "style", "rt", "rp", "ruby"):
            continue
        text = unicodedata.normalize("NFKC", str(node))
        if not text.strip():
            continue
        new_text = _apply_mapping_with_pattern(text, mapping, pat)
        if new_text != text:
            node.replace_with(new_text)


def _collapse_ruby_to_readings(soup: BeautifulSoup) -> None:
    """
    Replace each <ruby> with its reading only (concat of direct <rt> contents).
    """
    for ruby in list(soup.find_all("ruby")):
        reading = _hiragana_to_katakana(_ruby_reading_text(ruby))
        reading = _normalize_katakana(reading)
        ruby.replace_with(reading)


def _strip_html_to_text(soup: BeautifulSoup) -> str:
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
    mode: PropagationMode = "advanced",
    nlp: "NLPBackend" | None = None,
) -> list[ChapterText]:
    """
    Convert an EPUB into chapterized text segments with ruby expansion.

    Returns the processed spine items in order as ChapterText objects.
    """
    if mode not in ("fast", "advanced"):
        raise ValueError(f"Unsupported mode '{mode}'. Expected 'fast' or 'advanced'.")
    backend = nlp
    if mode == "advanced" and backend is None:
        from .nlp import NLPBackend  # Local import to avoid mandatory dependency for fast mode.

        backend = NLPBackend()
    with zipfile.ZipFile(inp_epub, "r") as zf:
        unique_mapping, common_mapping = _build_book_mapping(zf, mode, backend)
        spine = _spine_items(zf)
        book_title = _get_book_title(zf)
        title_candidates: list[str] = []
        if book_title:
            normalized_title = unicodedata.normalize("NFKC", book_title).strip()
            if normalized_title:
                title_candidates.append(normalized_title)
                title_variant = unicodedata.normalize(
                    "NFKC", _apply_mapping_to_plain_text(normalized_title, unique_mapping)
                )
                title_variant = _apply_mapping_to_plain_text(title_variant, common_mapping)
                variant_stripped = title_variant.strip()
                if variant_stripped and variant_stripped not in title_candidates:
                    title_candidates.append(variant_stripped)
                if mode == "advanced" and backend is not None:
                    candidates = {
                        backend.to_reading_text(normalized_title).strip(),
                        backend.to_reading_text(variant_stripped or normalized_title).strip(),
                    }
                    for cand in candidates:
                        if cand and cand not in title_candidates:
                            title_candidates.append(cand)
        title_seen = False

        chapters: list[ChapterText] = []
        for name in spine:
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
            original_soup = _soup_from_html(html)
            for rt in original_soup.find_all("rt"):
                rt.decompose()
            original_plain_text = _strip_html_to_text(original_soup)
            soup = _soup_from_html(html)
            # 1) propagate: replace base outside ruby using the global mapping
            _replace_outside_ruby_with_readings(soup, unique_mapping)
            _replace_outside_ruby_with_readings(soup, common_mapping)
            # 2) drop bases inside ruby, keep only readings
            _collapse_ruby_to_readings(soup)
            # 3) strip remaining html to text
            piece = _strip_html_to_text(soup)
            filtered_lines: list[str] = []
            skip_blank_after_title = False
            has_content_in_piece = False
            first_non_blank_original: str | None = None
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
                if stripped_line and first_non_blank_original is None:
                    first_non_blank_original = line
                if stripped_line:
                    has_content_in_piece = True

            preserved_line_raw = first_non_blank_original.strip() if first_non_blank_original else None
            raw_piece_text = "\n".join(filtered_lines)
            raw_piece_text = re.sub(r"\n{3,}", "\n\n", raw_piece_text)
            raw_piece_text = raw_piece_text.strip()
            piece_text = raw_piece_text
            if mode == "advanced" and backend is not None and piece_text:
                piece_text = backend.to_reading_text(piece_text).strip()
                piece_text = _normalize_ellipsis(piece_text)
            if not piece_text:
                continue
            original_title = next(
                (line.strip() for line in original_plain_text.splitlines() if line.strip()),
                None,
            )
            if original_title is None:
                original_title = preserved_line_raw or next(
                    (line.strip() for line in raw_piece_text.splitlines() if line.strip()),
                    None,
                )
            processed_title = next(
                (line.strip() for line in piece_text.splitlines() if line.strip()),
                None,
            )
            chapters.append(
                ChapterText(
                    source=name,
                    title=processed_title,
                    text=piece_text,
                    original_title=original_title,
                    book_title=book_title,
                )
            )

        return chapters


def epub_to_txt(
    inp_epub: str,
    mode: PropagationMode = "advanced",
    nlp: "NLPBackend" | None = None,
) -> str:
    """
    Convert an EPUB into plain text with ruby expansion.

    `fast` mode uses only in-book ruby evidence.
    `advanced` mode verifies ruby readings with an NLP backend, keeps the ones
    that match or dominate in-book evidence, and fills remaining kanji with
    dictionary readings.
    """
    chapters = epub_to_chapter_texts(inp_epub, mode=mode, nlp=nlp)
    combined = "\n\n".join(chapter.text for chapter in chapters).strip()
    return combined


__all__ = ["ChapterText", "CoverImage", "epub_to_chapter_texts", "epub_to_txt", "get_epub_cover"]
