from __future__ import annotations

import re
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter, defaultdict
from pathlib import PurePosixPath

from bs4 import BeautifulSoup, NavigableString, Tag  # type: ignore

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


def _is_cjk_char(ch: str) -> bool:
    if not ch:
        return False
    code = ord(ch)
    return (
        0x4E00 <= code <= 0x9FFF  # CJK Unified Ideographs
        or 0x3400 <= code <= 0x4DBF  # Extension A
        or 0x20000 <= code <= 0x2A6DF  # Extension B
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


def _zip_read_text(zf: zipfile.ZipFile, name: str) -> str:
    raw = zf.read(name)
    for enc in ("utf-8", "utf-16", "cp932", "shift_jis", "euc_jp"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


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


def _normalize_ws(s: str) -> str:
    # Collapse all whitespace; keep punctuation/kanji intact
    return "".join(s.split())


def _contains_cjk(s: str) -> bool:
    # Heuristic: any Han or iteration mark suggests kanji content
    for ch in s:
        code = ord(ch)
        if (0x4E00 <= code <= 0x9FFF) or ch in "々〆ヵヶ":
            return True
    return False


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


def _collect_reading_counts_from_soup(soup: BeautifulSoup) -> dict[str, Counter[str]]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for ruby in soup.find_all("ruby"):
        base = _normalize_ws(_ruby_base_text(ruby))
        reading = _normalize_ws(_ruby_reading_text(ruby))
        reading = _hiragana_to_katakana(reading)
        if base and reading and _contains_cjk(base):
            counts[base][reading] += 1
    return counts


def _select_reading_mapping(
    read_counts: dict[str, Counter[str]]
) -> tuple[dict[str, str], dict[str, str]]:
    unique_mapping: dict[str, str] = {}
    common_mapping: dict[str, str] = {}
    for base, counter in read_counts.items():
        if not counter:
            continue
        if len(counter) == 1:
            chosen_reading = counter.most_common(1)[0][0]
            unique_mapping[base] = chosen_reading
        else:
            most_common = counter.most_common()
            top_reading, top_count = most_common[0]
            second_count = most_common[1][1] if len(most_common) > 1 else 0
            if top_count > second_count:
                common_mapping[base] = top_reading
    return unique_mapping, common_mapping


def _build_book_mapping(zf: zipfile.ZipFile) -> tuple[dict[str, str], dict[str, str]]:
    read_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for name in zf.namelist():
        if not name.lower().endswith(HTML_EXTS):
            continue
        html = _zip_read_text(zf, name)
        soup = BeautifulSoup(html, "lxml-xml")
        partial_counts = _collect_reading_counts_from_soup(soup)
        for base, counter in partial_counts.items():
            read_counts[base].update(counter)
    return _select_reading_mapping(read_counts)


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
        text = str(node)
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
        ruby.replace_with(reading)


def _strip_html_to_text(soup: BeautifulSoup) -> str:
    # Remove rp/script/style
    for t in soup.find_all(["rp", "script", "style"]):
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
    # Normalize Unicode NFKC to standardize fullwidth/halfwidth variants.
    txt = unicodedata.normalize("NFKC", txt)
    # Trim trailing ASCII whitespace before newlines introduced above.
    txt = re.sub(r"[ \t]+\n", "\n", txt)
    # Harmonize symbols for TTS friendliness.
    txt = txt.replace("〝", '"').replace("〟", '"')
    txt = re.sub(r"\.{3,}", "…", txt)
    # Collapse excessive blank lines.
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt


def epub_to_txt(inp_epub: str) -> str:
    """
    Returns the final TXT as a single string.
    """
    with zipfile.ZipFile(inp_epub, "r") as zf:
        unique_mapping, common_mapping = _build_book_mapping(zf)
        spine = _spine_items(zf)
        book_title = _get_book_title(zf)
        if book_title:
            title_variant = unicodedata.normalize(
                "NFKC", _apply_mapping_to_plain_text(book_title, unique_mapping)
            )
            title_variant = _apply_mapping_to_plain_text(title_variant, common_mapping)
        else:
            title_variant = None
        title_seen = False

        pieces: list[str] = []
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
            soup = BeautifulSoup(html, "lxml-xml")
            # 1) propagate: replace base outside ruby using the global mapping
            _replace_outside_ruby_with_readings(soup, unique_mapping)
            _replace_outside_ruby_with_readings(soup, common_mapping)
            # 2) drop bases inside ruby, keep only readings
            _collapse_ruby_to_readings(soup)
            # 3) strip remaining html to text
            piece = _strip_html_to_text(soup)
            if book_title:
                filtered_lines: list[str] = []
                for line in piece.splitlines():
                    stripped_line = line.strip()
                    if stripped_line == book_title.strip() or (
                        title_variant and stripped_line == title_variant.strip()
                    ):
                        if title_seen:
                            continue
                        title_seen = True
                    filtered_lines.append(line)
                piece = "\n".join(filtered_lines).strip()
            pieces.append(piece)

        return "\n\n".join(pieces).strip()


__all__ = ["epub_to_txt"]
