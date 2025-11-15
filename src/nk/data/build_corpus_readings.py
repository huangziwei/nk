#!/usr/bin/env python3
"""Aggregate NHK Easier rubies into nk's dictionary file."""

from __future__ import annotations

import argparse
import json
import sys
import unicodedata
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from bs4 import BeautifulSoup
from bs4.element import NavigableString

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from nk.core import (  # noqa: E402
    _is_kana_string,
    _normalize_katakana,
    _ruby_base_text,
    _ruby_reading_text,
    _soup_from_html,
)


def normalize_base(text: str) -> str:
    return unicodedata.normalize("NFKC", "".join((text or "").split()))


def normalize_reading(text: str) -> str:
    return _normalize_katakana(unicodedata.normalize("NFKC", "".join((text or "").split())))


@dataclass
class RubyRecord:
    base: str
    reading: str
    suffix: str
    prefix: str


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
        or ch in "々〆ヵヶ"
    )


def _is_single_kanji(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    cjk_chars = [ch for ch in stripped if _is_cjk_char(ch)]
    return len(cjk_chars) == 1 and len(cjk_chars) == len(stripped)


_NUMERIC_PREFIX_CHARS = set("0123456789０１２３４５６７８９一二三四五六七八九十百千〇零")
_MAX_PREFIX_CHARS = 4


def _collect_numeric_prefix(text: str, start: int) -> str:
    if start <= 0:
        return ""
    chars: list[str] = []
    idx = start - 1
    while idx >= 0 and len(chars) < _MAX_PREFIX_CHARS:
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
    return unicodedata.normalize("NFKC", "".join(reversed(chars)))


def _collect_suffix(text: str, end: int) -> str:
    if end >= len(text):
        return ""
    chars: list[str] = []
    idx = end
    while idx < len(text):
        ch = text[idx]
        if ch.isspace():
            if chars:
                break
            idx += 1
            continue
        code = ord(ch)
        if 0x3040 <= code <= 0x30FF or ch in {"ー", "ゝ", "ゞ"}:
            chars.append(ch)
            idx += 1
            continue
        break
    return normalize_reading("".join(chars))


def iter_ruby_records(epub: Path) -> Iterable[RubyRecord]:
    with zipfile.ZipFile(epub, "r") as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".xhtml") or "/text/" not in name:
                continue
            html = zf.read(name).decode("utf-8")
            soup = _soup_from_html(html)
            body = soup.body or soup
            if body is None:
                continue
            text_parts: list[str] = []
            spans: list[tuple[int, int, str, str, str]] = []
            cursor = 0

            def append(fragment: str) -> None:
                nonlocal cursor
                if not fragment:
                    return
                text_parts.append(fragment)
                cursor += len(fragment)

            def walk(node) -> None:
                nonlocal cursor
                for child in node.children:
                    if isinstance(child, NavigableString):
                        append(str(child))
                        continue
                    tag = getattr(child, "name", None)
                    if tag is None:
                        continue
                    if tag == "rt":
                        continue
                    if tag == "ruby":
                        base_raw = _ruby_base_text(child)
                        base_norm = normalize_base(base_raw)
                        if not base_norm:
                            continue
                        reading_raw = _ruby_reading_text(child)
                        reading_norm = normalize_reading(reading_raw)
                        if not reading_norm or not _is_kana_string(reading_norm):
                            continue
                        start = cursor
                        append(base_raw)
                        end = cursor
                        spans.append((start, end, base_norm, base_raw, reading_norm))
                        continue
                    walk(child)

            walk(body)
            full_text = "".join(text_parts)
            for start, end, base_norm, base_raw, reading_norm in spans:
                yield RubyRecord(
                    base=base_norm,
                    reading=reading_norm,
                    suffix=_collect_suffix(full_text, end),
                    prefix=_collect_numeric_prefix(full_text, start),
                )

            idx = 0
            while idx < len(spans):
                start, end, base_norm, base_raw, reading_norm = spans[idx]
                if not _is_single_kanji(base_norm):
                    idx += 1
                    continue
                group = [spans[idx]]
                j = idx + 1
                while j < len(spans):
                    next_start, next_end, next_base, *_ = spans[j]
                    if next_start != spans[j - 1][1]:
                        break
                    if not _is_single_kanji(next_base):
                        break
                    group.append(spans[j])
                    j += 1
                if len(group) <= 1:
                    idx += 1
                    continue
                combined_base = normalize_base("".join(item[3] for item in group))
                combined_reading = normalize_reading("".join(item[4] for item in group))
                if not combined_reading or not _is_kana_string(combined_reading):
                    idx += 1
                    continue
                yield RubyRecord(
                    base=combined_base,
                    reading=combined_reading,
                    suffix=_collect_suffix(full_text, group[-1][1]),
                    prefix=_collect_numeric_prefix(full_text, group[0][0]),
                )
                idx = j
                continue
            idx += 1
            # loop continues via idx += 1 after continue skipping


def aggregate(epub: Path) -> dict[str, dict[str, dict[str, object]]]:
    def _bucket() -> dict[str, object]:
        return {
            "total": 0,
            "suffix_counts": Counter(),
            "prefix_counts": Counter(),
        }

    data: dict[str, dict[str, dict[str, object]]] = defaultdict(
        lambda: defaultdict(_bucket)
    )
    for record in iter_ruby_records(epub):
        bucket = data[record.base][record.reading]
        bucket["total"] = bucket.get("total", 0) + 1
        bucket["suffix_counts"][record.suffix] += 1
        if record.prefix:
            bucket["prefix_counts"].update([record.prefix])
    return data


def filter_records(
    data: dict[str, dict[str, dict[str, object]]],
    min_total: int,
    dominance: float,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for base, readings in data.items():
        total = sum(info.get("total", 0) for info in readings.values())
        if total < min_total:
            continue
        reading_counts = {reading: info.get("total", 0) for reading, info in readings.items()}
        top_reading, top_count = max(reading_counts.items(), key=lambda kv: kv[1])
        if top_count / total < dominance:
            continue
        info = readings[top_reading]
        suffix_counts: Counter[str] = info.get("suffix_counts", Counter())
        suffixes: list[dict[str, object]] = []
        dominant_suffix = ""
        if suffix_counts:
            for value, count in suffix_counts.most_common(8):
                suffixes.append({"value": value, "count": count})
            dominant_suffix = suffixes[0]["value"] if suffixes else ""
        prefix_counts: Counter[str] = info.get("prefix_counts", Counter())
        prefixes: list[dict[str, object]] = []
        if prefix_counts:
            for value, count in prefix_counts.most_common(8):
                prefixes.append({"value": value, "count": count})
        entry: dict[str, object] = {
            "base": base,
            "reading": top_reading,
            "count": top_count,
            "suffix": dominant_suffix,
        }
        if suffixes:
            entry["suffixes"] = suffixes
        if prefixes:
            entry["prefixes"] = prefixes
        results.append(entry)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--epub",
        type=Path,
        default=ROOT / "dev" / "nhkeasier.epub",
        help="Path to the concatenated NHK Easier EPUB (download from https://nhkeasier.com).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "src" / "nk" / "data" / "nhk_easy_readings.json",
        help="Where to write the aggregated readings JSON.",
    )
    parser.add_argument("--min-total", type=int, default=3, help="Minimum occurrences required for inclusion.")
    parser.add_argument("--min-dominance", type=float, default=0.9, help="Minimum share for the dominant reading (0-1).")
    args = parser.parse_args()
    if not args.epub.exists():
        sys.exit(f"Corpus EPUB not found: {args.epub}")
    aggregated = aggregate(args.epub)
    filtered = filter_records(aggregated, args.min_total, args.min_dominance)
    args.output.write_text(json.dumps(filtered, ensure_ascii=False, indent=2))
    print(f"Wrote {len(filtered)} entries to {args.output}")


if __name__ == "__main__":
    main()
