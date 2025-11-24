from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

DEFAULT_MAX_CHARS_PER_CHUNK = 360
_SENTENCE_BREAKS = (
    "\n",
    "。",
    "！",
    "？",
    "!",
    "?",
    "…",
    "‼",
    "⁉",
    "⁈",
    "｡",
)
_CLAUSE_BREAKS = (
    "、",
    "，",
    "､",
    ",",
    ";",
    "；",
    ":",
    "：",
    "・",
    "—",
    "─",
)
_QUOTE_PAIRS = {
    "「": "」",
    "『": "』",
    "“": "”",
    "”": "“",
    '"': '"',
}
_MIN_DIALOGUE_QUOTE_LEN = 6
_ATTRIBUTION_RE = re.compile(
    r"(?:と\s*(?:言|云|い|叫|叫び|叫ん|呟|つぶや|囁|ささや|答え|返|尋ね|問い|聞い|思い|話し|怒鳴|叫んで|笑い|叫んだ))"
    r"|(?:\b(?:said|asked|shouted|yelled|whispered|replied|cried|called|murmured|told)\b)",
    re.IGNORECASE,
)


@dataclass
class ChunkSpan:
    text: str
    start: int
    end: int


def split_text_on_breaks(
    text: str,
    *,
    max_chars_per_chunk: int = DEFAULT_MAX_CHARS_PER_CHUNK,
    quote_aware: bool = True,
) -> list[str]:
    """
    Split text into chunks using blank-line separated blocks.
    Empty lines are treated as delimiters; consecutive blanks collapse.
    """
    return [
        chunk.text
        for chunk in split_text_on_breaks_with_spans(
            text,
            max_chars_per_chunk=max_chars_per_chunk,
            quote_aware=quote_aware,
        )
    ]


def split_text_on_breaks_with_spans(
    text: str,
    *,
    max_chars_per_chunk: int = DEFAULT_MAX_CHARS_PER_CHUNK,
    quote_aware: bool = True,
) -> list[ChunkSpan]:
    chunks: list[ChunkSpan] = []
    current: list[tuple[str, int, int]] = []

    def flush() -> None:
        if not current:
            return
        first_line, first_start, _ = current[0]
        last_line, _, last_end = current[-1]
        start = first_start + _leading_trim_index(first_line)
        end = last_end - _trailing_trim_count(last_line)
        current[:] = []
        if start >= end:
            return
        chunk_text = text[start:end]
        sub_chunks = _split_chunk_with_spans(
            chunk_text,
            start,
            max_chars_per_chunk=max_chars_per_chunk,
            quote_aware=quote_aware,
        )
        chunks.extend(sub_chunks)

    for line, start, end in _iter_lines_with_positions(text):
        if line.strip():
            current.append((line, start, end))
        else:
            flush()
    flush()
    return chunks


def _split_chunk_if_needed(chunk: str, max_chars_per_chunk: int) -> list[str]:
    if not chunk:
        return []
    if len(chunk) <= max_chars_per_chunk:
        return [chunk]
    segments: list[str] = []
    remaining = chunk
    while len(remaining) > max_chars_per_chunk:
        cut = _preferred_chunk_cut_index(remaining, max_chars_per_chunk)
        head = remaining[:cut].rstrip()
        if head:
            segments.append(head)
        remaining = remaining[cut:].lstrip()
        if not remaining:
            break
    if remaining:
        tail = remaining.strip()
        if tail:
            segments.append(tail)
    return segments


def _split_chunk_with_spans(
    chunk_text: str,
    base_start: int,
    *,
    max_chars_per_chunk: int = DEFAULT_MAX_CHARS_PER_CHUNK,
    quote_aware: bool = False,
) -> list[ChunkSpan]:
    segments: list[tuple[str, int]] = [(chunk_text, 0)]
    if quote_aware:
        split_segments: list[tuple[str, int]] = []
        for segment, offset in segments:
            split_segments.extend(_maybe_split_dialogue_quote(segment, offset))
        segments = split_segments

    spans: list[ChunkSpan] = []
    for segment, offset in segments:
        if not segment:
            continue
        spans.extend(_spans_from_segment(segment, base_start + offset, max_chars_per_chunk))
    return spans


def _spans_from_segment(segment: str, absolute_start: int, max_chars_per_chunk: int) -> list[ChunkSpan]:
    pieces = _split_chunk_if_needed(segment, max_chars_per_chunk)
    spans: list[ChunkSpan] = []
    cursor = 0
    for piece in pieces:
        if not piece:
            continue
        idx = segment.find(piece, cursor)
        if idx == -1:
            idx = segment.find(piece)
            if idx == -1:
                continue
        start = absolute_start + idx
        end = start + len(piece)
        spans.append(ChunkSpan(text=piece, start=start, end=end))
        cursor = idx + len(piece)
        while cursor < len(segment) and segment[cursor].isspace():
            cursor += 1
    return spans


def _maybe_split_dialogue_quote(segment: str, offset: int) -> list[tuple[str, int]]:
    pair = _first_dialogue_quote_pair(segment)
    if pair is None:
        return [(segment, offset)]
    open_idx, close_idx = pair
    quote_text = segment[open_idx : close_idx + 1]
    quote_inner_len = len(quote_text.strip(" \t\r\n"))
    if quote_inner_len < _MIN_DIALOGUE_QUOTE_LEN:
        return [(segment, offset)]
    after = segment[close_idx + 1 :].strip()
    before = segment[:open_idx].strip()
    if not after:
        return [(segment, offset)]
    if not _ATTRIBUTION_RE.search(after):
        return [(segment, offset)]

    parts: list[tuple[str, int]] = []
    if before:
        parts.append((segment[:open_idx], offset))
    parts.append((quote_text, offset + open_idx))
    tail_start = close_idx + 1
    if segment[tail_start:].strip():
        parts.append((segment[tail_start:], offset + tail_start))
    return parts or [(segment, offset)]


def _first_dialogue_quote_pair(text: str) -> tuple[int, int] | None:
    stack: list[tuple[str, int]] = []
    for idx, ch in enumerate(text):
        if ch in _QUOTE_PAIRS:
            expected_close = _QUOTE_PAIRS[ch]
            stack.append((expected_close, idx))
            continue
        if stack and ch == stack[-1][0]:
            _, open_idx = stack.pop()
            return open_idx, idx
    return None


def _iter_lines_with_positions(text: str) -> Iterable[tuple[str, int, int]]:
    cursor = 0
    for raw in text.splitlines(keepends=True):
        line = raw.rstrip("\r\n")
        line_start = cursor
        line_end = line_start + len(line)
        yield (line, line_start, line_end)
        cursor += len(raw)
    if not text.endswith(("\n", "\r")) and text:
        # splitlines with keepends already adds final line without newline,
        # so this branch is only reached when the input is empty.
        return


def _leading_trim_index(line: str) -> int:
    idx = 0
    while idx < len(line) and line[idx].isspace():
        idx += 1
    return idx


def _trailing_trim_count(line: str) -> int:
    idx = len(line)
    while idx > 0 and line[idx - 1].isspace():
        idx -= 1
    return len(line) - idx


def _preferred_chunk_cut_index(text: str, limit: int) -> int:
    def _best_index(separators: tuple[str, ...]) -> tuple[int, int] | None:
        best: tuple[int, int] | None = None
        for sep in separators:
            idx = text.rfind(sep, 0, limit)
            if idx > 0:
                end = idx + len(sep)
                if best is None or end > best[0] + best[1]:
                    best = (idx, len(sep))
        return best

    for candidates in (_SENTENCE_BREAKS, _CLAUSE_BREAKS):
        match = _best_index(candidates)
        if match is not None:
            return match[0] + match[1]
    return max(1, limit)


__all__ = [
    "ChunkSpan",
    "DEFAULT_MAX_CHARS_PER_CHUNK",
    "split_text_on_breaks",
    "split_text_on_breaks_with_spans",
]
