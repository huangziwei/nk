from __future__ import annotations

from dataclasses import dataclass

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


@dataclass
class ChunkSpan:
    text: str
    start: int
    end: int


def split_text_on_breaks(text: str, *, max_chars_per_chunk: int = DEFAULT_MAX_CHARS_PER_CHUNK) -> list[str]:
    """
    Split text into chunks using blank-line separated blocks.
    Empty lines are treated as delimiters; consecutive blanks collapse.
    """
    return [chunk.text for chunk in split_text_on_breaks_with_spans(text, max_chars_per_chunk=max_chars_per_chunk)]


def split_text_on_breaks_with_spans(
    text: str, *, max_chars_per_chunk: int = DEFAULT_MAX_CHARS_PER_CHUNK
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
        sub_chunks = _split_chunk_with_spans(chunk_text, start, max_chars_per_chunk=max_chars_per_chunk)
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
) -> list[ChunkSpan]:
    segments = _split_chunk_if_needed(chunk_text, max_chars_per_chunk)
    if not segments:
        return []
    spans: list[ChunkSpan] = []
    cursor = 0
    for segment in segments:
        if not segment:
            continue
        idx = chunk_text.find(segment, cursor)
        if idx == -1:
            idx = chunk_text.find(segment)
            if idx == -1:
                continue
        start = base_start + idx
        end = start + len(segment)
        spans.append(ChunkSpan(text=segment, start=start, end=end))
        cursor = idx + len(segment)
        while cursor < len(chunk_text) and chunk_text[cursor].isspace():
            cursor += 1
    return spans


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
