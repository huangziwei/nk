from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .pitch import PitchToken, deserialize_pitch_tokens, serialize_pitch_tokens


@dataclass
class OverrideRule:
    pattern: str
    regex: bool
    replacement: str | None
    reading: str | None
    accent: int | None
    pos: str | None
    surface: str | None


def load_override_config(book_dir: Path) -> list[OverrideRule]:
    config_path = book_dir / "custom_pitch.json"
    if not config_path.exists():
        return []
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Failed to parse overrides file: {config_path}") from exc
    overrides_payload = raw.get("overrides")
    if not isinstance(overrides_payload, list):
        raise ValueError("custom_pitch.json must contain an 'overrides' array.")
    overrides: list[OverrideRule] = []
    for entry in overrides_payload:
        if not isinstance(entry, dict):
            continue
        pattern = entry.get("pattern")
        if not isinstance(pattern, str) or not pattern:
            continue
        regex = bool(entry.get("regex"))
        replacement = entry.get("replacement")
        if replacement is not None and not isinstance(replacement, str):
            replacement = None
        reading = entry.get("reading")
        if reading is not None and not isinstance(reading, str):
            reading = None
        accent_val = entry.get("accent")
        accent = None
        if isinstance(accent_val, int):
            accent = accent_val
        elif isinstance(accent_val, str) and accent_val.isdigit():
            accent = int(accent_val)
        pos = entry.get("pos")
        if pos is not None and not isinstance(pos, str):
            pos = None
        surface = entry.get("surface")
        if surface is not None and not isinstance(surface, str):
            surface = None
        overrides.append(
            OverrideRule(
                pattern=pattern,
                regex=regex,
                replacement=replacement,
                reading=reading,
                accent=accent,
                pos=pos,
                surface=surface,
            )
        )
    return overrides


def refine_book(book_dir: Path, overrides: Iterable[OverrideRule]) -> int:
    override_list = list(overrides)
    if not override_list:
        return 0
    refined = 0
    for txt_path in sorted(book_dir.glob("*.txt")):
        if refine_chapter(txt_path, override_list):
            refined += 1
    return refined


def refine_chapter(text_path: Path, overrides: Iterable[OverrideRule]) -> bool:
    text = text_path.read_text(encoding="utf-8")
    original_text = text
    matches_for_tokens: list[tuple[int, int, OverrideRule]] = []
    for rule in overrides:
        text, positions = _apply_override_to_text(text, rule)
        for start, end in positions:
            matches_for_tokens.append((start, end, rule))
    if text == original_text and not matches_for_tokens:
        return False
    text_path.write_text(text, encoding="utf-8")
    pitch_path = text_path.with_name(text_path.name + ".pitch.json")
    existing_tokens: list[PitchToken] = []
    version = 1
    if pitch_path.exists():
        try:
            payload = json.loads(pitch_path.read_text(encoding="utf-8"))
            version = payload.get("version", 1)
            tokens_payload = payload.get("tokens")
            if isinstance(tokens_payload, list):
                existing_tokens = deserialize_pitch_tokens(tokens_payload)
        except (OSError, json.JSONDecodeError):
            existing_tokens = []
    tokens = _merge_override_tokens(existing_tokens, matches_for_tokens, text)
    normalized_for_hash = text.strip()
    sha1 = hashlib.sha1(normalized_for_hash.encode("utf-8")).hexdigest()
    payload = {
        "version": version,
        "text_sha1": sha1,
        "tokens": serialize_pitch_tokens(tokens),
    }
    pitch_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def _apply_override_to_text(text: str, rule: OverrideRule) -> tuple[str, list[tuple[int, int]]]:
    pattern = rule.pattern if rule.regex else re.escape(rule.pattern)
    try:
        regex = re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"Invalid pattern '{rule.pattern}': {exc}") from exc
    matches = list(regex.finditer(text))
    if not matches:
        return text, []
    parts: list[str] = []
    positions: list[tuple[int, int]] = []
    cursor = 0
    out_length = 0
    for match in matches:
        start, end = match.span()
        parts.append(text[cursor:start])
        out_length += len(text[cursor:start])
        new_start = out_length
        if rule.replacement is not None:
            parts.append(rule.replacement)
            out_length += len(rule.replacement)
        else:
            segment = text[start:end]
            parts.append(segment)
            out_length += len(segment)
        new_end = out_length
        positions.append((new_start, new_end))
        cursor = end
    parts.append(text[cursor:])
    new_text = "".join(parts)
    return new_text, positions


def _merge_override_tokens(
    existing: list[PitchToken],
    overrides: list[tuple[int, int, OverrideRule]],
    text: str,
) -> list[PitchToken]:
    tokens = [PitchToken(**vars(token)) for token in existing]
    for start, end, rule in overrides:
        tokens = [
            token for token in tokens if token.end <= start or token.start >= end
        ]
        reading = rule.reading or rule.replacement or text[start:end]
        surface = rule.surface or rule.pattern
        tokens.append(
            PitchToken(
                surface=surface,
                reading=reading,
                accent_type=rule.accent,
                accent_connection=None,
                pos=rule.pos,
                start=start,
                end=end,
            )
        )
    tokens.sort(key=lambda token: (token.start, token.end))
    return tokens


__all__ = ["load_override_config", "refine_book"]
