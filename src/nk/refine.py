from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable

from .book_io import TOKEN_METADATA_VERSION
from .tokens import ChapterToken, deserialize_chapter_tokens, serialize_chapter_tokens

PRIMARY_OVERRIDE_FILENAME = "custom_token.json"
LEGACY_OVERRIDE_FILENAME = "custom_pitch.json"


@dataclass
class OverrideRule:
    pattern: str
    regex: bool
    replacement: str | None
    reading: str | None
    accent: int | None
    pos: str | None
    surface: str | None
    match_surface: str | None


def _migrate_legacy_override_file(book_dir: Path) -> Path | None:
    primary = book_dir / PRIMARY_OVERRIDE_FILENAME
    if primary.exists():
        return primary
    legacy = book_dir / LEGACY_OVERRIDE_FILENAME
    if not legacy.exists():
        return None
    try:
        legacy.replace(primary)
        return primary
    except OSError:
        try:
            primary.write_text(legacy.read_text(encoding="utf-8"), encoding="utf-8")
            legacy.unlink(missing_ok=True)
            return primary
        except OSError:
            return legacy


def ensure_override_file(book_dir: Path) -> Path:
    path = _migrate_legacy_override_file(book_dir)
    if path and path.exists():
        return path
    path = book_dir / PRIMARY_OVERRIDE_FILENAME
    if path.exists():
        return path
    payload = {"overrides": []}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def append_override_entry(book_dir: Path, entry: dict[str, object]) -> Path:
    path = ensure_override_file(book_dir)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raw = {"overrides": []}
    overrides = raw.get("overrides")
    if not isinstance(overrides, list):
        overrides = []
        raw = {"overrides": overrides}
    overrides.append(entry)
    path.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def load_override_config(book_dir: Path) -> list[OverrideRule]:
    config_path = ensure_override_file(book_dir)
    if not config_path.exists():
        return []
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Failed to parse overrides file: {config_path}") from exc
    overrides_payload = raw.get("overrides")
    if not isinstance(overrides_payload, list):
        raise ValueError(f"{config_path.name} must contain an 'overrides' array.")
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
        match_surface = entry.get("match_surface")
        if match_surface is not None and not isinstance(match_surface, str):
            match_surface = None
        overrides.append(
            OverrideRule(
                pattern=pattern,
                regex=regex,
                replacement=replacement,
                reading=reading,
                accent=accent,
                pos=pos,
                surface=surface,
                match_surface=match_surface,
            )
        )
    return overrides


def refine_book(book_dir: Path, overrides: Iterable[OverrideRule]) -> int:
    override_list = list(overrides)
    if not override_list:
        return 0
    refined = 0
    for txt_path in sorted(book_dir.glob("*.txt")):
        if txt_path.name.endswith(".partial.txt"):
            continue
        if refine_chapter(txt_path, override_list):
            refined += 1
    return refined


def refine_chapter(text_path: Path, overrides: Iterable[OverrideRule]) -> bool:
    text = text_path.read_text(encoding="utf-8")
    original_text = text
    token_path = text_path.with_name(text_path.name + ".token.json")
    existing_tokens: list[ChapterToken] = []
    version = 1
    if token_path.exists():
        try:
            payload = json.loads(token_path.read_text(encoding="utf-8"))
            version = payload.get("version", 1)
            tokens_payload = payload.get("tokens")
            if isinstance(tokens_payload, list):
                existing_tokens = deserialize_chapter_tokens(tokens_payload)
        except (OSError, json.JSONDecodeError):
            existing_tokens = []
    tokens = [replace(token) for token in existing_tokens]
    overrides_applied = False
    for rule in overrides:
        text, changed = _apply_override_rule(text, tokens, rule)
        if changed:
            overrides_applied = True
    if text == original_text and not overrides_applied:
        return False
    text_path.write_text(text, encoding="utf-8")
    tokens.sort(key=lambda token: (_token_transformed_start(token), _token_transformed_end(token)))
    normalized_for_hash = text.strip()
    sha1 = hashlib.sha1(normalized_for_hash.encode("utf-8")).hexdigest()
    payload = {
        "version": max(version, TOKEN_METADATA_VERSION),
        "text_sha1": sha1,
        "tokens": serialize_chapter_tokens(tokens),
    }
    token_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def edit_single_token(
    text_path: Path,
    token_index: int,
    *,
    reading: str | None = None,
    surface: str | None = None,
    pos: str | None = None,
    accent: int | None = None,
) -> bool:
    if token_index < 0:
        raise ValueError("token_index must be non-negative.")
    token_path = text_path.with_name(text_path.name + ".token.json")
    if not token_path.exists():
        raise ValueError("Token metadata not found for this chapter.")
    try:
        payload = json.loads(token_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Failed to parse token metadata: {token_path}") from exc
    tokens_payload = payload.get("tokens")
    if not isinstance(tokens_payload, list):
        raise ValueError("Token file is missing a 'tokens' array.")
    tokens = deserialize_chapter_tokens(tokens_payload)
    if token_index >= len(tokens):
        raise ValueError("token_index is out of range for this chapter.")
    target = tokens[token_index]
    changed = False

    def _normalize(value: str | None) -> str | None:
        if value is None:
            return None
        trimmed = value.strip()
        if not trimmed:
            return None
        return trimmed

    normalized_surface = _normalize(surface)
    if normalized_surface and normalized_surface != target.surface:
        target.surface = normalized_surface
        changed = True

    normalized_reading = _normalize(reading)
    if normalized_reading and normalized_reading != target.reading:
        target.reading = normalized_reading
        target.fallback_reading = normalized_reading
        target.reading_source = "manual"
        changed = True
    elif normalized_reading and target.reading_source != "manual":
        target.reading_source = "manual"
        changed = True

    normalized_pos = _normalize(pos)
    if normalized_pos and normalized_pos != target.pos:
        target.pos = normalized_pos
        changed = True

    if accent is not None and target.accent_type != accent:
        target.accent_type = accent
        changed = True

    if not changed:
        return False

    tokens.sort(key=lambda token: (_token_transformed_start(token), _token_transformed_end(token)))
    text = text_path.read_text(encoding="utf-8")
    normalized_for_hash = text.strip()
    sha1 = hashlib.sha1(normalized_for_hash.encode("utf-8")).hexdigest()
    version = payload.get("version", 1)
    new_payload = {
        "version": max(version, TOKEN_METADATA_VERSION),
        "text_sha1": sha1,
        "tokens": serialize_chapter_tokens(tokens),
    }
    token_path.write_text(json.dumps(new_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def _apply_override_rule(
    text: str, tokens: list[ChapterToken], rule: OverrideRule
) -> tuple[str, bool]:
    compiled: re.Pattern[str] | None = None
    if rule.regex:
        try:
            compiled = re.compile(rule.pattern)
        except re.error as exc:
            raise ValueError(f"Invalid pattern '{rule.pattern}': {exc}") from exc
    changed = False
    for token in tokens:
        bounds = _token_transformed_bounds(token)
        if not bounds:
            continue
        start, end = bounds
        if end <= start:
            continue
        segment = text[start:end]
        if not _token_matches_rule(token, segment, rule, compiled):
            continue
        replacement_text = _resolve_replacement(rule)
        token_changed = False
        if replacement_text is not None and replacement_text != segment:
            text = f"{text[:start]}{replacement_text}{text[end:]}"
            delta = len(replacement_text) - len(segment)
            _shift_tokens(tokens, end, delta, exclude=token)
            token.transformed_start = start
            token.transformed_end = start + len(replacement_text)
            token_changed = True
        new_reading = rule.reading
        if new_reading is None and replacement_text is not None:
            new_reading = replacement_text
        if new_reading is not None and token.reading != new_reading:
            token.reading = new_reading
            token_changed = True
        if token.reading_source != "override":
            token.reading_source = "override"
            token_changed = True
        if rule.surface:
            targeting_surface = rule.surface
            if token.surface != targeting_surface:
                token.surface = targeting_surface
                token_changed = True
        if rule.pos and token.pos != rule.pos:
            token.pos = rule.pos
            token_changed = True
        if rule.accent is not None and token.accent_type != rule.accent:
            token.accent_type = rule.accent
            token_changed = True
        if token_changed:
            changed = True
    return text, changed


def _token_transformed_bounds(token: ChapterToken) -> tuple[int, int] | None:
    start = token.transformed_start
    end = token.transformed_end
    if isinstance(start, int) and isinstance(end, int):
        return start, end
    return None


def _resolve_replacement(rule: OverrideRule) -> str | None:
    if rule.replacement is not None:
        return rule.replacement
    if rule.reading is not None:
        return rule.reading
    return None


def _token_matches_rule(
    token: ChapterToken,
    segment: str,
    rule: OverrideRule,
    compiled: re.Pattern[str] | None,
) -> bool:
    target_surface = rule.match_surface or rule.surface
    if target_surface and (not token.surface or token.surface != target_surface):
        return False
    if rule.pos and token.pos != rule.pos:
        return False
    reading = token.reading or token.fallback_reading or segment
    if compiled:
        return bool(compiled.search(reading))
    return reading == rule.pattern


def _shift_tokens(
    tokens: list[ChapterToken], cutoff: int, delta: int, exclude: ChapterToken | None = None
) -> None:
    if delta == 0:
        return
    for token in tokens:
        if exclude is not None and token is exclude:
            continue
        if token.transformed_start is not None and token.transformed_start >= cutoff:
            token.transformed_start += delta
        if token.transformed_end is not None and token.transformed_end >= cutoff:
            token.transformed_end += delta


def _token_transformed_start(token: ChapterToken) -> int:
    if token.transformed_start is not None:
        return token.transformed_start
    if token.start is not None:
        return token.start
    return 0


def _token_transformed_end(token: ChapterToken) -> int:
    if token.transformed_end is not None:
        return token.transformed_end
    if token.end is not None:
        return token.end
    return _token_transformed_start(token)


__all__ = ["load_override_config", "refine_book", "refine_chapter"]
