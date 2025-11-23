from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable

from .book_io import TOKEN_METADATA_VERSION, is_original_text_file
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


def _build_offset_mapper(source: str | None, target: str | None):
    if source is None or target is None:
        return None
    matcher = SequenceMatcher(None, source, target, autojunk=False)
    opcodes = matcher.get_opcodes()

    def _map(value: int) -> int:
        if value <= 0:
            return 0
        for tag, i1, i2, j1, j2 in opcodes:
            if value < j1:
                return i1
            if j1 <= value <= j2:
                if tag == "equal":
                    return i1 + (value - j1)
                t_len = j2 - j1
                s_len = i2 - i1
                if t_len == 0:
                    return i1
                ratio = (value - j1) / t_len
                mapped = int(round(i1 + ratio * s_len))
                if mapped < i1:
                    return i1
                if mapped > i2:
                    return i2
                return mapped
        return len(source)

    return _map


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
        if txt_path.name.endswith(".partial.txt") or is_original_text_file(txt_path):
            continue
        if refine_chapter(txt_path, override_list):
            refined += 1
    return refined


def refine_chapter(text_path: Path, overrides: Iterable[OverrideRule]) -> bool:
    text = text_path.read_text(encoding="utf-8")
    initial_text = text
    try:
        original_path = text_path.with_name(f"{text_path.stem}.original.txt")
        original_text = original_path.read_text(encoding="utf-8")
    except OSError:
        original_text = None
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
        text, changed = _apply_override_rule(text, tokens, rule, original_text=original_text)
        if changed:
            overrides_applied = True
    if text == initial_text and not overrides_applied:
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
    replacement: str | None = None,
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
    text_value = text_path.read_text(encoding="utf-8")
    text_changed = False

    def _normalize(value: str | None) -> str | None:
        if value is None:
            return None
        trimmed = value.strip()
        if not trimmed:
            return None
        return trimmed

    normalized_replacement = _normalize(replacement)
    normalized_reading_input = _normalize(reading)
    normalized_reading = normalized_reading_input or normalized_replacement
    normalized_surface = _normalize(surface)
    if normalized_surface and normalized_surface != target.surface:
        target.surface = normalized_surface
        changed = True

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

    desired_segment = normalized_replacement or normalized_reading
    bounds = _token_transformed_bounds(target)
    if desired_segment and bounds is None:
        raise ValueError("Token does not have transformed offsets; cannot edit text.")
    if desired_segment and bounds is not None:
        start, end = bounds
        if end > start:
            segment = text_value[start:end]
            if segment != desired_segment:
                text_value = f"{text_value[:start]}{desired_segment}{text_value[end:]}"
                delta = len(desired_segment) - len(segment)
                _shift_tokens(tokens, end, delta, exclude=target)
                target.transformed_start = start
                target.transformed_end = start + len(desired_segment)
                text_changed = True
                changed = True

    if not changed:
        return False

    if text_changed:
        text_path.write_text(text_value, encoding="utf-8")

    tokens.sort(key=lambda token: (_token_transformed_start(token), _token_transformed_end(token)))
    normalized_for_hash = text_value.strip()
    sha1 = hashlib.sha1(normalized_for_hash.encode("utf-8")).hexdigest()
    version = payload.get("version", 1)
    new_payload = {
        "version": max(version, TOKEN_METADATA_VERSION),
        "text_sha1": sha1,
        "tokens": serialize_chapter_tokens(tokens),
    }
    token_path.write_text(json.dumps(new_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def remove_token(text_path: Path, token_index: int) -> bool:
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
    del tokens[token_index]
    try:
        text_value = text_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Failed to read chapter: {exc}") from exc
    text_changed = False
    bounds = _token_transformed_bounds(target)
    if bounds:
        start, end = bounds
        if end > start and end <= len(text_value):
            replacement = target.surface.strip() if target.surface else None
            if replacement:
                segment = text_value[start:end]
                if segment != replacement:
                    text_value = f"{text_value[:start]}{replacement}{text_value[end:]}"
                    delta = len(replacement) - len(segment)
                    _shift_tokens(tokens, end, delta, exclude=target)
                    text_changed = True
    if text_changed:
        text_path.write_text(text_value, encoding="utf-8")
    tokens.sort(key=lambda token: (_token_transformed_start(token), _token_transformed_end(token)))
    normalized_for_hash = text_value.strip()
    sha1 = hashlib.sha1(normalized_for_hash.encode("utf-8")).hexdigest()
    version = payload.get("version", 1)
    new_payload = {
        "version": max(version, TOKEN_METADATA_VERSION),
        "text_sha1": sha1,
        "tokens": serialize_chapter_tokens(tokens),
    }
    token_path.write_text(json.dumps(new_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def create_token_from_selection(
    text_path: Path,
    start: int,
    end: int,
    *,
    replacement: str | None = None,
    reading: str | None = None,
    surface: str | None = None,
    pos: str | None = None,
    accent: int | None = None,
) -> bool:
    if start < 0 or end <= start:
        raise ValueError("Invalid selection bounds.")
    token_path = text_path.with_name(text_path.name + ".token.json")
    try:
        text = text_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Failed to read chapter: {exc}") from exc
    if end > len(text):
        raise ValueError("Selection exceeds text length.")
    try:
        payload = json.loads(token_path.read_text(encoding="utf-8"))
        version = payload.get("version", 1)
        tokens_payload = payload.get("tokens")
        existing_tokens = deserialize_chapter_tokens(tokens_payload) if isinstance(tokens_payload, list) else []
    except (OSError, json.JSONDecodeError):
        version = TOKEN_METADATA_VERSION
        existing_tokens = []
    tokens = [replace(token) for token in existing_tokens]
    segment = text[start:end]
    replacement_text = replacement if replacement is not None else segment
    if replacement_text != segment:
        text = f"{text[:start]}{replacement_text}{text[end:]}"
        delta = len(replacement_text) - len(segment)
        _shift_tokens(tokens, end, delta)
        end = start + len(replacement_text)

    original_text: str | None = None
    try:
        original_path = text_path.with_name(f"{text_path.stem}.original.txt")
        original_text = original_path.read_text(encoding="utf-8")
    except OSError:
        original_text = None

    if original_text is not None:
        map_to_original = _build_offset_mapper(original_text, text)
        original_start = map_to_original(start)
        original_end = map_to_original(end)
        original_len = len(original_text)
        original_start = max(0, min(original_start, original_len))
        original_end = max(original_start, min(original_end, original_len))
        if original_end <= original_start:
            raise ValueError("Selection could not be mapped to original text.")
        surface_segment = original_text[original_start:original_end]
        context_prefix = original_text[max(0, original_start - 3) : original_start]
        context_suffix = original_text[original_end : original_end + 3]
    else:
        original_start = start
        original_end = end
        surface_segment = segment
        context_prefix = text[max(0, start - 3) : start]
        context_suffix = text[end : end + 3]

    def _ranges_overlap(a_start: int | None, a_end: int | None, b_start: int, b_end: int) -> bool:
        if a_start is None or a_end is None:
            return False
        return b_end > a_start and a_end > b_start

    filtered_tokens: list[ChapterToken] = []
    for token in tokens:
        t_bounds = _token_transformed_bounds(token)
        overlaps_transformed = t_bounds is not None and _ranges_overlap(t_bounds[0], t_bounds[1], start, end)
        overlaps_original = _ranges_overlap(token.start, token.end, original_start, original_end)
        if overlaps_transformed or overlaps_original:
            continue
        filtered_tokens.append(token)
    tokens = filtered_tokens

    reading_val = reading or replacement_text
    surface_val = surface or surface_segment
    new_token = ChapterToken(
        surface=surface_val,
        start=original_start,
        end=original_end,
        reading=reading_val,
        fallback_reading=reading_val,
        reading_source="override",
        context_prefix=context_prefix,
        context_suffix=context_suffix,
        accent_type=accent,
        pos=pos,
        transformed_start=start,
        transformed_end=end,
        reading_validated=False,
    )
    tokens.append(new_token)
    tokens.sort(key=lambda token: (_token_transformed_start(token), _token_transformed_end(token)))
    normalized_for_hash = text.strip()
    sha1 = hashlib.sha1(normalized_for_hash.encode("utf-8")).hexdigest()
    new_payload = {
        "version": max(version, TOKEN_METADATA_VERSION),
        "text_sha1": sha1,
        "tokens": serialize_chapter_tokens(tokens),
    }
    token_path.write_text(json.dumps(new_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    text_path.write_text(text, encoding="utf-8")
    return True


def _apply_override_rule(
    text: str,
    tokens: list[ChapterToken],
    rule: OverrideRule,
    *,
    original_text: str | None = None,
) -> tuple[str, bool]:
    compiled: re.Pattern[str] | None = None
    if rule.regex:
        try:
            compiled = re.compile(rule.pattern)
        except re.error as exc:
            raise ValueError(f"Invalid pattern '{rule.pattern}': {exc}") from exc
    changed = False
    mapper_cache: dict[str, object] = {"text": None, "mapper": None}

    def _get_mapper():
        if original_text is None:
            return None
        cached_text = mapper_cache.get("text")
        if cached_text == text:
            return mapper_cache.get("mapper")
        mapper = _build_offset_mapper(original_text, text)
        mapper_cache["text"] = text
        mapper_cache["mapper"] = mapper
        return mapper

    def _overlaps(start: int, end: int) -> list[ChapterToken]:
        overlapping: list[ChapterToken] = []
        for tok in tokens:
            bounds = _token_transformed_bounds(tok)
            if not bounds:
                continue
            t_start, t_end = bounds
            if end > t_start and start < t_end:
                overlapping.append(tok)
        return overlapping

    def _map_bounds(transformed_start: int, transformed_end: int) -> tuple[int, int]:
        mapper = _get_mapper()
        if mapper is None:
            return transformed_start, transformed_end
        original_len = len(original_text or "")
        o_start = mapper(transformed_start)
        o_end = mapper(transformed_end)
        o_start = max(0, min(o_start, original_len))
        o_end = max(o_start, min(o_end, original_len))
        return o_start, o_end

    def _add_token(
        transformed_start: int,
        transformed_end: int,
        segment: str,
        replacement_text: str | None,
    ) -> None:
        replacement = replacement_text if replacement_text is not None else segment
        reading_val = rule.reading or replacement
        surface_val = rule.surface or replacement
        original_start, original_end = _map_bounds(transformed_start, transformed_end)
        token = ChapterToken(
            surface=surface_val,
            start=original_start,
            end=original_end,
            reading=reading_val,
            fallback_reading=reading_val,
            reading_source="override",
            context_prefix=(original_text or text)[max(0, original_start - 3) : original_start],
            context_suffix=(original_text or text)[original_end : original_end + 3],
            accent_type=rule.accent,
            pos=rule.pos,
            transformed_start=transformed_start,
            transformed_end=transformed_end,
        )
        tokens.append(token)

    # Pass 1: update existing tokens
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
        if rule.surface and token.surface != rule.surface:
            token.surface = rule.surface
            token_changed = True
        if rule.pos and token.pos != rule.pos:
            token.pos = rule.pos
            token_changed = True
        if rule.accent is not None and token.accent_type != rule.accent:
            token.accent_type = rule.accent
            token_changed = True
        if token_changed:
            changed = True

    if changed:
        return text, True

    # Pass 2: find matches in text without existing tokens (e.g., ASCII headings)
    snapshot = text
    raw_matches: list[tuple[int, int, str]] = []
    if compiled:
        for match in compiled.finditer(snapshot):
            raw_matches.append((match.start(), match.end(), match.group(0)))
    else:
        search_pos = 0
        while True:
            idx = snapshot.find(rule.pattern, search_pos)
            if idx == -1:
                break
            raw_matches.append((idx, idx + len(rule.pattern), rule.pattern))
            search_pos = idx + len(rule.pattern)

    if not raw_matches:
        return text, False

    delta = 0
    for start, end, segment in raw_matches:
        adj_start = start + delta
        adj_end = end + delta
        overlapping_tokens = _overlaps(adj_start, adj_end)
        if overlapping_tokens:
            # User overrides should win; drop overlapping tokens so we can
            # replace and inject the curated token.
            tokens[:] = [tok for tok in tokens if tok not in overlapping_tokens]
        replacement_text = _resolve_replacement(rule)
        if replacement_text is not None and replacement_text != segment:
            text = f"{text[:adj_start]}{replacement_text}{text[adj_end:]}"
            change_delta = len(replacement_text) - len(segment)
            _shift_tokens(tokens, adj_end, change_delta)
            adj_end = adj_start + len(replacement_text)
            delta += change_delta
        expected_surface = rule.match_surface or rule.surface
        if expected_surface and original_text is not None:
            o_start, o_end = _map_bounds(adj_start, adj_end)
            if original_text[o_start:o_end] != expected_surface:
                continue
        _add_token(adj_start, adj_end, segment, replacement_text)
        changed = True

    if changed:
        tokens.sort(key=lambda token: (_token_transformed_start(token), _token_transformed_end(token)))
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


__all__ = [
    "load_override_config",
    "refine_book",
    "refine_chapter",
    "edit_single_token",
    "remove_token",
    "create_token_from_selection",
]
