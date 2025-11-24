from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace
from difflib import SequenceMatcher
from bisect import bisect_left
from pathlib import Path
from typing import Callable, Iterable

from .book_io import TOKEN_METADATA_VERSION, is_original_text_file
from .tokens import ChapterToken, deserialize_chapter_tokens, serialize_chapter_tokens

PRIMARY_OVERRIDE_FILENAME = "custom_token.json"
LEGACY_OVERRIDE_FILENAME = "custom_pitch.json"
ProgressCallback = Callable[[dict[str, object]], None]


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


@dataclass
class RemoveRule:
    reading: str | None
    surface: str | None


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
    payload = {"overrides": [], "remove": []}
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


def load_refine_config(book_dir: Path) -> tuple[list[OverrideRule], list[RemoveRule]]:
    config_path = ensure_override_file(book_dir)
    if not config_path.exists():
        return [], []
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Failed to parse overrides file: {config_path}") from exc
    overrides_payload = raw.get("overrides")
    if not isinstance(overrides_payload, list):
        raise ValueError(f"{config_path.name} must contain an 'overrides' array.")
    overrides: list[OverrideRule] = []
    removals: list[RemoveRule] = []
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
    remove_payload = raw.get("remove")
    if remove_payload is None:
        remove_payload = raw.get("removals")
    if remove_payload is None:
        remove_payload = []
    if remove_payload is not None and not isinstance(remove_payload, list):
        raise ValueError(f"{config_path.name} must contain a 'remove' array if provided.")
    for entry in remove_payload:
        if not isinstance(entry, dict):
            continue
        reading = entry.get("reading")
        if reading is not None and not isinstance(reading, str):
            reading = None
        surface = entry.get("surface")
        if surface is not None and not isinstance(surface, str):
            surface = None
        if reading is None and surface is None:
            continue
        removals.append(RemoveRule(reading=reading, surface=surface))
    return overrides, removals


def load_override_config(book_dir: Path) -> list[OverrideRule]:
    overrides, _ = load_refine_config(book_dir)
    return overrides


def load_removal_rules(book_dir: Path) -> list[RemoveRule]:
    _, removals = load_refine_config(book_dir)
    return removals


def refine_book(
    book_dir: Path,
    overrides: Iterable[OverrideRule] | None,
    *,
    removals: Iterable[RemoveRule] | None = None,
    progress: ProgressCallback | None = None,
) -> int:
    if overrides is None and removals is None:
        override_list, removal_list = load_refine_config(book_dir)
    else:
        override_list = list(overrides) if overrides is not None else load_override_config(book_dir)
        removal_list = list(removals) if removals is not None else load_removal_rules(book_dir)
    if not override_list and not removal_list:
        return 0
    refined = 0
    chapters: list[Path] = []
    for txt_path in sorted(book_dir.glob("*.txt")):
        if txt_path.name.endswith(".partial.txt") or is_original_text_file(txt_path):
            continue
        chapters.append(txt_path)
    total_chapters = len(chapters)
    if progress:
        progress({"event": "book_start", "total_chapters": total_chapters, "book_dir": book_dir})
    for index, txt_path in enumerate(chapters, start=1):
        if refine_chapter(
            txt_path,
            override_list,
            removals=removal_list,
            progress=progress,
            chapter_index=index,
            chapter_total=total_chapters,
        ):
            refined += 1
    return refined


def refine_chapter(
    text_path: Path,
    overrides: Iterable[OverrideRule] | None,
    *,
    removals: Iterable[RemoveRule] | None = None,
    progress: ProgressCallback | None = None,
    chapter_index: int | None = None,
    chapter_total: int | None = None,
) -> bool:
    override_list = list(overrides) if overrides is not None else []
    removal_list = list(removals or [])
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
    seen_tokens: set[int] = set()

    def _emit_progress(event: dict[str, object]) -> None:
        if progress is None:
            return
        progress(event)

    def _advance_token_progress(advance: int = 0, *, total_delta: int = 0) -> None:
        if progress is None:
            return
        if advance <= 0 and total_delta == 0:
            return
        payload: dict[str, object] = {
            "event": "token_progress",
            "path": text_path,
        }
        if chapter_index is not None:
            payload["index"] = chapter_index
        if chapter_total is not None:
            payload["total"] = chapter_total
        if advance > 0:
            payload["advance"] = advance
        if total_delta:
            payload["total_delta"] = total_delta
        progress(payload)

    def _advance_token_total(delta: int) -> None:
        if delta > 0:
            _advance_token_progress(total_delta=delta)

    def _mark_token_seen(token: ChapterToken) -> None:
        token_id = id(token)
        if token_id in seen_tokens:
            return
        seen_tokens.add(token_id)
        _advance_token_progress(advance=1)

    def _token_has_bounds(token: ChapterToken) -> bool:
        bounds = _token_transformed_bounds(token)
        return bool(bounds) and bounds[1] > bounds[0]

    trackable_total = sum(1 for tok in tokens if _token_has_bounds(tok))
    _emit_progress(
        {
            "event": "chapter_start",
            "path": text_path,
            "index": chapter_index,
            "total": chapter_total,
            "token_total": trackable_total,
        }
    )
    if not override_list and not removal_list:
        for tok in tokens:
            if _token_has_bounds(tok):
                _mark_token_seen(tok)
        _emit_progress(
            {
                "event": "chapter_done",
                "path": text_path,
                "index": chapter_index,
                "total": chapter_total,
                "changed": False,
                "tokens_processed": len(seen_tokens),
            }
        )
        return False

    overrides_applied = False
    removals_applied = False
    for rule in override_list:
        text, changed = _apply_override_rule(
            text,
            tokens,
            rule,
            original_text=original_text,
            on_token=_mark_token_seen,
            on_token_total=_advance_token_total,
        )
        if changed:
            overrides_applied = True
    if removal_list:
        text, removed_changed = _apply_remove_rules(
            text,
            tokens,
            removal_list,
            on_token=_mark_token_seen,
        )
        if removed_changed:
            removals_applied = True
    refined = text != initial_text or overrides_applied or removals_applied
    if not refined:
        _emit_progress(
            {
                "event": "chapter_done",
                "path": text_path,
                "index": chapter_index,
                "total": chapter_total,
                "changed": False,
                "tokens_processed": len(seen_tokens),
            }
        )
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
    _emit_progress(
        {
            "event": "chapter_done",
            "path": text_path,
            "index": chapter_index,
            "total": chapter_total,
            "changed": True,
            "tokens_processed": len(seen_tokens),
        }
    )
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
    on_token: Callable[[ChapterToken], None] | None = None,
    on_token_total: Callable[[int], None] | None = None,
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
        original_bounds: tuple[int, int] | None = None,
    ) -> None:
        replacement = replacement_text if replacement_text is not None else segment
        reading_val = rule.reading or replacement
        surface_val = rule.surface or replacement
        if original_bounds is not None:
            original_start, original_end = original_bounds
        else:
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
        if on_token_total:
            on_token_total(1)
        tokens.append(token)
        if on_token:
            on_token(token)

    # Pass 1: update existing tokens that already carry transformed offsets.
    tokens_with_bounds = [
        tok for tok in tokens if _token_transformed_bounds(tok) and _token_transformed_bounds(tok)[1] > _token_transformed_bounds(tok)[0]
    ]
    tokens_with_bounds.sort(key=lambda tok: _token_transformed_start(tok))

    for token in tokens_with_bounds:
        if on_token:
            on_token(token)
        bounds = _token_transformed_bounds(token)
        if not bounds:
            continue
        start, end = bounds
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

    # Rebuild coverage after potential shifts in pass 1.
    text_len = len(text)

    def _build_token_index() -> tuple[list[tuple[int, int, ChapterToken]], list[int]]:
        indexed: list[tuple[int, int, ChapterToken]] = []
        for tok in tokens:
            bounds = _token_transformed_bounds(tok)
            if bounds and bounds[1] > bounds[0]:
                indexed.append((bounds[0], bounds[1], tok))
        indexed.sort(key=lambda entry: entry[0])
        starts = [entry[0] for entry in indexed]
        return indexed, starts

    token_index, token_starts = _build_token_index()
    bounds_dirty = False

    # Pass 2: search across the full transformed text so overrides can replace spans that already have tokens.
    search_pos = 0
    while search_pos < text_len:
        if bounds_dirty:
            token_index, token_starts = _build_token_index()
            bounds_dirty = False
        if compiled:
            match = compiled.search(text, search_pos)
            if not match:
                break
            start, end = match.span()
            segment = match.group(0)
        else:
            start = text.find(rule.pattern, search_pos)
            if start == -1:
                break
            end = start + len(rule.pattern)
            segment = rule.pattern

        expected_surface = rule.match_surface or rule.surface
        replacement_text = _resolve_replacement(rule)
        expected_reading = rule.reading or replacement_text or segment
        overlapping: list[ChapterToken] = []
        overlaps_outside_span = False
        already_applied = False
        overlap_surface_combined: str | None = None
        overlap_original_bounds: tuple[int, int] | None = None

        start_idx = bisect_left(token_starts, start)
        idx = start_idx
        while idx < len(token_index) and token_index[idx][0] < end:
            t_start, t_end, token = token_index[idx]
            overlapping.append(token)
            if t_start < start or t_end > end:
                overlaps_outside_span = True
            if (
                t_start == start
                and t_end == end
                and (expected_reading is None or token.reading == expected_reading or token.fallback_reading == expected_reading)
                and (rule.surface is None or token.surface == rule.surface)
                and (rule.match_surface is None or token.surface == rule.match_surface)
                and (rule.pos is None or token.pos == rule.pos)
                and (rule.accent is None or token.accent_type == rule.accent)
                and (replacement_text is None or replacement_text == segment)
            ):
                already_applied = True
            idx += 1
        idx = start_idx - 1
        while idx >= 0 and token_index[idx][1] > start:
            t_start, t_end, token = token_index[idx]
            overlapping.append(token)
            if t_start < start or t_end > end:
                overlaps_outside_span = True
            if (
                t_start == start
                and t_end == end
                and (expected_reading is None or token.reading == expected_reading or token.fallback_reading == expected_reading)
                and (rule.surface is None or token.surface == rule.surface)
                and (rule.match_surface is None or token.surface == rule.match_surface)
                and (rule.pos is None or token.pos == rule.pos)
                and (rule.accent is None or token.accent_type == rule.accent)
                and (replacement_text is None or replacement_text == segment)
            ):
                already_applied = True
            idx -= 1

        if overlapping:
            sorted_overlap = sorted(overlapping, key=_token_transformed_start)
            overlap_surface_combined = "".join(tok.surface or "" for tok in sorted_overlap)
            if all(tok.start is not None and tok.end is not None for tok in sorted_overlap):
                overlap_original_bounds = (
                    min(tok.start for tok in sorted_overlap if tok.start is not None),
                    max(tok.end for tok in sorted_overlap if tok.end is not None),
                )

        if expected_surface:
            if overlap_surface_combined is not None:
                if overlap_surface_combined != expected_surface:
                    search_pos = end
                    continue
            elif original_text is not None:
                o_start, o_end = _map_bounds(start, end)
                if original_text[o_start:o_end] != expected_surface:
                    search_pos = end
                    continue

        new_end = end
        if already_applied or overlaps_outside_span:
            search_pos = new_end if new_end > start else end
            continue

        if overlapping:
            tokens[:] = [tok for tok in tokens if tok not in overlapping]
            bounds_dirty = True
            changed = True

        if replacement_text is not None and replacement_text != segment:
            text = f"{text[:start]}{replacement_text}{text[end:]}"
            change_delta = len(replacement_text) - len(segment)
            new_end = start + len(replacement_text)
            _shift_tokens(tokens, end, change_delta)
            text_len += change_delta
            changed = True
            bounds_dirty = True
        _add_token(start, new_end, segment, replacement_text, original_bounds=overlap_original_bounds)
        bounds_dirty = True
        changed = True
        search_pos = new_end if new_end > start else end

    if changed:
        tokens.sort(key=lambda token: (_token_transformed_start(token), _token_transformed_end(token)))
    return text, changed


def _apply_remove_rules(
    text: str,
    tokens: list[ChapterToken],
    rules: Iterable[RemoveRule],
    *,
    on_token: Callable[[ChapterToken], None] | None = None,
) -> tuple[str, bool]:
    rule_list = list(rules)
    if not rule_list:
        return text, False
    changed = False
    for token in list(tokens):
        bounds = _token_transformed_bounds(token)
        if on_token and bounds and bounds[1] > bounds[0]:
            on_token(token)
        if not any(_removal_matches_rule(token, rule) for rule in rule_list):
            continue
        if bounds:
            start, end = bounds
            if end > start and end <= len(text):
                replacement = token.surface.strip() if token.surface else None
                if replacement:
                    segment = text[start:end]
                    if segment != replacement:
                        text = f"{text[:start]}{replacement}{text[end:]}"
                        delta = len(replacement) - len(segment)
                        _shift_tokens(tokens, end, delta, exclude=token)
        try:
            tokens.remove(token)
        except ValueError:
            pass
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


def _removal_matches_rule(token: ChapterToken, rule: RemoveRule) -> bool:
    if rule.surface and token.surface != rule.surface:
        return False
    token_reading = token.reading or token.fallback_reading
    if rule.reading and token_reading != rule.reading:
        return False
    return True


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
    "RemoveRule",
    "load_override_config",
    "load_refine_config",
    "load_removal_rules",
    "refine_book",
    "refine_chapter",
    "edit_single_token",
    "remove_token",
    "create_token_from_selection",
]
