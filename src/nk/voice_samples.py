from __future__ import annotations

import re
from typing import Sequence

DEFAULT_VOICE_SAMPLE_TEXTS = [
    "こんにちは。こちらはVOICEVOXの音声サンプルです。",
    "読み上げの速度と音程を確認しています。",
    "お好みの声を選んでください。",
]

_VOICE_SAMPLE_INVALID_CHARS = set('<>:"/\\|?*')


def build_sample_text(lines: Sequence[str] | None = None) -> str:
    source = lines if lines else DEFAULT_VOICE_SAMPLE_TEXTS
    cleaned = [line.strip() for line in source if line and line.strip()]
    if not cleaned:
        raise ValueError("Sample text is empty.")
    return "\n".join(cleaned)


def sanitize_voice_sample_name(name: str) -> str:
    if not isinstance(name, str):
        name = ""
    candidate = name.strip()
    if not candidate:
        candidate = "voice"
    chars: list[str] = []
    for ch in candidate:
        if ch in {"/", "\\"}:
            chars.append("-")
        elif ch in _VOICE_SAMPLE_INVALID_CHARS:
            chars.append("_")
        elif ord(ch) < 32:
            continue
        else:
            chars.append(ch)
    sanitized = "".join(chars).strip(" .-_")
    if not sanitized:
        sanitized = "voice"
    sanitized = re.sub(r"\s+", "-", sanitized)
    sanitized = re.sub(r"-{2,}", "-", sanitized)
    return sanitized[:120]


def voice_samples_from_payload(payload: object) -> list[tuple[int, str]]:
    if not isinstance(payload, list):
        return []
    voices: list[tuple[int, str]] = []
    seen_ids: set[int] = set()
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        speaker_name = str(entry.get("name") or "").strip()
        styles = entry.get("styles")
        if not isinstance(styles, list):
            continue
        for style in styles:
            if not isinstance(style, dict):
                continue
            style_id = style.get("id")
            if not isinstance(style_id, int):
                continue
            if style_id in seen_ids:
                continue
            seen_ids.add(style_id)
            style_name = str(style.get("name") or "").strip()
            name_parts = [part for part in (speaker_name, style_name) if part]
            display_name = "-".join(name_parts) if name_parts else f"Voice-{style_id}"
            voices.append((style_id, display_name))
    voices.sort(key=lambda item: item[0])
    return voices


def voice_roster_from_payload(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, list):
        return []
    roster: list[dict[str, object]] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        speaker_name = str(entry.get("name") or "").strip()
        styles = entry.get("styles")
        if not isinstance(styles, list):
            continue
        voices: list[dict[str, object]] = []
        seen_ids: set[int] = set()
        for style in styles:
            if not isinstance(style, dict):
                continue
            style_id = style.get("id")
            if isinstance(style_id, bool) or not isinstance(style_id, int):
                continue
            if style_id in seen_ids:
                continue
            seen_ids.add(style_id)
            style_name = str(style.get("name") or "").strip()
            name_parts = [part for part in (speaker_name, style_name) if part]
            display_name = "-".join(name_parts) if name_parts else f"Voice-{style_id}"
            voices.append(
                {
                    "id": style_id,
                    "name": style_name or f"Voice-{style_id}",
                    "display_name": display_name,
                }
            )
        if not voices:
            continue
        voices.sort(key=lambda item: item["id"])
        roster.append(
            {
                "name": speaker_name or voices[0]["display_name"],
                "voices": voices,
            }
        )
    roster.sort(key=lambda item: str(item.get("name") or "").casefold())
    return roster


def format_voice_sample_filename(
    speaker_id: int,
    display_name: str,
    *,
    width: int = 3,
) -> str:
    safe_name = sanitize_voice_sample_name(display_name)
    return f"{speaker_id:0{width}d}-{safe_name}.mp3"
