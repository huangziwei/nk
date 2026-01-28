from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import quote

from fastapi import Body, FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response

from .book_io import (
    ChapterMetadata,
    LoadedBookMetadata,
    is_original_text_file,
    load_book_metadata,
    update_book_tts_defaults,
)
from .library import BookListing, list_books_sorted
from .refine import create_token_from_selection
from .tts import (
    FFmpegError,
    TTSTarget,
    VoiceVoxClient,
    VoiceVoxError,
    VoiceVoxRuntimeError,
    VoiceVoxUnavailableError,
    _parse_track_number_from_name,
    _synthesize_target_with_client,
    _target_cache_dir,
    discover_voicevox_runtime,
    managed_voicevox_runtime,
    wav_bytes_to_mp3,
)
from .uploads import UploadJob, UploadManager
from .voice_defaults import (
    DEFAULT_INTONATION_SCALE,
    DEFAULT_PITCH_SCALE,
    DEFAULT_SPEAKER_ID,
    DEFAULT_SPEED_SCALE,
)
from .voice_samples import (
    build_sample_text,
    format_voice_sample_filename,
    sanitize_voice_sample_name,
    voice_roster_from_payload,
    voice_samples_from_payload,
)
from .web_assets import NK_APPLE_TOUCH_ICON_PNG, NK_FAVICON_URL


@dataclass(slots=True)
class PlayerConfig:
    root: Path
    speaker: int = 2
    engine_url: str = "http://127.0.0.1:50021"
    engine_runtime: Path | None = None
    engine_wait: float = 30.0
    engine_threads: int | None = None
    ffmpeg_path: str = "ffmpeg"
    pause: float = 0.4
    speed_scale: float | None = None
    pitch_scale: float | None = None
    intonation_scale: float | None = None
    cache_dir: Path | None = None
    keep_cache: bool = True


COVER_EXTENSIONS = (".jpg", ".jpeg", ".png")
BOOKMARKS_FILENAME = ".nk-player-bookmarks.json"
BOOKMARK_STATE_VERSION = 1
_SORT_MODES = {"author", "recent", "played"}
RECENTLY_PLAYED_PREFIX = "__nk_recently_played__"
RECENTLY_PLAYED_LABEL = "Recently Played"
VOICE_SAMPLES_DIR = "samples"
VOICE_SAMPLES_ROSTER_FILENAME = "voices.json"
VOICE_SAMPLES_INDEX_FILENAME = "index.json"
VOICE_SAMPLE_DEFAULT_SPEED = 1.0
VOICE_SAMPLE_DEFAULT_PITCH = 0.0
VOICE_SAMPLE_DEFAULT_INTONATION = 1.0


def _normalize_sort_mode(value: str | None) -> str:
    if not value:
        return "author"
    normalized = value.strip().lower()
    if normalized in _SORT_MODES:
        return normalized
    raise HTTPException(status_code=400, detail="Invalid sort mode.")


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>Player</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <link rel="icon" type="image/svg+xml" href="__NK_FAVICON__">
  <link rel="apple-touch-icon" href="/apple-touch-icon.png">
  <style>
    :root {
      color-scheme: dark;
      font-family: -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Hiragino Sans", sans-serif;
      --bg: #090b12;
      --panel: #141724;
      --panel-alt: #1b1f32;
      --text: #f5f5f5;
      --muted: #9aa0b5;
      --accent: #3b82f6;
      --accent-dark: #2563eb;
      --badge: #20263b;
      --danger: #f87171;
      --radius: 18px;
    }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
    }
    body.modal-open {
      overflow: hidden;
    }
    .hidden {
      display: none !important;
    }
    .sr-only {
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
      border: 0;
    }
    header {
      padding: 1.3rem 1.6rem 1rem;
      background: linear-gradient(135deg, rgba(59,130,246,0.18), transparent);
    }
    .header-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 1rem;
      flex-wrap: wrap;
    }
    .header-title {
      display: flex;
      flex-direction: column;
    }
    .header-actions {
      display: flex;
      align-items: center;
      gap: 0.6rem;
    }
    .header-actions button {
      white-space: nowrap;
    }
    header h1 {
      margin: 0;
      font-size: 1.55rem;
      font-weight: 700;
      letter-spacing: 0.01em;
    }
    header h1 a {
      color: inherit;
      text-decoration: none;
    }
    header h1 a:focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 4px;
      border-radius: 8px;
    }
    header p {
      margin: 0.35rem 0 0;
      color: var(--muted);
      font-size: 0.95rem;
    }
    main {
      padding: 0 1.6rem 2rem;
      display: flex;
      flex-direction: column;
      gap: 1.6rem;
    }
    section.panel {
      background: var(--panel);
      border-radius: var(--radius);
      margin-top: 1rem;
      padding: 1.2rem 1.4rem;
      box-shadow: 0 16px 30px rgba(7, 9, 19, 0.28);
    }
    section.panel.hidden {
      display: none;
    }
    section.panel h2 {
      margin: 0 0 1rem;
      font-size: 1.1rem;
      font-weight: 600;
      letter-spacing: 0.02em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .library-toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 0.75rem;
      margin-bottom: 0.75rem;
    }
    .library-breadcrumbs {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
      align-items: center;
      font-size: 0.9rem;
      color: var(--muted);
    }
    .library-breadcrumbs .breadcrumb {
      background: none;
      border: none;
      padding: 0;
      margin: 0;
      font: inherit;
      color: inherit;
      cursor: pointer;
    }
    .library-breadcrumbs .breadcrumb.current {
      font-weight: 600;
      color: var(--text);
      cursor: default;
    }
    .library-breadcrumbs .breadcrumb:focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 3px;
    }
    .library-breadcrumbs .breadcrumb-divider {
      opacity: 0.6;
    }
    .library-actions {
      display: flex;
      gap: 0.6rem;
      flex-wrap: wrap;
      align-items: center;
    }
    .sort-select {
      display: inline-flex;
      flex-direction: column;
      gap: 0.2rem;
      font-size: 0.8rem;
      color: var(--muted);
    }
    .sort-select select {
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(15,18,32,0.75);
      color: var(--text);
      padding: 0.35rem 0.65rem;
      font-size: 0.85rem;
    }
    .cards {
      display: grid;
      gap: 1rem;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      justify-items: center;
      width: 100%;
    }
    .card {
      background: var(--panel-alt);
      border-radius: calc(var(--radius) - 4px);
      padding: 0.33rem 0.33rem;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      min-height: 160px;
      width: 100%;
      max-width: 420px;
      position: relative;
    }
    .card.empty-card {
      justify-content: center;
      text-align: center;
      color: var(--muted);
      border: 1px dashed rgba(255,255,255,0.08);
    }
    .voice-sample-card {
      min-height: auto;
      padding: 0.75rem 0.9rem 0.9rem;
      gap: 0.6rem;
      width: 260px;
      max-width: 260px;
      flex: 0 0 260px;
    }
    .voice-sample-header {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 0.75rem;
    }
    .voice-sample-name {
      font-weight: 600;
    }
    .voice-sample-id {
      font-size: 0.8rem;
      color: var(--muted);
    }
    .voice-sample-controls {
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }
    .voice-sample-text textarea {
      min-height: 4.5rem;
    }
    .voice-sample-params {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: 0.5rem;
    }
    .voice-sample-actions {
      display: flex;
      align-items: center;
      gap: 0.6rem;
      flex-wrap: wrap;
    }
    .voice-sample-status {
      font-size: 0.8rem;
      color: var(--muted);
    }
    .voice-sample-saved {
      border-top: 1px solid rgba(255, 255, 255, 0.08);
      padding-top: 0.5rem;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }
    .voice-sample-saved-title {
      font-size: 0.8rem;
      color: var(--muted);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.5rem;
    }
    .voice-sample-saved-list {
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }
    .voice-sample-saved-item {
      background: rgba(8, 10, 18, 0.6);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 12px;
      padding: 0.5rem;
      display: flex;
      flex-direction: column;
      gap: 0.4rem;
    }
    .voice-sample-saved-meta {
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
    }
    .voice-sample-text-preview {
      font-size: 0.82rem;
      color: var(--muted);
      white-space: pre-wrap;
    }
    .voice-sample-chips {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
    }
    .voice-sample-chip {
      font-size: 0.72rem;
      color: #cbd5f5;
      background: rgba(148, 163, 184, 0.2);
      padding: 0.2rem 0.4rem;
      border-radius: 999px;
    }
    .voice-sample-saved-actions {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      justify-content: flex-start;
      flex-wrap: wrap;
    }
    .voice-sample-play,
    .voice-sample-delete {
      padding: 0.35rem 0.8rem;
      font-size: 0.8rem;
    }
    .voice-sample-empty {
      font-size: 0.8rem;
      color: var(--muted);
    }
    .voice-samples-list {
      display: flex;
      flex-direction: column;
      gap: 0.8rem;
      width: 100%;
    }
    .voice-samples-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 0.6rem;
    }
    .voice-samples-header h2 {
      margin: 0;
    }
    .voice-samples-intro {
      margin: 0.35rem 0 0;
      color: var(--muted);
      font-size: 0.9rem;
    }
    .voice-sample-group {
      border-radius: calc(var(--radius) - 6px);
      border: 1px solid rgba(255, 255, 255, 0.08);
      background: rgba(12, 16, 26, 0.75);
      padding: 0.35rem 0.6rem 0.8rem;
    }
    .voice-sample-group summary {
      cursor: pointer;
      list-style: none;
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-weight: 600;
      font-size: 0.95rem;
      color: var(--text);
      padding: 0.35rem 0.2rem;
    }
    .voice-sample-group summary::marker {
      display: none;
    }
    .voice-sample-group summary::after {
      content: "▾";
      font-size: 0.8rem;
      color: var(--muted);
      transition: transform 0.2s ease;
    }
    .voice-sample-group[open] summary::after {
      transform: rotate(180deg);
    }
    .voice-sample-group-grid {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-start;
      align-items: stretch;
      gap: 0.8rem;
      margin-top: 0.6rem;
    }
    .voice-samples-actions {
      display: flex;
      align-items: center;
      gap: 0.6rem;
      flex-wrap: wrap;
    }
    .voice-samples-status {
      font-size: 0.85rem;
      color: var(--muted);
    }
    .card-menu-button {
      position: absolute;
      bottom: 0.33rem;
      right: 0.33rem;
      width: 32px;
      height: 32px;
      border-radius: 50%;
      border: none;
      background: rgba(255,255,255,0.08);
      color: var(--text);
      font-size: 1.2rem;
      line-height: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      cursor: pointer;
      z-index: 3;
    }
    .card-menu-button:hover:not(:disabled),
    .card-menu-button:focus-visible {
      background: rgba(59,130,246,0.35);
      outline: none;
    }
    .card-menu {
      position: absolute;
      bottom: 2.5rem;
      right: 0.5rem;
      min-width: 160px;
      background: rgba(10,12,20,0.95);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 12px;
      box-shadow: 0 18px 35px rgba(0,0,0,0.55);
      padding: 0.4rem 0;
      z-index: 5;
    }
    .card-menu.hidden {
      display: none;
    }
    .card-menu button {
      width: 100%;
      text-align: left;
      padding: 0.45rem 1rem;
      background: transparent;
      border: none;
      color: inherit;
      font: inherit;
      cursor: pointer;
    }
    .card-menu button:hover:not(:disabled),
    .card-menu button:focus-visible {
      background: rgba(59,130,246,0.12);
      outline: none;
    }
    .card-menu button.danger {
      background: transparent;
      color: var(--danger);
    }
    .card-menu button.danger:hover:not(:disabled),
    .card-menu button.danger:focus-visible {
      background: rgba(248,113,113,0.18);
      color: #fee2e2;
    }
    .collection-card {
      cursor: pointer;
      transition: transform 0.15s ease, border-color 0.15s ease;
      display: flex;
      flex-direction: column;
      gap: 0.8rem;
      padding: 0.33rem;
    }
    .collection-card:hover {
      transform: translateY(-2px);
      border-color: rgba(59,130,246,0.35);
    }
    .collection-card .collection-covers {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 0.3rem;
    }
    .collection-card .collection-cover-tile {
      position: relative;
      width: 100%;
      aspect-ratio: 1 / 1;
      border-radius: 10px;
      background: rgba(255,255,255,0.04);
      background-size: cover;
      background-position: center center;
      box-shadow: 0 6px 12px rgba(5,6,17,0.35);
    }
    .collection-card .collection-cover-placeholder {
      background: linear-gradient(135deg, rgba(59,130,246,0.18), rgba(147,51,234,0.18));
      box-shadow: none;
    }
    .collection-card .collection-info {
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }
    .collection-card .collection-meta-row {
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .collection-card .collection-icon {
      width: 48px;
      height: 48px;
      border-radius: 14px;
      background: rgba(59,130,246,0.12);
      color: var(--accent);
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1.25rem;
    }
    .collection-card .collection-name {
      font-weight: 600;
      font-size: 1rem;
    }
    .collection-card .collection-meta {
      font-size: 0.85rem;
      color: var(--muted);
    }
    .epub-alert {
      border: 1px solid rgba(59,130,246,0.25);
      border-radius: calc(var(--radius) - 6px);
      background: rgba(15,18,32,0.75);
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      margin-bottom: 1rem;
      padding: 0;
      overflow: hidden;
    }
    .epub-alert-header {
      display: flex;
      justify-content: space-between;
      gap: 0.75rem;
      align-items: center;
      flex-wrap: wrap;
      cursor: pointer;
      padding: 0.9rem 1rem;
      list-style: none;
    }
    .epub-alert summary::-webkit-details-marker {
      display: none;
    }
    .epub-alert[open] .epub-alert-header {
      border-bottom: 1px solid rgba(59,130,246,0.15);
    }
    .epub-alert-header strong {
      font-size: 1rem;
    }
    .epub-alert-note {
      font-size: 0.85rem;
      color: var(--muted);
      margin: 0;
    }
    .epub-list {
      display: flex;
      flex-direction: column;
      gap: 0.6rem;
      padding: 0 1rem 0.75rem;
    }
    .epub-item {
      display: flex;
      justify-content: space-between;
      gap: 0.8rem;
      align-items: center;
      padding: 0.65rem 0.4rem;
      border-bottom: 1px solid rgba(255,255,255,0.05);
    }
    .epub-item:last-child {
      border-bottom: none;
    }
    .epub-item-info {
      display: flex;
      flex-direction: column;
      gap: 0.2rem;
      font-size: 0.9rem;
    }
    .epub-item-name {
      font-weight: 600;
    }
    .epub-item-meta {
      font-size: 0.8rem;
      color: var(--muted);
    }
    .upload-card {
      background: rgba(27, 31, 50, 0.9);
      position: relative;
      overflow: hidden;
    }
    .upload-card .upload-drop {
      border-radius: calc(var(--radius) - 8px);
      border: 1px dashed rgba(59,130,246,0.5);
      padding: 0.9rem;
      text-align: center;
      cursor: pointer;
      background: rgba(59,130,246,0.07);
      display: flex;
      flex-direction: column;
      gap: 0.4rem;
      outline: none;
      transition: border-color 0.15s ease, background 0.15s ease, opacity 0.15s ease;
    }
    .upload-card .upload-drop strong {
      font-size: 1.05rem;
      letter-spacing: 0.01em;
    }
    .upload-card .upload-drop p {
      margin: 0;
      font-size: 0.88rem;
      color: var(--muted);
    }
    .upload-card .upload-drop.dragging {
      border-color: rgba(59,130,246,0.9);
      background: rgba(59,130,246,0.15);
    }
    .upload-card .upload-drop.upload-busy {
      opacity: 0.7;
      pointer-events: none;
    }
    .upload-card .upload-error {
      min-height: 1.1rem;
      font-size: 0.85rem;
      color: var(--danger);
    }
    .upload-card .upload-jobs {
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
      max-height: 240px;
      overflow-y: auto;
    }
    .upload-card .upload-empty {
      color: var(--muted);
      font-size: 0.85rem;
      text-align: center;
    }
    .upload-card input[type="file"][data-role="upload-input"] {
      position: absolute;
      width: 1px;
      height: 1px;
      opacity: 0;
      pointer-events: none;
    }
    .upload-card .upload-job {
      border: 1px solid rgba(255,255,255,0.07);
      border-radius: 12px;
      padding: 0.6rem 0.75rem;
      background: rgba(15,18,32,0.9);
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }
    .upload-card .upload-job[data-status="success"] {
      border-color: rgba(34,197,94,0.5);
    }
    .upload-card .upload-job[data-status="error"] {
      border-color: rgba(248,113,113,0.5);
    }
    .upload-card .upload-job-header {
      display: flex;
      justify-content: space-between;
      gap: 0.4rem;
      font-size: 0.85rem;
      align-items: center;
    }
    .upload-card .upload-job-title {
      font-weight: 600;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .upload-card .upload-job-status {
      font-size: 0.78rem;
      border-radius: 999px;
      padding: 0.1rem 0.6rem;
      border: 1px solid rgba(255,255,255,0.15);
    }
    .upload-card .upload-job[data-status="success"] .upload-job-status {
      color: #bbf7d0;
      border-color: rgba(34,197,94,0.6);
    }
    .upload-card .upload-job[data-status="error"] .upload-job-status {
      color: var(--danger);
      border-color: rgba(248,113,113,0.6);
    }
    .upload-card .upload-job-message {
      font-size: 0.82rem;
      color: var(--muted);
      min-height: 1rem;
    }
    .upload-card .upload-target {
      font-size: 0.78rem;
      font-weight: 600;
    }
    .upload-card .upload-progress {
      height: 6px;
      background: rgba(255,255,255,0.08);
      border-radius: 999px;
      overflow: hidden;
    }
    .upload-card .upload-progress-bar {
      height: 100%;
      width: 0%;
      background: var(--accent);
      transition: width 0.2s ease;
    }
    .card .title {
      font-size: 1.05rem;
      font-weight: 600;
      line-height: 1.4;
      word-break: break-word;
    }
    .badges {
      display: flex;
      flex-wrap: wrap;
      gap: 0.45rem;
    }
    .badge {
      background: var(--badge);
      color: var(--muted);
      border-radius: 999px;
      padding: 0.25rem 0.7rem;
      font-size: 0.78rem;
      letter-spacing: 0.02em;
      white-space: nowrap;
      flex: 0 0 auto;
    }
    .badge.success {
      background: rgba(34, 197, 94, 0.28);
      color: #bbf7d0;
    }
    .badge.played {
      background: rgba(59, 130, 246, 0.28);
      color: #bfdbfe;
    }
    .badge.warning {
      background: rgba(251, 191, 36, 0.3);
      color: #fcd34d;
    }
    .badge.muted {
      background: rgba(148, 163, 184, 0.2);
      color: #cbd5f5;
    }
    .badge.danger {
      background: rgba(248, 113, 113, 0.25);
      color: #fecaca;
    }
    button {
      border: none;
      border-radius: 999px;
      padding: 0.5rem 1.1rem;
      font-size: 0.9rem;
      font-weight: 600;
      letter-spacing: 0.02em;
      cursor: pointer;
      color: #fff;
      background: var(--accent);
      transition: background 0.2s ease;
    }
    button.danger {
      background: var(--danger);
    }
    button.secondary {
      background: rgba(148, 163, 184, 0.18);
      color: var(--muted);
    }
    button:hover:not(:disabled) {
      background: var(--accent-dark);
    }
    button.secondary:hover:not(:disabled) {
      background: rgba(148, 163, 184, 0.32);
      color: #e2e8f0;
    }
    button.danger:hover:not(:disabled) {
      background: #dc2626;
    }
    button:disabled {
      opacity: 0.5;
      cursor: default;
    }
    .action-bar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.8rem;
      flex-wrap: wrap;
      margin-bottom: 1rem;
    }
    .voice-controls {
      margin-bottom: 1rem;
      background: rgba(20, 23, 36, 0.8);
      border-radius: calc(var(--radius) - 6px);
      padding: 0.4rem 0.8rem 0.8rem;
      border: 1px solid rgba(255, 255, 255, 0.05);
    }
    .voice-controls summary {
      cursor: pointer;
      list-style: none;
      font-weight: 600;
      font-size: 0.95rem;
      letter-spacing: 0.03em;
      color: var(--muted);
      padding: 0.4rem 0.4rem 0.2rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .voice-controls summary::marker {
      display: none;
    }
    .voice-controls summary::after {
      content: "▸";
      font-size: 0.8rem;
      transition: transform 0.2s ease;
    }
    .voice-controls[open] summary::after {
      transform: rotate(90deg);
    }
    .voice-controls-content {
      padding: 0.6rem 0.4rem 0;
      display: flex;
      flex-direction: column;
      gap: 0.6rem;
    }
    .voice-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 0.6rem;
    }
    .voice-field {
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
      font-size: 0.85rem;
      color: var(--muted);
    }
    .voice-field input {
      border-radius: 10px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      background: rgba(10, 12, 22, 0.8);
      color: var(--text);
      padding: 0.4rem 0.6rem;
      font-size: 1rem;
    }
    .voice-field textarea {
      border-radius: 10px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      background: rgba(10, 12, 22, 0.8);
      color: var(--text);
      padding: 0.4rem 0.6rem;
      font-size: 0.95rem;
      resize: vertical;
    }
    .voice-actions {
      display: flex;
      gap: 0.6rem;
      flex-wrap: wrap;
      align-items: center;
    }
    .voice-status {
      font-size: 0.85rem;
      color: var(--muted);
    }
    .action-bar .left {
      display: flex;
      align-items: center;
      gap: 0.8rem;
      flex-wrap: wrap;
    }
    .action-bar .left h3 {
      margin: 0;
      font-size: 1.2rem;
      font-weight: 600;
    }
    .metrics {
      display: flex;
      gap: 0.6rem;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 0.85rem;
    }
    .chapters {
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }
    .chapter {
      background: var(--panel-alt);
      border-radius: calc(var(--radius) - 6px);
      padding: 0.85rem 1rem;
      display: flex;
      flex-direction: column;
      gap: 0.6rem;
    }
    .chapter .bookmark-list {
      margin-top: 0.2rem;
      border-top: 1px solid rgba(148,163,184,0.2);
      padding-top: 0.4rem;
    }
    .chapter.playing {
      outline: 2px solid rgba(59, 130, 246, 0.65);
      outline-offset: 2px;
    }
    .chapter-header {
      display: flex;
      justify-content: space-between;
      gap: 0.75rem;
      align-items: center;
    }
    .chapter-header .name {
      font-weight: 600;
      line-height: 1.35;
      word-break: break-word;
      flex: 1;
    }
    .chapter-footer {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.75rem;
      flex-wrap: wrap;
    }
    .chapter-footer .badges {
      flex: 1;
    }
    .chapter-footer .badges.controls {
      flex: 0 0 auto;
      display: flex;
      flex-wrap: nowrap;
      justify-content: flex-end;
      gap: 0.5rem;
    }
    .card .cover-wrapper {
      position: relative;
      width: 100%;
    }
    .card .cover {
      width: 100%;
      border-radius: calc(var(--radius) - 6px);
      object-fit: cover;
      display: block;
    }
    .card .cover-link {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      opacity: 0;
      border: none;
      background: transparent;
      cursor: pointer;
    }
    .card .author {
      color: var(--muted);
      font-size: 0.85rem;
    }
    audio#player {
      position: absolute;
      width: 0;
      height: 0;
      opacity: 0;
      pointer-events: none;
    }
    .chapter-player {
      margin-top: 0.75rem;
      padding: 1rem 1.2rem 1.2rem;
      border: 1px solid rgba(148, 163, 184, 0.2);
      border-radius: calc(var(--radius) - 4px);
      background: linear-gradient(135deg, rgba(59,130,246,0.08), rgba(9,11,18,0.9));
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
    }
    .chapter-player.hidden {
      display: none;
    }
    .status-line {
      margin-top: 0.75rem;
      color: var(--muted);
      font-size: 0.9rem;
    }
    .player-meta {
      display: flex;
      gap: 1rem;
      align-items: center;
    }
    .player-cover {
      width: 88px;
      height: 88px;
      border-radius: 16px;
      object-fit: cover;
      background: rgba(255,255,255,0.05);
      flex-shrink: 0;
    }
    .player-cover.hidden {
      display: none;
    }
    .player-text {
      flex: 1;
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }
    .player-text .now-playing {
      font-size: 1.15rem;
      font-weight: 600;
    }
    .player-subtitle {
      font-size: 0.9rem;
      color: var(--muted);
    }
    .player-progress {
      margin-top: 1rem;
    }
    .player-progress input[type="range"] {
      -webkit-appearance: none;
      appearance: none;
      width: 100%;
      height: 10px;
      border-radius: 999px;
      background: linear-gradient(var(--accent), var(--accent)) no-repeat;
      background-color: rgba(255,255,255,0.1);
      background-size: var(--progress, 0%) 100%;
      outline: none;
      cursor: pointer;
      transition: background-size 0.2s ease;
    }
    .player-progress input[type="range"]::-webkit-slider-thumb {
      -webkit-appearance: none;
      width: 16px;
      height: 16px;
      border-radius: 50%;
      background: var(--accent);
      box-shadow: 0 0 0 4px rgba(59,130,246,0.3);
      border: none;
    }
    .player-progress input[type="range"]::-moz-range-thumb {
      width: 16px;
      height: 16px;
      border-radius: 50%;
      background: var(--accent);
      border: none;
      box-shadow: 0 0 0 4px rgba(59,130,246,0.3);
    }
    .player-progress input[type="range"]:disabled {
      opacity: 0.4;
      cursor: not-allowed;
    }
    .progress-times {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-top: 0.35rem;
      font-size: 0.85rem;
      color: var(--muted);
      gap: 0.5rem;
    }
    .progress-times .time-left {
      text-align: center;
      flex: 1;
      font-weight: 600;
      color: var(--text);
    }
    .player-primary-controls {
      margin-top: 1.2rem;
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      align-items: center;
      gap: 1rem;
    }
    .control-btn {
      border: none;
      background: rgba(255,255,255,0.08);
      color: var(--text);
      border-radius: 999px;
      height: 52px;
      font-size: 1.1rem;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.35rem;
      cursor: pointer;
      transition: transform 0.2s ease, background 0.2s ease;
    }
    .control-btn:disabled {
      opacity: 0.4;
      cursor: not-allowed;
    }
    .control-btn:not(.play-toggle):not(:disabled):hover {
      transform: translateY(-1px);
      background: rgba(255,255,255,0.14);
    }
    .control-btn.play-toggle:not(:disabled):hover {
      transform: translateY(-1px);
      background: var(--text);
    }
    .control-btn.play-toggle {
      background: var(--text);
      color: var(--panel);
      border-radius: 50%;
      height: 72px;
      width: 72px;
      justify-self: center;
      position: relative;
    }
    @media (max-width: 680px) {
      .player-primary-controls {
        grid-template-columns: repeat(5, minmax(0, 1fr));
        grid-template-areas: "prev rew play fwd next";
        gap: 0.65rem;
        justify-items: center;
      }
      .control-btn {
        width: 76px;
        height: 76px;
        min-width: 76px;
        min-height: 76px;
        border-radius: 50%;
      }
      .control-btn.play-toggle {
        width: 92px;
        height: 92px;
        min-width: 92px;
        min-height: 92px;
      }
      #player-prev {
        grid-area: prev;
      }
      #player-rewind {
        grid-area: rew;
        justify-self: flex-start;
      }
      #player-toggle {
        grid-area: play;
      }
      #player-forward {
        grid-area: fwd;
        justify-self: flex-end;
      }
      #player-next {
        grid-area: next;
      }
    }
    .control-btn.play-toggle::before,
    .control-btn.play-toggle::after {
      content: '';
      position: absolute;
      transition: opacity 0.15s ease;
    }
    .control-btn.play-toggle::before {
      width: 0;
      height: 0;
      border-top: 14px solid transparent;
      border-bottom: 14px solid transparent;
      border-left: 22px solid var(--panel);
      left: calc(50% - 9px);
      top: calc(50% - 14px);
      opacity: 1;
    }
    .control-btn.play-toggle::after {
      width: 12px;
      height: 26px;
      border-radius: 2px;
      background: var(--panel);
      box-shadow: 14px 0 0 var(--panel);
      left: calc(50% - 13px);
      top: calc(50% - 13px);
      opacity: 0;
    }
    .control-btn.play-toggle.playing::before {
      opacity: 0;
    }
    .control-btn.play-toggle.playing::after {
      opacity: 1;
    }
    .control-btn.compact {
      background: transparent;
      border: 1px solid rgba(255,255,255,0.2);
    }
    .radial-skip {
      width: 42px;
      height: 42px;
      border-radius: 50%;
      position: relative;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      color: var(--text);
    }
    .radial-skip svg {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      pointer-events: none;
      transform-origin: 50% 50%;
    }
    .radial-skip svg circle {
      fill: none;
      stroke: currentColor;
      stroke-width: 2.6;
      stroke-dasharray: 95 48;
      transform: rotate(-90deg);
      transform-origin: 50% 50%;
    }
    .radial-skip svg .radial-tail {
      fill: none;
      stroke: currentColor;
      stroke-width: 2.6;
      stroke-linecap: round;
    }
    .radial-skip svg .radial-arrowhead {
      fill: currentColor;
      transform-origin: 32px 10px;
    }
    .radial-skip.fwd svg circle {
      transform: rotate(-35deg);
    }
    .radial-skip .radial-value {
      font-size: 0.9rem;
      font-weight: 700;
      letter-spacing: 0.02em;
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
    }
    .skip-icon {
      width: 26px;
      height: 18px;
      position: relative;
      display: inline-block;
    }
    .skip-icon::before,
    .skip-icon::after {
      content: '';
      position: absolute;
      top: 50%;
      width: 0;
      height: 0;
      border-top: 9px solid transparent;
      border-bottom: 9px solid transparent;
    }
    .skip-icon.prev::before {
      left: 60%;
      border-right: 10px solid currentColor;
      transform: translate(-50%, -50%);
    }
    .skip-icon.prev::after {
      left: 35%;
      border-right: 10px solid currentColor;
      transform: translate(-50%, -50%);
    }
    .skip-icon.next::before {
      left: 40%;
      border-left: 10px solid currentColor;
      transform: translate(-50%, -50%);
    }
    .skip-icon.next::after {
      left: 65%;
      border-left: 10px solid currentColor;
      transform: translate(-50%, -50%);
    }
    .player-quick-actions {
      margin-top: 1rem;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 0.75rem;
      align-items: stretch;
    }
    .quick-btn {
      width: 100%;
      border-radius: 999px;
      border: 1px solid rgba(255,255,255,0.18);
      background: rgba(255,255,255,0.04);
      color: var(--text);
      padding: 0.55rem 0.9rem;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.4rem;
      font-weight: 600;
    }
    .quick-btn.primary {
      background: var(--accent);
      border-color: transparent;
      color: #fff;
    }
    .quick-btn:disabled {
      opacity: 0.45;
      cursor: not-allowed;
    }
    .quick-btn .icon {
      width: 20px;
      height: 18px;
      position: relative;
      display: inline-block;
    }
    .icon-airplay {
      border: 2px solid currentColor;
      border-top: 0;
      border-radius: 4px;
      width: 20px;
      height: 12px;
    }
    .icon-airplay::after {
      content: '';
      display: block;
      width: 0;
      height: 0;
      margin: 4px auto 0;
      border-left: 6px solid transparent;
      border-right: 6px solid transparent;
      border-top: 8px solid currentColor;
    }
    .bookmark-status {
      font-size: 0.85rem;
      color: var(--muted);
    }
    .bookmark-list {
      margin-top: 0.5rem;
      display: flex;
      flex-direction: column;
      gap: 0.4rem;
    }
    .bookmark-item {
      background: rgba(255,255,255,0.05);
      border-radius: 12px;
      padding: 0.4rem 0.6rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.5rem;
    }
    .bookmark-item .info {
      display: flex;
      flex-direction: column;
      gap: 0.1rem;
      font-size: 0.85rem;
      flex: 1;
      min-width: 0;
    }
    .bookmark-item .label {
      font-weight: 600;
      word-break: break-word;
    }
    .bookmark-item .time {
      color: var(--muted);
    }
    .bookmark-item .actions {
      display: flex;
      gap: 0.4rem;
      flex-shrink: 0;
      align-items: center;
    }
    .bookmark-item button {
      font-size: 0.8rem;
      padding: 0.3rem 0.6rem;
    }
    .bookmark-empty {
      font-size: 0.85rem;
      color: var(--muted);
    }
    .bookmark-panel {
      background: rgba(20, 23, 36, 0.6);
      border: 1px solid rgba(59, 130, 246, 0.2);
      border-radius: calc(var(--radius) - 8px);
      padding: 0.8rem 1rem;
      margin-bottom: 1rem;
    }
    .bookmark-panel.hidden {
      display: none;
    }
    .bookmark-panel .header {
      display: flex;
      flex-wrap: wrap;
      gap: 0.6rem;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 0.6rem;
    }
    .bookmark-panel h4 {
      margin: 0;
      font-size: 1rem;
      letter-spacing: 0.02em;
      color: var(--muted);
    }
    .bookmark-panel .bookmark-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 0.6rem;
      align-items: center;
    }
    .modal {
      position: fixed;
      inset: 0;
      background: rgba(5, 6, 17, 0.78);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 1.5rem;
      z-index: 120;
    }
    .modal.hidden {
      display: none;
    }
    .modal-card {
      background: var(--panel);
      border-radius: calc(var(--radius) - 8px);
      width: min(520px, 100%);
      max-height: 90vh;
      padding: 1.25rem;
      box-shadow: 0 20px 45px rgba(5, 6, 17, 0.5);
      display: flex;
      flex-direction: column;
      gap: 0.9rem;
      border: 1px solid rgba(59,130,246,0.3);
    }
    .modal-card h3 {
      margin: 0;
      font-size: 1.1rem;
    }
    .modal-meta {
      margin: -0.5rem 0 0;
      color: var(--muted);
      font-size: 0.85rem;
    }
    .modal-card textarea {
      width: 100%;
      min-height: 8rem;
      border-radius: 10px;
      border: 1px solid rgba(255,255,255,0.18);
      background: rgba(0,0,0,0.35);
      color: var(--text);
      font-size: 0.9rem;
      font-family: inherit;
      line-height: 1.4;
      padding: 0.6rem 0.75rem;
      resize: vertical;
      box-sizing: border-box;
    }
    .modal-card textarea:focus {
      outline: none;
      border-color: rgba(59,130,246,0.7);
      box-shadow: 0 0 0 1px rgba(59,130,246,0.4);
    }
    .modal-actions {
      display: flex;
      justify-content: flex-end;
      gap: 0.6rem;
    }
    @media (max-width: 640px) {
      header {
        padding: 1.2rem 1.1rem 0.9rem;
      }
      main {
        padding: 0 1.1rem 1.6rem;
      }
      .library-toolbar {
        flex-direction: column;
        align-items: flex-start;
      }
      .library-actions {
        width: 100%;
        justify-content: space-between;
      }
      .cards {
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      }
      .action-bar {
        flex-direction: column;
        align-items: flex-start;
      }
      .action-bar .left {
        flex-direction: column;
        align-items: flex-start;
      }
      .chapter {
        padding: 0.8rem 0.9rem;
      }
      .chapter-footer {
        align-items: flex-start;
      }
      .chapter > #player-dock.chapter-player {
        margin-left: -0.9rem;
        margin-right: -0.9rem;
      }
      .chapter-player {
        padding: 0.85rem 0.95rem 1rem;
      }
      .player-meta {
        flex-direction: column;
        gap: 0.55rem;
        align-items: flex-start;
      }
      .player-cover {
        width: 64px;
        height: 64px;
        border-radius: 14px;
      }
      .player-text .now-playing {
        font-size: 1rem;
      }
      .player-progress input[type="range"] {
        height: 8px;
      }
      .player-primary-controls {
        grid-template-columns: repeat(3, minmax(0, 1fr));
        grid-template-areas:
          "prev play next"
          "rew play fwd";
        gap: 0.55rem;
      }
      #player-prev {
        grid-area: prev;
        justify-self: flex-start;
      }
      #player-next {
        grid-area: next;
        justify-self: flex-end;
      }
      #player-rewind {
        grid-area: rew;
      }
      #player-forward {
        grid-area: fwd;
      }
      #player-toggle {
        grid-area: play;
        justify-self: center;
      }
      .control-btn {
        height: 48px;
        font-size: 0.95rem;
      }
      .control-btn.play-toggle {
        width: 68px;
        height: 68px;
      }
      .player-quick-actions {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      #bookmark-add.quick-btn {
        grid-column: span 2;
      }
      .bookmark-item {
        flex-direction: column;
        align-items: stretch;
      }
      .bookmark-item .actions {
        justify-content: flex-start;
        flex-wrap: wrap;
      }
      .bookmark-item .actions button {
        flex: 1 1 auto;
        min-width: 30%;
      }
      .modal-card {
        padding: 1rem;
      }
      .modal-card textarea {
        min-height: 6rem;
      }
      .epub-alert-header {
        flex-direction: column;
        align-items: flex-start;
      }
      .epub-item {
        flex-direction: column;
        align-items: flex-start;
      }
      .voice-sample-card {
        width: 100%;
        max-width: 100%;
        flex: 1 1 100%;
      }
    }
  </style>
</head>
<body>
  <header>
    <div class="header-row">
      <div class="header-title">
        <h1><a href="/" id="home-link">nk Player</a></h1>
        <p>Stream your EPUB chapter by chapter.</p>
      </div>
      <div class="header-actions">
        <button id="voice-samples-link" class="secondary" type="button">Voice samples</button>
      </div>
    </div>
  </header>
  <main>
    <section class="panel" id="books-panel">
      <h2>Library</h2>
      <div class="library-toolbar">
        <div class="library-breadcrumbs" id="library-breadcrumb"></div>
        <div class="library-actions">
          <button id="library-back" class="secondary hidden" type="button">◀ Up one level</button>
          <label class="sort-select" for="books-sort">
            <select id="books-sort">
              <option value="author">Author · Title</option>
              <option value="recent">Recently Added</option>
              <option value="played">Recently Played</option>
            </select>
          </label>
        </div>
      </div>
      <details class="epub-alert hidden" id="pending-epubs">
        <summary class="epub-alert-header">
          <div>
            <strong>Unprocessed EPUBs</strong>
            <p class="epub-alert-note">These EPUB files are in this folder but haven't been chapterized yet.</p>
          </div>
          <button type="button" class="secondary" id="epub-chapterize-all">Chapterize all</button>
        </summary>
        <div class="epub-list" id="pending-epub-list"></div>
      </details>
      <div class="cards collection-cards hidden" id="collections-grid"></div>
      <div class="cards" id="books-grid"></div>
    </section>

    <section class="panel hidden" id="voice-samples-page">
      <div class="voice-samples-header">
        <div>
          <h2>Voice samples</h2>
          <p class="voice-samples-intro">Generate sample clips on demand with custom text and tuning.</p>
        </div>
      </div>
      <div class="voice-controls-content">
        <div class="voice-samples-actions">
          <button id="voice-samples-refresh" class="secondary" type="button">Load voices</button>
          <span class="voice-samples-status" id="voice-samples-status"></span>
        </div>
        <div class="voice-samples-list" id="voice-samples-grid"></div>
      </div>
    </section>

    <section class="panel hidden" id="chapters-panel">
      <div class="action-bar">
        <div class="left">
          <button id="back-button" class="secondary">◀ Library</button>
          <div>
            <h3 id="chapters-title"></h3>
            <div class="metrics" id="chapters-metrics"></div>
          </div>
        </div>
        <div class="right">
          <button id="play-book" disabled>Resume last play</button>
          <button id="restart-book" class="secondary">Restart Book</button>
        </div>
      </div>
      <details class="voice-controls">
        <summary>Voice settings</summary>
        <div class="voice-controls-content">
          <div class="voice-grid">
            <label class="voice-field">
              Speaker
              <input type="number" id="voice-speaker" min="1" step="1">
            </label>
            <label class="voice-field">
              Speed
              <input type="number" id="voice-speed" step="0.01">
            </label>
            <label class="voice-field">
              Pitch
              <input type="number" id="voice-pitch" step="0.01">
            </label>
            <label class="voice-field">
              Intonation
              <input type="number" id="voice-intonation" step="0.01">
            </label>
          </div>
          <div class="voice-actions">
            <button id="voice-save">Save voice defaults</button>
            <button id="voice-reset" class="secondary">Reset to global defaults</button>
            <span class="voice-status" id="voice-status" style="color: var(--muted);"></span>
          </div>
        </div>
      </details>
      <div class="chapters" id="chapters-list"></div>
    </section>

    <div id="player-dock" class="chapter-player hidden">
      <div class="player-meta">
        <img id="player-cover" class="player-cover hidden" alt="Book cover">
        <div class="player-text">
          <div id="now-playing" class="now-playing">Select a chapter to begin.</div>
          <div id="player-subtitle" class="player-subtitle"></div>
        </div>
      </div>
      <div class="player-progress">
        <input id="player-seek" type="range" min="0" max="1000" value="0" step="1" disabled>
        <div class="progress-times">
          <span id="player-current-time">0:00</span>
          <span id="player-time-left" class="time-left">--</span>
          <span id="player-duration">0:00</span>
        </div>
      </div>
      <div class="player-primary-controls">
        <button id="player-prev" class="control-btn compact" type="button" aria-label="Previous chapter">
          <span class="skip-icon prev" aria-hidden="true"></span>
        </button>
        <button id="player-rewind" class="control-btn compact" type="button" aria-label="Rewind 15 seconds">
          <span class="radial-skip rew" aria-hidden="true">
            <svg viewBox="0 0 64 64" role="presentation" focusable="false">
              <circle class="radial-arc" cx="32" cy="32" r="18"></circle>
              <path class="radial-arrowhead" d="M32 9.5L22.5 14.5L32 19.5Z"></path>
            </svg>
            <span class="radial-value">15</span>
          </span>
        </button>
        <button id="player-toggle" class="control-btn play-toggle" type="button" aria-label="Play">
          <span class="sr-only">Play</span>
        </button>
        <button id="player-forward" class="control-btn compact" type="button" aria-label="Forward 15 seconds">
          <span class="radial-skip fwd" aria-hidden="true">
            <svg viewBox="0 0 64 64" role="presentation" focusable="false">
              <circle class="radial-arc" cx="32" cy="32" r="18"></circle>
              <path class="radial-arrowhead" d="M32 9.5L41.5 14.5L32 19.5Z"></path>
            </svg>
            <span class="radial-value">15</span>
          </span>
        </button>
        <button id="player-next" class="control-btn compact" type="button" aria-label="Next chapter">
          <span class="skip-icon next" aria-hidden="true"></span>
        </button>
      </div>
      <div class="player-quick-actions">
        <button id="player-speed" class="quick-btn" type="button">1.0x Speed</button>
        <button id="player-airplay" class="quick-btn" type="button">
          <span class="icon icon-airplay" aria-hidden="true"></span>
          AirPlay
        </button>
        <button id="bookmark-add" class="quick-btn primary" type="button">Add bookmark</button>
      </div>
      <div class="status-line" id="status">Idle</div>
      <audio id="player" preload="none" aria-hidden="true"></audio>
    </div>
  </main>

  <div id="note-modal" class="modal hidden" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="note-modal-title">
      <h3 id="note-modal-title">Bookmark notes</h3>
      <p class="modal-meta" id="note-modal-meta" hidden></p>
      <textarea id="note-input" rows="6" placeholder="Add notes…"></textarea>
      <div class="modal-actions">
        <button id="note-cancel" class="secondary">Cancel</button>
        <button id="note-save">Save</button>
      </div>
    </div>
  </div>
  <div id="delete-modal" class="modal hidden" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="delete-modal-title">
      <h3 id="delete-modal-title">Delete book</h3>
      <p class="modal-meta" id="delete-modal-message"></p>
      <div class="modal-actions">
        <button type="button" class="secondary" id="delete-cancel">Cancel</button>
        <button type="button" class="danger" id="delete-confirm">Delete</button>
      </div>
    </div>
  </div>
  <div id="replace-modal" class="modal hidden" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="replace-modal-title">
      <h3 id="replace-modal-title">Replace existing book?</h3>
      <p class="modal-meta" id="replace-modal-message"></p>
      <div class="modal-actions">
        <button type="button" class="secondary" id="replace-cancel">Cancel</button>
        <button type="button" id="replace-confirm">Replace</button>
      </div>
    </div>
  </div>

  <script type="application/json" id="nk-player-config">__NK_PLAYER_CONFIG__</script>
  <script>
    const booksPanel = document.getElementById('books-panel');
    const booksGrid = document.getElementById('books-grid');
    const collectionsGrid = document.getElementById('collections-grid');
    const voiceSamplesPage = document.getElementById('voice-samples-page');
    const voiceSamplesGrid = document.getElementById('voice-samples-grid');
    const voiceSamplesRefreshBtn = document.getElementById('voice-samples-refresh');
    const voiceSamplesStatus = document.getElementById('voice-samples-status');
    const voiceSamplesLink = document.getElementById('voice-samples-link');
    const libraryBreadcrumb = document.getElementById('library-breadcrumb');
    const libraryBackButton = document.getElementById('library-back');
    const chaptersPanel = document.getElementById('chapters-panel');
    const homeLink = document.getElementById('home-link');
    const chaptersList = document.getElementById('chapters-list');
    const chaptersTitle = document.getElementById('chapters-title');
    const chaptersMetrics = document.getElementById('chapters-metrics');
    const backButton = document.getElementById('back-button');
    const playBookBtn = document.getElementById('play-book');
    const restartBookBtn = document.getElementById('restart-book');
    const playerDock = document.getElementById('player-dock');
    const player = document.getElementById('player');
    const nowPlaying = document.getElementById('now-playing');
    const statusLine = document.getElementById('status');
    const playerCover = document.getElementById('player-cover');
    const playerSubtitle = document.getElementById('player-subtitle');
    const playerSeek = document.getElementById('player-seek');
    const playerCurrentTime = document.getElementById('player-current-time');
    const playerDurationLabel = document.getElementById('player-duration');
    const playerTimeLeft = document.getElementById('player-time-left');
    const playerPlayToggle = document.getElementById('player-toggle');
    const playerRewindBtn = document.getElementById('player-rewind');
    const playerForwardBtn = document.getElementById('player-forward');
    const playerPrevBtn = document.getElementById('player-prev');
    const playerNextBtn = document.getElementById('player-next');
    const playerAirPlayBtn = document.getElementById('player-airplay');
    const playerSpeedBtn = document.getElementById('player-speed');
    const lastPlayStatus = document.getElementById('last-play-status');
    const bookmarkAddBtn = document.getElementById('bookmark-add');
    const bookmarkList = document.getElementById('bookmark-list');
    const bookmarkPanel = document.getElementById('bookmark-panel');
    const noteModal = document.getElementById('note-modal');
    const noteTextarea = document.getElementById('note-input');
    const noteModalMeta = document.getElementById('note-modal-meta');
    const noteSaveBtn = document.getElementById('note-save');
    const noteCancelBtn = document.getElementById('note-cancel');
    const deleteModal = document.getElementById('delete-modal');
    const deleteModalMessage = document.getElementById('delete-modal-message');
    const deleteConfirmBtn = document.getElementById('delete-confirm');
    const deleteCancelBtn = document.getElementById('delete-cancel');
    const replaceModal = document.getElementById('replace-modal');
    const replaceModalMessage = document.getElementById('replace-modal-message');
    const replaceConfirmBtn = document.getElementById('replace-confirm');
    const replaceCancelBtn = document.getElementById('replace-cancel');
    const PLAYER_SEEK_MAX = 1000;
    const PLAYBACK_RATES = [0.75, 0.9, 1, 1.15, 1.3, 1.5, 1.75, 2];
    const SPEED_STORAGE_KEY = 'nk-player-speed';
    const SEEK_STEP = 15;
    const PREVIOUS_CHAPTER_RESTART_THRESHOLD = 5;
    const RESUME_TARGET_TOLERANCE = 0.5;
    const CHAPTER_TITLE_MAX_LENGTH = 50;
    let isScrubbing = false;
    let playbackRateIndex = PLAYBACK_RATES.indexOf(1);
    let voiceSamplePlayer = null;
    const voiceSpeakerInput = document.getElementById('voice-speaker');
    const voiceSpeedInput = document.getElementById('voice-speed');
    const voicePitchInput = document.getElementById('voice-pitch');
    const voiceIntonationInput = document.getElementById('voice-intonation');
    const voiceSaveBtn = document.getElementById('voice-save');
    const voiceResetBtn = document.getElementById('voice-reset');
    const voiceStatus = document.getElementById('voice-status');
    const booksSortSelect = document.getElementById('books-sort');
    const booksSortWrapper = booksSortSelect ? booksSortSelect.closest('.sort-select') : null;
    const pendingEpubPanel = document.getElementById('pending-epubs');
    const pendingEpubList = document.getElementById('pending-epub-list');
    const pendingEpubAllBtn = document.getElementById('epub-chapterize-all');
    if (pendingEpubPanel) {
      pendingEpubPanel.open = false;
    }
    const playerConfigNode = document.getElementById('nk-player-config');
    let readerBaseUrl = null;
    let initialView = 'library';
    if (playerConfigNode && typeof playerConfigNode.textContent === 'string') {
      try {
        const payload = JSON.parse(playerConfigNode.textContent);
        if (payload && typeof payload.reader_url === 'string' && payload.reader_url.trim()) {
          readerBaseUrl = payload.reader_url.trim();
        }
        if (payload && typeof payload.view === 'string' && payload.view.trim()) {
          initialView = payload.view.trim();
        }
      } catch (err) {
        console.warn('Failed to parse nk player config:', err);
      }
    }
    const trimmedPath = window.location.pathname.replace(/\/+$/, '');
    if (initialView === 'library' && trimmedPath.endsWith('/voice-samples')) {
      initialView = 'voice-samples';
    }

    function isIpLikeHost(hostname) {
      if (!hostname) return false;
      const lower = hostname.toLowerCase();
      if (lower === 'localhost' || lower === '0.0.0.0' || lower === '::1') {
        return true;
      }
      if (/^\d{1,3}(\.\d{1,3}){3}$/.test(hostname)) {
        return true;
      }
      return hostname.includes(':');
    }

    // Keep reader links aligned with how the player was reached (hostname vs IP).
    function normalizeReaderUrl(baseUrl) {
      if (!baseUrl) return null;
      try {
        const parsed = new URL(baseUrl, window.location.href);
        const originHost = window.location.hostname;
        if (
          originHost
          && parsed.hostname
          && originHost !== parsed.hostname
          && isIpLikeHost(parsed.hostname)
        ) {
          parsed.hostname = originHost;
        }
        return parsed.toString();
      } catch (err) {
        return baseUrl;
      }
    }

    const readerUrl = normalizeReaderUrl(readerBaseUrl);

    const DEFAULT_VOICE = {
      speaker: 2,
      speed: 1,
      pitch: -0.08,
      intonation: 1.25,
    };
    const VOICE_SAMPLE_DEFAULTS = {
      speed: 1,
      pitch: 0,
      intonation: 1,
    };
    const VIEW_LIBRARY = 'library';
    const VIEW_SAMPLES = 'voice-samples';
    if (![VIEW_LIBRARY, VIEW_SAMPLES].includes(initialView)) {
      initialView = VIEW_LIBRARY;
    }

    const state = {
      books: [],
      collections: [],
      activeView: initialView,
      voiceRoster: [],
      voiceRosterLoaded: false,
      voiceRosterLoading: false,
      voiceSampleDefaults: { ...VOICE_SAMPLE_DEFAULTS },
      voiceSampleText: '',
      voiceSampleError: null,
      voiceSampleCache: {},
      voiceSampleCacheLoaded: false,
      voiceSampleCacheLoading: false,
      voiceSampleCacheCount: 0,
      chapters: [],
      currentBook: null,
      currentChapterIndex: -1,
      autoAdvance: false,
      media: null,
      voiceDefaults: { ...DEFAULT_VOICE },
      savedVoiceDefaults: {},
      bookmarks: {
        manual: [],
        lastPlayed: null,
        played: new Set(),
      },
      lastPlayedBook: null,
      localBuilds: new Set(),
      buildQueue: [],
      activeBuild: null,
      uploadJobs: [],
      librarySortOrder: 'author',
      libraryPrefix: '',
      parentPrefix: '',
      pendingEpubs: [],
      epubBusy: new Set(),
      readerUrl,
    };
    const libraryCache = new Map();
    const LIBRARY_CACHE_LIMIT = 10;
    let libraryRequestToken = 0;
    let deleteContext = null;
    let activeCardMenu = null;
    const LAST_PLAY_THROTTLE_MS = 1000;
    const LAST_PLAY_MIN_DELTA = 1;
    const LIBRARY_SORT_KEY = 'nkPlayerSortOrder';
    const LIBRARY_SORT_OPTIONS = ['author', 'recent', 'played'];
    const storedLibrarySort = window.localStorage.getItem(LIBRARY_SORT_KEY);
    if (LIBRARY_SORT_OPTIONS.includes(storedLibrarySort)) {
      state.librarySortOrder = storedLibrarySort;
    }
    const LOCATION_PREFIX_PARAM = 'folder';
    const LOCATION_BOOK_PARAM = 'book';
    // Keep RECENTLY_PLAYED_PREFIX in sync with the backend constant.
    const RECENTLY_PLAYED_PREFIX = '__nk_recently_played__';
    const RECENTLY_PLAYED_LABEL = 'Recently Played';

    function normalizeLibraryPath(value) {
      if (typeof value !== 'string') {
        return '';
      }
      const replaced = value.replace(/\\\\/g, '/').trim();
      if (!replaced) {
        return '';
      }
      return replaced.replace(/^\/+|\/+$/g, '');
    }

    function parentLibraryPath(value) {
      const normalized = normalizeLibraryPath(value);
      if (!normalized) {
        return '';
      }
      const idx = normalized.lastIndexOf('/');
      return idx === -1 ? '' : normalized.slice(0, idx);
    }

    function readLibraryLocation() {
      const params = new URLSearchParams(window.location.search);
      return {
        prefix: normalizeLibraryPath(params.get(LOCATION_PREFIX_PARAM)),
        bookId: normalizeLibraryPath(params.get(LOCATION_BOOK_PARAM)),
      };
    }

    function updateLocationFromState({ replace = false } = {}) {
      const params = new URLSearchParams(window.location.search);
      const previous = params.toString();
      const prefix = normalizeLibraryPath(state.libraryPrefix);
      const bookId = normalizeLibraryPath(state.currentBook && state.currentBook.id);
      if (prefix) {
        params.set(LOCATION_PREFIX_PARAM, prefix);
      } else {
        params.delete(LOCATION_PREFIX_PARAM);
      }
      if (bookId) {
        params.set(LOCATION_BOOK_PARAM, bookId);
      } else {
        params.delete(LOCATION_BOOK_PARAM);
      }
      const next = params.toString();
      if (next === previous && !replace) {
        return;
      }
      const hash = window.location.hash || '';
      const nextUrl = next ? `${window.location.pathname}?${next}${hash}` : `${window.location.pathname}${hash}`;
      if (replace) {
        window.history.replaceState(null, '', nextUrl);
      } else {
        window.history.pushState(null, '', nextUrl);
      }
    }

    const initialLocation = readLibraryLocation();
    const initialBookId = initialLocation.bookId;
    const initialLibraryPrefix = initialLocation.prefix || parentLibraryPath(initialBookId);

    async function applyLibraryLocationFromUrl() {
      const location = readLibraryLocation();
      const targetPrefix = location.prefix || parentLibraryPath(location.bookId);
      const normalizedPrefix = normalizeLibraryPath(targetPrefix);
      const currentPrefix = normalizeLibraryPath(state.libraryPrefix);
      if (normalizedPrefix !== currentPrefix) {
        try {
          await loadBooks(normalizedPrefix || '', { skipHistory: true });
        } catch (err) {
          console.error('Failed to sync library for navigation', err);
          return;
        }
      }
      if (location.bookId) {
        const opened = await openBookById(location.bookId, { skipHistory: true });
        if (!opened) {
          console.warn('History requested unknown book', location.bookId);
          closeBookView({ skipHistory: true });
        }
      } else {
        closeBookView({ skipHistory: true });
      }
    }
    let statusPollHandle = null;
    let lastPlaySyncAt = 0;
    let lastPlayPending = null;
    let lastPlayedRequestId = 0;
    let lastPlayedAppliedId = 0;
    let lastOpenedBookId = null;
    let noteModalContext = null;
    const uploadUI = {
      root: null,
      drop: null,
      input: null,
      error: null,
      jobsList: null,
    };
    let uploadPollTimer = null;
    let uploadDragDepth = 0;
    const UPLOAD_POLL_INTERVAL = 4000;
    if (booksSortSelect) {
      booksSortSelect.value = state.librarySortOrder;
      booksSortSelect.addEventListener('change', () => {
        const next = LIBRARY_SORT_OPTIONS.includes(booksSortSelect.value)
          ? booksSortSelect.value
          : 'author';
        if (state.librarySortOrder === next) {
          return;
        }
        state.librarySortOrder = next;
        try {
          window.localStorage.setItem(LIBRARY_SORT_KEY, next);
        } catch (error) {
          console.warn('Failed to persist sort order', error);
        }
        handlePromise(loadBooks());
      });
    }
    if (pendingEpubAllBtn) {
      pendingEpubAllBtn.addEventListener('click', () => {
        if (!state.pendingEpubs.length) return;
        const available = state.pendingEpubs
          .map(epub => epub.path || epub.filename)
          .filter(path => path && !state.epubBusy.has(path));
        if (available.length) {
          handlePromise(queueChapterizeEpubs(available));
        }
      });
    }
    if (libraryBackButton) {
      libraryBackButton.addEventListener('click', () => {
        if (!state.libraryPrefix) return;
        if (state.currentBook) {
          closeBookView({ skipHistory: true });
        }
        handlePromise(loadBooks(state.parentPrefix || ''));
      });
    }
    if (homeLink) {
      homeLink.addEventListener('click', event => {
        if (isVoiceSamplesView()) {
          return;
        }
        event.preventDefault();
        if (state.currentBook) {
          closeBookView({ skipHistory: true });
        }
        handlePromise(loadBooks('', { replaceHistory: true }));
      });
    }

    function formatTrackNumber(num) {
      if (typeof num !== 'number' || !Number.isFinite(num)) return '';
      return String(num).padStart(3, '0');
    }

    function formatTimecode(seconds) {
      if (!Number.isFinite(seconds)) return '0:00';
      const total = Math.max(0, Math.floor(seconds));
      const hours = Math.floor(total / 3600);
      const minutes = Math.floor((total % 3600) / 60);
      const secs = total % 60;
      const mm = hours ? String(minutes).padStart(2, '0') : String(minutes);
      const ss = String(secs).padStart(2, '0');
      return hours ? `${hours}:${mm}:${ss}` : `${mm}:${ss}`;
    }

    function formatTimeLeft(seconds) {
      if (!Number.isFinite(seconds)) return '--';
      const total = Math.max(0, Math.floor(seconds));
      const hours = Math.floor(total / 3600);
      const minutes = Math.floor((total % 3600) / 60);
      if (hours > 0) {
        return minutes > 0 ? `${hours}h ${minutes}m left` : `${hours}h left`;
      }
      if (minutes > 0) {
        return `${minutes}m left`;
      }
      return `${total}s left`;
    }

    function formatSpeedLabel(rate) {
      if (!Number.isFinite(rate) || rate <= 0) return '1x Speed';
      const normalized = Number(rate.toFixed(2));
      return `${normalized % 1 === 0 ? normalized.toFixed(0) : normalized}x Speed`;
    }

    function truncateText(text, maxLength) {
      if (typeof text !== 'string') return '';
      const normalized = text.trim();
      if (!normalized) return '';
      const limit = Number.isFinite(maxLength) && maxLength > 0 ? Math.floor(maxLength) : null;
      if (!limit || normalized.length <= limit) return normalized;
      const ellipsis = '...';
      const sliceLength = Math.max(1, limit - ellipsis.length);
      return `${normalized.slice(0, sliceLength).trimEnd()}${ellipsis}`;
    }

    function chapterTitleText(chapter) {
      if (!chapter) return '';
      const original = typeof chapter.original_title === 'string' ? chapter.original_title.trim() : '';
      if (original) return original;
      const transformed = typeof chapter.title === 'string' ? chapter.title.trim() : '';
      if (transformed) return transformed;
      if (typeof chapter.id === 'string') {
        return chapter.id.replace(/_/g, ' ');
      }
      return '';
    }

    function chapterDisplayTitle(chapter) {
      return truncateText(chapterTitleText(chapter), CHAPTER_TITLE_MAX_LENGTH);
    }

    function currentChapter() {
      if (state.currentChapterIndex < 0) return null;
      return state.chapters[state.currentChapterIndex] || null;
    }

    function chapterById(chapterId) {
      return state.chapters.find(ch => ch.id === chapterId) || null;
    }

    function ensureUploadCard() {
      if (uploadUI.root) {
        return uploadUI.root;
      }
      const card = document.createElement('article');
      card.className = 'card upload-card';
      card.innerHTML = `
        <div class="upload-drop" data-role="upload-drop" tabindex="0" role="button" aria-label="Upload EPUB">
          <strong>Upload EPUB</strong>
          <p>Drop an .epub here or click to select a file. nk will chapterize it and add it to your library.</p>
        </div>
        <input type="file" accept=".epub" data-role="upload-input" tabindex="-1" aria-hidden="true">
        <div class="upload-error" data-role="upload-error"></div>
        <div class="upload-jobs" data-role="upload-jobs">
          <div class="upload-empty">No uploads yet.</div>
        </div>
      `;
      uploadUI.root = card;
      uploadUI.drop = card.querySelector('[data-role="upload-drop"]');
      uploadUI.input = card.querySelector('[data-role="upload-input"]');
      uploadUI.error = card.querySelector('[data-role="upload-error"]');
      uploadUI.jobsList = card.querySelector('[data-role="upload-jobs"]');
      const triggerFileDialog = () => {
        uploadUI.input?.click();
      };

      if (uploadUI.drop) {
        uploadUI.drop.addEventListener('click', (event) => {
          event.preventDefault();
          triggerFileDialog();
        });
        uploadUI.drop.addEventListener('keydown', (event) => {
          if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            triggerFileDialog();
          }
        });
        uploadUI.drop.addEventListener('dragenter', (event) => {
          event.preventDefault();
          uploadDragDepth += 1;
          uploadUI.drop.classList.add('dragging');
        });
        uploadUI.drop.addEventListener('dragover', (event) => {
          event.preventDefault();
        });
        uploadUI.drop.addEventListener('dragleave', (event) => {
          event.preventDefault();
          uploadDragDepth = Math.max(0, uploadDragDepth - 1);
          if (uploadDragDepth === 0) {
            uploadUI.drop.classList.remove('dragging');
          }
        });
        uploadUI.drop.addEventListener('drop', (event) => {
          event.preventDefault();
          uploadDragDepth = 0;
          uploadUI.drop.classList.remove('dragging');
          const files = event.dataTransfer ? event.dataTransfer.files : null;
          handleUploadFiles(files);
        });
      }
      if (uploadUI.input) {
        uploadUI.input.addEventListener('change', () => {
          handleUploadFiles(uploadUI.input?.files || null);
        });
      }
      renderUploadJobs();
      return card;
    }

    function formatUploadStatus(status) {
      if (!status) return 'Pending';
      const text = String(status);
      return text.charAt(0).toUpperCase() + text.slice(1);
    }

    function setUploadError(message) {
      if (!uploadUI.error) return;
      uploadUI.error.textContent = message || '';
    }

    function renderUploadJobs() {
      if (!uploadUI.jobsList) return;
      uploadUI.jobsList.innerHTML = '';
      if (!state.uploadJobs.length) {
        const empty = document.createElement('div');
        empty.className = 'upload-empty';
        empty.textContent = 'No uploads yet.';
        uploadUI.jobsList.appendChild(empty);
        return;
      }
      state.uploadJobs.forEach((job) => {
        const item = document.createElement('div');
        item.className = 'upload-job';
        if (job.status) {
          item.dataset.status = job.status;
        }
        const header = document.createElement('div');
        header.className = 'upload-job-header';
        const title = document.createElement('div');
        title.className = 'upload-job-title';
        title.textContent = job.filename || 'Upload';
        const statusLabel = document.createElement('span');
        statusLabel.className = 'upload-job-status';
        statusLabel.textContent = formatUploadStatus(job.status);
        header.appendChild(title);
        header.appendChild(statusLabel);
        item.appendChild(header);

        const message = document.createElement('div');
        message.className = 'upload-job-message';
        const progressLabel = job.progress && job.progress.label ? job.progress.label : null;
        const errorMessage = job.error || null;
        message.textContent = errorMessage || job.message || progressLabel || 'Pending…';
        item.appendChild(message);

        if (job.book_dir || job.target_name) {
          const target = document.createElement('div');
          target.className = 'upload-target';
          target.textContent = `→ ${job.book_dir || job.target_name}`;
          item.appendChild(target);
        }

        const progress = job.progress;
        if (
          progress
          && typeof progress.index === 'number'
          && typeof progress.total === 'number'
          && progress.total > 0
        ) {
          const percent = Math.max(0, Math.min(100, (progress.index / progress.total) * 100));
          const wrap = document.createElement('div');
          wrap.className = 'upload-progress';
          const bar = document.createElement('div');
          bar.className = 'upload-progress-bar';
          bar.style.width = `${percent}%`;
          wrap.appendChild(bar);
          item.appendChild(wrap);
        }

        uploadUI.jobsList.appendChild(item);
      });
    }

    function applyUploadJobs(jobs) {
      if (!Array.isArray(jobs)) {
        if (!state.uploadJobs.length) {
          renderUploadJobs();
        }
        return;
      }
      const prevStatuses = new Map(state.uploadJobs.map(job => [job.id, job.status]));
      const normalized = jobs.filter(entry => entry && typeof entry === 'object').map(entry => entry);
      normalized.sort((a, b) => {
        const aTime = new Date(a.updated || a.created || 0).getTime();
        const bTime = new Date(b.updated || b.created || 0).getTime();
        return bTime - aTime;
      });
      state.uploadJobs = normalized;
      renderUploadJobs();
      if (normalized.length) {
        startUploadPolling();
      } else {
        stopUploadPolling();
      }
      const hasNewSuccess = normalized.some(
        job => job.status === 'success' && prevStatuses.get(job.id) !== 'success'
      );
      if (hasNewSuccess) {
        handlePromise(loadBooks(state.libraryPrefix || '', { skipHistory: true }));
      }
    }

    async function loadUploads() {
      try {
        const payload = await fetchJSON('/api/uploads');
        if (payload && Array.isArray(payload.jobs)) {
          applyUploadJobs(payload.jobs);
        } else if (!state.uploadJobs.length) {
          renderUploadJobs();
        }
      } catch (err) {
        console.warn('Failed to load uploads', err);
      }
    }

    function stopUploadPolling() {
      if (uploadPollTimer === null) return;
      window.clearInterval(uploadPollTimer);
      uploadPollTimer = null;
    }

    function startUploadPolling() {
      if (uploadPollTimer !== null) {
        return;
      }
      if (!state.uploadJobs.length) {
        return;
      }
      uploadPollTimer = window.setInterval(() => {
        loadUploads();
      }, UPLOAD_POLL_INTERVAL);
    }

    const INVALID_DIR_CHARS = new Set(['<', '>', ':', '"', '/', String.fromCharCode(92), '|', '?', '*']);

    function sanitizeDirFragment(name) {
      let candidate = typeof name === 'string' ? name.trim() : '';
      if (!candidate) {
        candidate = 'book';
      }
      const chars = [];
      for (const ch of candidate) {
        const code = ch.codePointAt(0);
        if (INVALID_DIR_CHARS.has(ch)) {
          chars.push('_');
        } else if (typeof code === 'number' && code < 32) {
          continue;
        } else {
          chars.push(ch);
        }
      }
      let sanitized = chars.join('').replace(/^[\s.]+|[\s.]+$/g, '');
      if (!sanitized) {
        sanitized = 'book';
      }
      if (sanitized.length > 120) {
        sanitized = sanitized.slice(0, 120);
      }
      return sanitized;
    }

    function deriveUploadTargetPath(file) {
      const fileName = typeof file?.name === 'string' ? file.name : '';
      const base = fileName.replace(/\\\\/g, '/').split('/').pop() || fileName;
      const stem = base.replace(/\.[^.]+$/, '') || base;
      const sanitized = sanitizeDirFragment(stem);
      const prefix = normalizeLibraryPath(state.libraryPrefix);
      return prefix ? `${prefix}/${sanitized}` : sanitized;
    }

    async function handleUploadFiles(fileList) {
      const files = [];
      if (!fileList) {
        // no-op
      } else if (typeof fileList.length === 'number') {
        for (let i = 0; i < fileList.length; i += 1) {
          const entry = fileList[i];
          if (entry) files.push(entry);
        }
      } else if (fileList && fileList.name) {
        files.push(fileList);
      }
      const file = files.find(candidate => candidate?.name?.toLowerCase().endsWith('.epub'));
      if (!file) {
        setUploadError('Please choose an .epub file.');
        return;
      }
      setUploadError('');
      const targetPath = deriveUploadTargetPath(file);
      if (targetPath) {
        const collision = Array.isArray(state.books)
          ? state.books.some(book => normalizeLibraryPath(book?.path || book?.id) === targetPath)
          : false;
        if (collision) {
          const proceed = await confirmReplaceUpload(targetPath, file.name);
          if (!proceed) {
            if (uploadUI.input) {
              uploadUI.input.value = '';
            }
            uploadDragDepth = 0;
            return;
          }
        }
      }
      if (uploadUI.drop) {
        uploadUI.drop.classList.remove('dragging');
        uploadUI.drop.classList.add('upload-busy');
      }
      const formData = new FormData();
      formData.append('file', file, file.name);
      if (state.libraryPrefix && typeof state.libraryPrefix === 'string') {
        formData.append('prefix', state.libraryPrefix);
      }
      fetch('/api/uploads', {
        method: 'POST',
        body: formData,
      })
        .then(async (res) => {
          let payload = null;
          try {
            payload = await res.json();
          } catch {
            payload = null;
          }
          if (!res.ok) {
            const detail = payload && payload.detail;
            throw new Error(detail || `Upload failed (${res.status})`);
          }
          return payload;
        })
        .then(() => {
          loadUploads();
          startUploadPolling();
        })
        .catch((err) => {
          console.error(err);
          setUploadError(err.message || 'Upload failed.');
        })
        .finally(() => {
          if (uploadUI.input) {
            uploadUI.input.value = '';
          }
          if (uploadUI.drop) {
            uploadUI.drop.classList.remove('upload-busy');
            uploadUI.drop.classList.remove('dragging');
          }
          uploadDragDepth = 0;
        });
    }

    function normalizeBookmarkEntry(entry) {
      if (!entry || typeof entry !== 'object') return null;
      const chapter = typeof entry.chapter === 'string' ? entry.chapter.trim() : '';
      if (!chapter) return null;
      const time = Number(entry.time);
      if (!Number.isFinite(time) || time < 0) return null;
      let id = null;
      if (typeof entry.id === 'string' && entry.id.trim()) {
        id = entry.id.trim();
      } else {
        id = `bm_${Math.random().toString(16).slice(2)}`;
      }
      const label = typeof entry.label === 'string' ? entry.label : null;
      return { id, chapter, time, label };
    }

    function setBookmarks(payload) {
      const manual = Array.isArray(payload?.manual)
        ? payload.manual.map(normalizeBookmarkEntry).filter(Boolean)
        : [];
      const last = payload?.last_played;
      const played = Array.isArray(payload?.played)
        ? payload.played
            .map(entry => (typeof entry === 'string' ? entry.trim() : ''))
            .filter(Boolean)
        : [];
      state.bookmarks = {
        manual,
        lastPlayed:
          last &&
          typeof last.chapter === 'string' &&
          Number.isFinite(Number(last.time))
            ? { chapter: last.chapter, time: Number(last.time) }
            : null,
        played: new Set(played),
      };
      updateBookmarkUI();
      updateChapterStatusUI();
    }

    function bookmarksForChapter(chapterId) {
      return (state.bookmarks.manual || [])
        .filter(entry => entry.chapter === chapterId)
        .sort((a, b) => (a.time || 0) - (b.time || 0));
    }

    function renderBookmarkList(container, entries, chapter) {
      container.innerHTML = '';
      if (!entries.length) {
        container.innerHTML =
          '<div class="bookmark-empty">No bookmarks for this chapter yet.</div>';
        return;
      }
      entries.forEach(entry => {
        const item = document.createElement('div');
        item.className = 'bookmark-item';
        const info = document.createElement('div');
        info.className = 'info';
        const label = document.createElement('div');
        label.className = 'label';
        label.textContent = entry.label && entry.label.trim().length
          ? entry.label.trim()
          : `@ ${formatTimecode(entry.time || 0)}`;
        const time = document.createElement('div');
        time.className = 'time';
        time.textContent = formatTimecode(entry.time || 0);
        info.appendChild(label);
        info.appendChild(time);
        item.appendChild(info);

        const actions = document.createElement('div');
        actions.className = 'actions';
        const playBtn = document.createElement('button');
        playBtn.dataset.role = 'primary-action';
        playBtn.dataset.bookmarkId = entry.id || '';
        playBtn.dataset.bookmarkTime = String(entry.time || 0);
        playBtn.dataset.bookmarkChapter = entry.chapter || '';
        playBtn.type = 'button';
        playBtn.textContent = 'Play';
        playBtn.onclick = () => {
          handlePromise(playBookmark(entry));
        };
        actions.appendChild(playBtn);
        const renameBtn = document.createElement('button');
        renameBtn.textContent = 'Notes';
        renameBtn.className = 'secondary';
        renameBtn.onclick = () => {
          const fallback = `@ ${formatTimecode(entry.time || 0)}`;
          const currentLabel = entry.label && entry.label.trim().length ? entry.label.trim() : fallback;
          openNoteEditor({
            value: entry.label || '',
            fallback,
            onSave: (nextValue) => renameBookmark(entry.id, nextValue),
          });
        };
        actions.appendChild(renameBtn);

        const deleteBtn = document.createElement('button');
        deleteBtn.textContent = 'Delete';
        deleteBtn.className = 'secondary';
        deleteBtn.onclick = () => {
          const fallback = `@ ${formatTimecode(entry.time || 0)}`;
          const labelText = entry.label && entry.label.trim().length
            ? entry.label.trim()
            : fallback;
          if (!window.confirm(`Delete bookmark "${labelText}"?`)) {
            return;
          }
          handlePromise(deleteBookmark(entry.id));
        };
        actions.appendChild(deleteBtn);
        item.appendChild(actions);
        container.appendChild(item);
      });
    }

    function updateBookmarkUI() {
      const current = currentChapter();
      // chapter-level bookmarks
      const containers = document.querySelectorAll('[data-role="chapter-bookmarks"]');
      containers.forEach(container => {
        const chapterId = container.dataset.chapterId;
        if (!chapterId) return;
        const chapter = state.chapters.find(ch => ch.id === chapterId);
        const entries = chapter ? bookmarksForChapter(chapter.id) : [];
        renderBookmarkList(container, entries, chapter);
      });

      const last = state.bookmarks.lastPlayed;
      const playBtn = playBookBtn;
      if (
        last &&
        typeof last.chapter === 'string' &&
        Number.isFinite(last.time)
      ) {
        const chapter = chapterById(last.chapter);
        const title = chapter ? (chapter.title || chapter.id) : last.chapter;
        if (lastPlayStatus) {
          lastPlayStatus.textContent = `Last played: ${title} @ ${formatTimecode(last.time)}`;
        }
        if (playBtn) {
          playBtn.disabled = false;
          playBtn.textContent = 'Resume last play';
        }
      } else {
        if (lastPlayStatus) {
          lastPlayStatus.textContent = 'No last play saved.';
        }
        if (playBtn) {
          playBtn.disabled = true;
          playBtn.textContent = 'Resume last play';
        }
      }
    }

    function ensureSeekAfterLoad(seconds, label) {
      if (!Number.isFinite(seconds) || seconds < 0) return;
      const applySeek = () => {
        try {
          player.currentTime = seconds;
        } catch {
          return;
        }
        if (label) {
          statusLine.textContent = label;
        }
      };
      if (player.readyState >= 1) {
        applySeek();
      } else {
        const handler = () => {
          player.removeEventListener('loadedmetadata', handler);
          applySeek();
        };
        player.addEventListener('loadedmetadata', handler);
      }
    }

    function enforceResumeTarget(seconds) {
      if (!Number.isFinite(seconds) || seconds <= 0) return;
      let attempts = 0;
      const maxAttempts = 3;
      const nudge = () => {
        const current = Number.isFinite(player.currentTime) ? player.currentTime : 0;
        if (Math.abs(current - seconds) <= RESUME_TARGET_TOLERANCE) {
          return;
        }
        attempts += 1;
        try {
          player.currentTime = seconds;
        } catch {
          // ignore seek errors
        }
        if (attempts < maxAttempts) {
          setTimeout(nudge, 200);
        }
      };
      setTimeout(nudge, 200);
    }

    async function refreshBookmarks() {
      if (!state.currentBook) return;
      try {
        const data = await fetchJSON(
          `/api/books/${encodeURIComponent(state.currentBook.id)}/bookmarks`
        );
        setBookmarks(data || {});
      } catch {
        // ignore refresh errors
      }
    }

    async function playBookmark(entry) {
      if (!entry || typeof entry.chapter !== 'string') return;
      const index = state.chapters.findIndex(ch => ch.id === entry.chapter);
      if (index === -1) {
        alert('Chapter not found for this bookmark.');
        return;
      }
      const resumeValue = Number(entry.time);
      await playChapter(index, { resumeTime: Number.isFinite(resumeValue) ? resumeValue : 0 });
    }

    async function deleteBookmark(bookmarkId) {
      if (!state.currentBook || !bookmarkId) return;
      const data = await fetchJSON(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/bookmarks/${encodeURIComponent(bookmarkId)}`,
        { method: 'DELETE' }
      );
      setBookmarks(data || {});
      statusLine.textContent = 'Bookmark deleted.';
    }

    async function createBookmarkForCurrent(time, label) {
      const chapter = currentChapter();
      if (!state.currentBook || !chapter) return;
      const payload = {
        chapter_id: chapter.id,
        time,
        label: label && label.length ? label : null,
      };
      const data = await fetchJSON(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/bookmarks`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        }
      );
      setBookmarks(data || {});
      statusLine.textContent = 'Bookmark saved.';
    }

    async function renameBookmark(bookmarkId, label) {
      if (!state.currentBook || !bookmarkId) return;
      const payload = { label: label && label.length ? label : null };
      const data = await fetchJSON(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/bookmarks/${encodeURIComponent(bookmarkId)}`,
        {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        }
      );
      setBookmarks(data || {});
      statusLine.textContent = 'Bookmark updated.';
    }

    async function persistLastPlayed(chapterId, time, playedChapterId = null) {
      if (!state.currentBook || !chapterId || !Number.isFinite(time)) return;
      const requestId = ++lastPlayedRequestId;
      const currentBookId = state.currentBook.id;
      const payload = { chapter_id: chapterId, time };
      if (playedChapterId) {
        payload.played_chapter_id = playedChapterId;
      }
      const data = await fetchJSON(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/bookmarks/last-played`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        }
      );
      if (!state.currentBook || state.currentBook.id !== currentBookId) return;
      if (requestId < lastPlayedAppliedId) return;
      lastPlayedAppliedId = requestId;
      setBookmarks(data || {});
    }

    async function clearLastPlayed() {
      if (!state.currentBook) return;
      const data = await fetchJSON(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/bookmarks/last-played`,
        { method: 'DELETE' }
      );
      setBookmarks(data || {});
    }

    function scheduleLastPlaySync(time) {
      const chapter = currentChapter();
      if (!chapter || !Number.isFinite(time) || time < 5) return;
      const last = state.bookmarks.lastPlayed;
      if (
        last &&
        last.chapter === chapter.id &&
        Math.abs(last.time - time) < LAST_PLAY_MIN_DELTA
      ) {
        return;
      }
      const now = Date.now();
      if (lastPlayPending || now - lastPlaySyncAt < LAST_PLAY_THROTTLE_MS) {
        return;
      }
      lastPlayPending = persistLastPlayed(chapter.id, time)
        .catch(() => {})
        .finally(() => {
          lastPlayPending = null;
          lastPlaySyncAt = Date.now();
        });
    }

    function markChapterPlayed(chapterId) {
      if (!chapterId) return;
      const played = state.bookmarks.played instanceof Set
        ? state.bookmarks.played
        : new Set();
      if (!played.has(chapterId)) {
        played.add(chapterId);
        state.bookmarks.played = played;
        updateChapterStatusUI();
      }
    }

    function recordCompletionProgress() {
      const chapter = currentChapter();
      if (!chapter || !state.currentBook) return;
      markChapterPlayed(chapter.id);
      let targetChapterId = chapter.id;
      let resumeTime = Number.isFinite(player.duration) ? player.duration : player.currentTime || 0;
      const nextIndex = state.currentChapterIndex + 1;
      const nextChapter = state.chapters[nextIndex];
      if (nextChapter && nextChapter.mp3_exists) {
        targetChapterId = nextChapter.id;
        resumeTime = 0;
      }
      if (!Number.isFinite(resumeTime) || resumeTime < 0) {
        resumeTime = 0;
      }
      handlePromise(persistLastPlayed(targetChapterId, resumeTime, chapter.id));
    }

    function updatePlayerDetails(chapter) {
      if (!chapter) {
        nowPlaying.textContent = 'Select a chapter to begin.';
        nowPlaying.title = 'Select a chapter to begin.';
        playerSubtitle.textContent = '';
        if (playerCover) {
          playerCover.classList.add('hidden');
          playerCover.removeAttribute('src');
        }
        resetProgressUI();
        return;
      }
      const trackLabel = formatTrackNumber(chapter.track_number);
      const chapterTitle = chapterDisplayTitle(chapter) || chapter.id;
      const fullChapterTitle = chapterTitleText(chapter) || chapter.id;
      const displayLabel = trackLabel ? `${trackLabel} ${chapterTitle}` : chapterTitle;
      const fullLabel = trackLabel ? `${trackLabel} ${fullChapterTitle}` : fullChapterTitle;
      nowPlaying.textContent = displayLabel;
      nowPlaying.title = fullLabel;
      const album =
        (state.media && state.media.album) ||
        (state.currentBook && state.currentBook.title) ||
        '';
      const artistCandidate =
        (state.media && state.media.artist) ||
        (state.currentBook && state.currentBook.author) ||
        album;
      const subtitleParts = [];
      if (album) subtitleParts.push(album);
      if (artistCandidate && (artistCandidate !== album || !album)) {
        subtitleParts.push(artistCandidate);
      }
      playerSubtitle.textContent = subtitleParts.join(' ・ ');
      if (playerCover && state.media && state.media.cover_url) {
        playerCover.src = state.media.cover_url;
        playerCover.classList.remove('hidden');
      } else if (playerCover) {
        playerCover.classList.add('hidden');
        playerCover.removeAttribute('src');
      }
    }

    updatePlayerDetails(null);

    function adjacentPlayableIndex(direction) {
      if (!state.chapters.length || state.currentChapterIndex < 0) return -1;
      const step = direction < 0 ? -1 : 1;
      for (
        let idx = state.currentChapterIndex + step;
        idx >= 0 && idx < state.chapters.length;
        idx += step
      ) {
        const candidate = state.chapters[idx];
        if (candidate && candidate.mp3_exists) {
          return idx;
        }
      }
      return -1;
    }

    function updateTransportAvailability() {
      const current =
        state.currentChapterIndex >= 0 ? state.chapters[state.currentChapterIndex] : null;
      const hasChapter = Boolean(current && current.mp3_exists);
      if (bookmarkAddBtn) {
        bookmarkAddBtn.disabled = !hasChapter;
      }
      if (playerPlayToggle) {
        playerPlayToggle.disabled = !hasChapter;
      }
      if (playerRewindBtn) {
        playerRewindBtn.disabled = !hasChapter;
      }
      if (playerForwardBtn) {
        playerForwardBtn.disabled = !hasChapter;
      }
      if (playerPrevBtn) {
        playerPrevBtn.disabled = !hasChapter;
      }
      if (playerNextBtn) {
        playerNextBtn.disabled = adjacentPlayableIndex(1) === -1;
      }
    }

    updateTransportAvailability();
    updatePlayToggleState();

    function setPlaybackRate(rate) {
      if (!player) return;
      const normalized = Number.isFinite(rate) && rate > 0 ? rate : 1;
      const idx = PLAYBACK_RATES.findIndex(value => Math.abs(value - normalized) < 0.001);
      if (idx !== -1) {
        playbackRateIndex = idx;
      }
      player.playbackRate = normalized;
      if (playerSpeedBtn) {
        playerSpeedBtn.textContent = formatSpeedLabel(normalized);
      }
      try {
        localStorage.setItem(SPEED_STORAGE_KEY, String(normalized));
      } catch {
        // ignore persistence failures
      }
    }

    function cyclePlaybackRate() {
      playbackRateIndex = (playbackRateIndex + 1) % PLAYBACK_RATES.length;
      setPlaybackRate(PLAYBACK_RATES[playbackRateIndex]);
    }

    (function initializePlaybackRate() {
      let stored = 1;
      try {
        const fromStorage = localStorage.getItem(SPEED_STORAGE_KEY);
        if (fromStorage) {
          stored = parseFloat(fromStorage);
        }
      } catch {
        stored = 1;
      }
      if (!Number.isFinite(stored) || stored <= 0) {
        stored = 1;
      }
      setPlaybackRate(stored);
    })();

    function nudgePlayback(seconds) {
      if (!player || !Number.isFinite(player.currentTime)) return;
      const duration = Number.isFinite(player.duration) ? player.duration : null;
      const target = duration
        ? Math.max(0, Math.min(duration, player.currentTime + seconds))
        : Math.max(0, player.currentTime + seconds);
      try {
        player.currentTime = target;
      } catch {
        return;
      }
      updateProgressUI();
    }

    function restartCurrentChapter() {
      const chapter = currentChapter();
      if (!player || !chapter || !chapter.mp3_exists) return false;
      try {
        player.currentTime = 0;
      } catch {
        return false;
      }
      updateProgressUI();
      statusLine.textContent = 'Restarted chapter.';
      return true;
    }

    function updateMediaSession(chapter) {
      if (!('mediaSession' in navigator) || !chapter || !state.currentBook) return;
      const trackLabel = formatTrackNumber(chapter.track_number || chapter.index);
      const titleLabel = chapterDisplayTitle(chapter) || chapter.title || chapter.id;
      const chapterLabel = trackLabel ? `${trackLabel} ${titleLabel}` : titleLabel;
      const album =
        (state.media && state.media.album) ||
        state.currentBook.title ||
        (state.currentBook && state.currentBook.id) ||
        'nk';
      const artist =
        (state.media && state.media.artist) ||
        state.currentBook.author ||
        album;
      navigator.mediaSession.metadata = new MediaMetadata({
        title: chapterLabel,
        artist,
        album,
        artwork:
          state.media && state.media.cover_url
            ? [{ src: state.media.cover_url }]
            : [],
      });
    }

    function resetProgressUI() {
      if (playerSeek) {
        playerSeek.disabled = true;
        playerSeek.value = '0';
        playerSeek.style.setProperty('--progress', '0%');
      }
      if (playerCurrentTime) {
        playerCurrentTime.textContent = '0:00';
      }
      if (playerDurationLabel) {
        playerDurationLabel.textContent = '0:00';
      }
      if (playerTimeLeft) {
        playerTimeLeft.textContent = '--';
      }
    }

    function updateProgressUI() {
      if (!player) return;
      const duration = Number.isFinite(player.duration) ? player.duration : 0;
      const current = Number.isFinite(player.currentTime) ? player.currentTime : 0;
      if (playerSeek) {
        if (duration > 0) {
          playerSeek.disabled = false;
          if (!isScrubbing) {
            const nextValue = Math.round((current / duration) * PLAYER_SEEK_MAX);
            playerSeek.value = String(Math.max(0, Math.min(PLAYER_SEEK_MAX, nextValue)));
          }
          const percent = Math.max(0, Math.min(100, (current / duration) * 100));
          playerSeek.style.setProperty('--progress', `${percent}%`);
        } else {
          playerSeek.disabled = true;
          if (!isScrubbing) {
            playerSeek.value = '0';
          }
          playerSeek.style.setProperty('--progress', '0%');
        }
      }
      if (!isScrubbing && playerCurrentTime) {
        playerCurrentTime.textContent = formatTimecode(current);
      }
      if (playerDurationLabel) {
        playerDurationLabel.textContent = duration > 0 ? formatTimecode(duration) : '0:00';
      }
      if (!isScrubbing && playerTimeLeft) {
        playerTimeLeft.textContent = duration > 0 ? formatTimeLeft(duration - current) : '--';
      }
    }

    function previewProgressFromValue(value) {
      if (!player) return;
      const duration = Number.isFinite(player.duration) ? player.duration : 0;
      if (!duration) return;
      const ratio = Math.min(1, Math.max(0, value / PLAYER_SEEK_MAX));
      const preview = duration * ratio;
      if (playerSeek) {
        playerSeek.style.setProperty('--progress', `${ratio * 100}%`);
      }
      if (playerCurrentTime) {
        playerCurrentTime.textContent = formatTimecode(preview);
      }
      if (playerTimeLeft) {
        playerTimeLeft.textContent = formatTimeLeft(duration - preview);
      }
    }

    function commitSeekFromValue(value) {
      if (!player || !Number.isFinite(player.duration) || playerSeek?.disabled) {
        isScrubbing = false;
        updateProgressUI();
        return;
      }
      const ratio = Math.min(1, Math.max(0, value / PLAYER_SEEK_MAX));
      try {
        player.currentTime = player.duration * ratio;
      } catch {
        // ignore assignment errors
      }
      isScrubbing = false;
      updateProgressUI();
    }

    function updatePlayToggleState() {
      if (!playerPlayToggle || !player) return;
      const isPlaying = !player.paused && !player.ended;
      playerPlayToggle.classList.toggle('playing', isPlaying);
      const label = isPlaying ? 'Pause' : 'Play';
      playerPlayToggle.setAttribute('aria-label', label);
      const srOnly = playerPlayToggle.querySelector('.sr-only');
      if (srOnly) {
        srOnly.textContent = label;
      }
    }

    function handlePromise(promise) {
      promise.catch(err => {
        if (err && err.name === 'AbortError') return;
        const msg = err?.message || String(err);
        statusLine.textContent = `Error: ${msg}`;
      });
    }

    function closeCardMenu() {
      if (!activeCardMenu) return;
      activeCardMenu.menu.classList.add('hidden');
      activeCardMenu.button.setAttribute('aria-expanded', 'false');
      activeCardMenu = null;
    }

    function toggleCardMenu(button, menu) {
      if (!button || !menu) return;
      if (activeCardMenu && activeCardMenu.menu === menu) {
        closeCardMenu();
        return;
      }
      closeCardMenu();
      menu.classList.remove('hidden');
      button.setAttribute('aria-expanded', 'true');
      activeCardMenu = { button, menu };
    }

    function isNoteModalActive() {
      return noteModal && !noteModal.classList.contains('hidden');
    }

    function isDeleteModalActive() {
      return deleteModal && !deleteModal.classList.contains('hidden');
    }

    function isReplaceModalActive() {
      return replaceModal && !replaceModal.classList.contains('hidden');
    }

    function setNoteModalDisabled(disabled) {
      if (noteSaveBtn) noteSaveBtn.disabled = disabled;
      if (noteCancelBtn) noteCancelBtn.disabled = disabled;
    }

    function setDeleteModalBusy(busy) {
      if (deleteConfirmBtn) {
        deleteConfirmBtn.disabled = busy;
        deleteConfirmBtn.textContent = busy ? 'Deleting…' : 'Delete';
      }
      if (deleteCancelBtn) {
        deleteCancelBtn.disabled = busy;
      }
    }

    function closeNoteEditor() {
      if (!noteModal) return;
      noteModal.classList.add('hidden');
      noteModal.setAttribute('aria-hidden', 'true');
      document.body.classList.remove('modal-open');
      setNoteModalDisabled(false);
      noteModalContext = null;
      if (noteTextarea) {
        noteTextarea.value = '';
      }
    }

    function closeDeleteModal() {
      if (!deleteModal) return;
      deleteModal.classList.add('hidden');
      deleteModal.setAttribute('aria-hidden', 'true');
      document.body.classList.remove('modal-open');
      setDeleteModalBusy(false);
      deleteContext = null;
    }

    let replaceResolver = null;

    function setReplaceModalBusy(busy) {
      if (replaceConfirmBtn) {
        replaceConfirmBtn.disabled = busy;
        replaceConfirmBtn.textContent = busy ? 'Replacing…' : 'Replace';
      }
      if (replaceCancelBtn) {
        replaceCancelBtn.disabled = busy;
      }
    }

    function hideReplaceModal() {
      if (!replaceModal) return;
      replaceModal.classList.add('hidden');
      replaceModal.setAttribute('aria-hidden', 'true');
      document.body.classList.remove('modal-open');
      setReplaceModalBusy(false);
    }

    function resolveReplaceModal(result) {
      const resolver = replaceResolver;
      replaceResolver = null;
      hideReplaceModal();
      if (resolver) resolver(result);
    }

    function confirmReplaceUpload(targetPath, filename) {
      const message = `A book already exists at "${targetPath}". Uploading "${filename}" will replace its chapters, metadata, and cover. Existing audio files stay as-is; rebuild if you need fresh audio. Continue?`;
      if (!replaceModal || !replaceConfirmBtn || !replaceCancelBtn) {
        return Promise.resolve(window.confirm(message));
      }
      if (replaceResolver) {
        resolveReplaceModal(false);
      }
      if (replaceModalMessage) {
        replaceModalMessage.textContent = message;
      }
      document.body.classList.add('modal-open');
      replaceModal.classList.remove('hidden');
      replaceModal.setAttribute('aria-hidden', 'false');
      setReplaceModalBusy(false);
      return new Promise(resolve => {
        replaceResolver = resolve;
        if (replaceConfirmBtn) {
          replaceConfirmBtn.focus();
        }
      });
    }

    function openDeleteModal(book) {
      if (!deleteModal) return;
      const bookId = book?.id || book?.path;
      if (!bookId) return;
      deleteContext = {
        id: bookId,
        title: book?.title || bookId,
        path: book?.path || '',
      };
      const name = deleteContext.title || deleteContext.id;
      const relPath = deleteContext.path && deleteContext.path !== deleteContext.title
        ? deleteContext.path
        : '';
      if (deleteModalMessage) {
        const suffix = relPath && relPath !== name ? ` (${relPath})` : '';
        deleteModalMessage.textContent = `This will permanently delete "${name}"${suffix}. All chapters, audio, and bookmarks for this book will be removed.`;
      }
      document.body.classList.add('modal-open');
      deleteModal.classList.remove('hidden');
      deleteModal.setAttribute('aria-hidden', 'false');
      setDeleteModalBusy(false);
      if (deleteConfirmBtn) {
        deleteConfirmBtn.focus();
      }
    }

    async function confirmDeleteBook() {
      if (!deleteContext) return;
      setDeleteModalBusy(true);
      const target = { ...deleteContext };
      try {
        const response = await fetch(`/api/books/${encodeURIComponent(target.id)}`, {
          method: 'DELETE',
        });
        if (!response.ok) {
          const detail = await readErrorResponse(response);
          throw new Error(detail);
        }
        closeDeleteModal();
        if (state.currentBook && state.currentBook.id === target.id) {
          closeBookView({ replaceHistory: true });
        }
        statusLine.textContent = `Deleted ${target.title || target.id}.`;
        await loadBooks(state.libraryPrefix || '', { skipHistory: true, replaceHistory: true });
      } catch (error) {
        setDeleteModalBusy(false);
        const message = error?.message || String(error);
        alert(`Failed to delete book: ${message}`);
      }
    }

    function submitNoteEditor() {
      if (!noteModalContext || typeof noteModalContext.onSave !== 'function' || !noteTextarea) {
        closeNoteEditor();
        return;
      }
      const trimmed = noteTextarea.value.trim();
      const result = noteModalContext.onSave(trimmed);
      if (!result || typeof result.then !== 'function') {
        closeNoteEditor();
        return;
      }
      setNoteModalDisabled(true);
      handlePromise(
        result
          .then(() => {
            closeNoteEditor();
          })
          .catch((error) => {
            setNoteModalDisabled(false);
            throw error;
          })
      );
    }

    function openNoteEditor({ value = '', fallback = '', onSave } = {}) {
      if (!noteModal || !noteTextarea || typeof onSave !== 'function') {
        const initial = value || fallback || '';
        const next = window.prompt('Bookmark notes (leave empty to clear).', initial);
        if (next === null) {
          return;
        }
        handlePromise(onSave(next.trim()));
        return;
      }
      noteModalContext = { onSave };
      noteTextarea.value = value || '';
      noteTextarea.placeholder = fallback ? `Defaults to ${fallback}` : '';
      if (noteModalMeta) {
        noteModalMeta.textContent = fallback ? `Fallback when empty: ${fallback}` : '';
        noteModalMeta.hidden = !fallback;
      }
      noteModal.classList.remove('hidden');
      noteModal.setAttribute('aria-hidden', 'false');
      document.body.classList.add('modal-open');
      setNoteModalDisabled(false);
      requestAnimationFrame(() => {
        noteTextarea.focus();
        noteTextarea.setSelectionRange(noteTextarea.value.length, noteTextarea.value.length);
      });
    }

    if (noteCancelBtn) {
      noteCancelBtn.addEventListener('click', (event) => {
        event.preventDefault();
        closeNoteEditor();
      });
    }
    if (noteSaveBtn) {
      noteSaveBtn.addEventListener('click', (event) => {
        event.preventDefault();
        submitNoteEditor();
      });
    }
    if (noteModal) {
      noteModal.addEventListener('click', (event) => {
        if (event.target === noteModal) {
          closeNoteEditor();
        }
      });
    }
    if (deleteModal) {
      deleteModal.addEventListener('click', (event) => {
        if (event.target === deleteModal && (!deleteConfirmBtn || !deleteConfirmBtn.disabled)) {
          closeDeleteModal();
        }
      });
    }
    if (replaceModal) {
      replaceModal.addEventListener('click', (event) => {
        if (event.target === replaceModal && (!replaceConfirmBtn || !replaceConfirmBtn.disabled)) {
          resolveReplaceModal(false);
        }
      });
    }
    if (noteTextarea) {
      noteTextarea.addEventListener('keydown', (event) => {
        if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
          event.preventDefault();
          submitNoteEditor();
        }
      });
    }
    if (deleteCancelBtn) {
      deleteCancelBtn.addEventListener('click', (event) => {
        event.preventDefault();
        if (deleteConfirmBtn && deleteConfirmBtn.disabled) return;
        closeDeleteModal();
      });
    }
    if (deleteConfirmBtn) {
      deleteConfirmBtn.addEventListener('click', (event) => {
        event.preventDefault();
        confirmDeleteBook();
      });
    }
    if (replaceCancelBtn) {
      replaceCancelBtn.addEventListener('click', (event) => {
        event.preventDefault();
        if (replaceConfirmBtn && replaceConfirmBtn.disabled) return;
        resolveReplaceModal(false);
      });
    }
    if (replaceConfirmBtn) {
      replaceConfirmBtn.addEventListener('click', (event) => {
        event.preventDefault();
        if (replaceConfirmBtn.disabled) return;
        setReplaceModalBusy(true);
        resolveReplaceModal(true);
      });
    }
    document.addEventListener('keydown', (event) => {
      if (event.key !== 'Escape') {
        return;
      }
      if (isNoteModalActive()) {
        event.preventDefault();
        closeNoteEditor();
        return;
      }
      if (isDeleteModalActive()) {
        event.preventDefault();
        if (!deleteConfirmBtn || !deleteConfirmBtn.disabled) {
          closeDeleteModal();
        }
        return;
      }
      if (isReplaceModalActive()) {
        event.preventDefault();
        if (!replaceConfirmBtn || !replaceConfirmBtn.disabled) {
          resolveReplaceModal(false);
        }
        return;
      }
      if (activeCardMenu) {
        event.preventDefault();
        closeCardMenu();
      }
    });
    document.addEventListener('click', (event) => {
      if (!activeCardMenu) return;
      const path = typeof event.composedPath === 'function' ? event.composedPath() : null;
      if (path) {
        if (path.includes(activeCardMenu.menu) || path.includes(activeCardMenu.button)) {
          return;
        }
      } else {
        const target = event.target;
        if (
          (target && activeCardMenu.menu.contains(target)) ||
          (target && activeCardMenu.button.contains(target))
        ) {
          return;
        }
      }
      closeCardMenu();
    });

    async function fetchJSON(url, options = {}) {
      const init = { cache: 'no-store', ...options };
      if (options.headers) {
        init.headers = { ...options.headers };
      }
      const res = await fetch(url, init);
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      return res.json();
    }

    async function readErrorResponse(response) {
      const text = await response.text();
      if (!text) return `HTTP ${response.status}`;
      try {
        const payload = JSON.parse(text);
        if (payload && typeof payload.detail === 'string') {
          return payload.detail;
        }
      } catch {
        // ignore parse errors
      }
      return text;
    }

    function badge(label, className = '') {
      const span = document.createElement('span');
      span.className = className ? `badge ${className}` : 'badge';
      span.textContent = label;
      return span;
    }

    function applyVoiceDefaults(effective, saved) {
      state.voiceDefaults = { ...DEFAULT_VOICE, ...(effective || {}) };
      state.savedVoiceDefaults = saved || {};
      updateVoiceForm();
    }

    function updateVoiceForm() {
      const values = state.voiceDefaults || DEFAULT_VOICE;
      voiceSpeakerInput.value = values.speaker ?? '';
      voiceSpeedInput.value = values.speed ?? '';
      voicePitchInput.value = values.pitch ?? '';
      voiceIntonationInput.value = values.intonation ?? '';
      setVoiceStatus('', false);
    }

    function setVoiceStatus(message, isError = false) {
      if (!voiceStatus) return;
      voiceStatus.textContent = message;
      voiceStatus.style.color = isError ? '#f87171' : 'var(--muted)';
    }

    function parseVoiceValue(input, { integer = false, allowEmpty = false } = {}) {
      const raw = input.value.trim();
      if (raw === '') {
        if (allowEmpty) return null;
        throw new Error('All voice fields must be set.');
      }
      const num = Number(raw);
      if (!Number.isFinite(num)) {
        throw new Error('Voice fields must be numeric.');
      }
      if (integer) {
        if (!Number.isInteger(num)) {
          throw new Error('Speaker must be an integer.');
        }
        if (num <= 0) {
          throw new Error('Speaker must be positive.');
        }
        return Math.round(num);
      }
      return num;
    }

    function gatherVoicePayload() {
      const payload = {};
      const speaker = voiceSpeakerInput.value.trim();
      const speed = voiceSpeedInput.value.trim();
      const pitch = voicePitchInput.value.trim();
      const intonation = voiceIntonationInput.value.trim();
      payload.speaker = speaker === '' ? null : parseVoiceValue(voiceSpeakerInput, { integer: true, allowEmpty: true });
      payload.speed = speed === '' ? null : parseVoiceValue(voiceSpeedInput, { allowEmpty: true });
      payload.pitch = pitch === '' ? null : parseVoiceValue(voicePitchInput, { allowEmpty: true });
      payload.intonation = intonation === '' ? null : parseVoiceValue(voiceIntonationInput, { allowEmpty: true });
      return payload;
    }

    async function persistVoiceDefaults(payload) {
      if (!state.currentBook) throw new Error('Select a book first.');
      setVoiceStatus('Saving...');
      const res = await fetch(`/api/books/${encodeURIComponent(state.currentBook.id)}/tts-defaults`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      const data = await res.json();
      applyVoiceDefaults(data.effective || DEFAULT_VOICE, data.saved || {});
      setVoiceStatus('Saved.');
    }

    applyVoiceDefaults(DEFAULT_VOICE, {});

    function scrollToLastBook() {
      const gridNodes = booksGrid ? Array.from(booksGrid.querySelectorAll('[data-book-id]')) : [];
      const collectionNodes = collectionsGrid
        ? Array.from(collectionsGrid.querySelectorAll('[data-book-id]'))
        : [];
      const nodes = [...gridNodes, ...collectionNodes];
      if (!nodes.length) return;
      const isRecentView = state.libraryPrefix === RECENTLY_PLAYED_PREFIX;
      let target = null;
      if (lastOpenedBookId) {
        target = nodes.find(node => node.dataset.bookId === lastOpenedBookId) || null;
      }
      if (isRecentView && gridNodes.length > 1 && (!target || target === gridNodes[0])) {
        target = gridNodes[1];
      }
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }

    function renderLibraryNav() {
      if (libraryBreadcrumb) {
        libraryBreadcrumb.innerHTML = '';
        const crumbs = [{ label: 'Library', path: '' }];
        const prefixValue = typeof state.libraryPrefix === 'string' ? state.libraryPrefix : '';
        const segments = prefixValue ? prefixValue.split('/').filter(Boolean) : [];
        let running = '';
        segments.forEach(segment => {
          running = running ? `${running}/${segment}` : segment;
          crumbs.push({ label: segment, path: running });
        });
        crumbs.forEach((crumb, index) => {
          const isLast = index === crumbs.length - 1;
          const node = document.createElement(isLast ? 'span' : 'button');
          node.className = isLast ? 'breadcrumb current' : 'breadcrumb';
          const displayLabel =
            crumb.path === RECENTLY_PLAYED_PREFIX ? RECENTLY_PLAYED_LABEL : (crumb.label || 'Library');
          node.textContent = displayLabel;
          if (!isLast) {
            node.type = 'button';
            node.addEventListener('click', () => {
              if (state.currentBook) {
                closeBookView({ skipHistory: true });
              }
              handlePromise(loadBooks(crumb.path));
            });
          }
          libraryBreadcrumb.appendChild(node);
          if (index < crumbs.length - 1) {
            const divider = document.createElement('span');
            divider.className = 'breadcrumb-divider';
            divider.textContent = '/';
            libraryBreadcrumb.appendChild(divider);
          }
        });
      }
      if (libraryBackButton) {
        const hasParent = Boolean(state.libraryPrefix);
        libraryBackButton.classList.toggle('hidden', !hasParent);
        libraryBackButton.disabled = !hasParent;
      }
    }

    function createBookCard(book, options = {}) {
      if (!book || typeof book !== 'object') {
        return null;
      }
      const { featuredLabel = null } = options;
      const card = document.createElement('article');
      card.className = 'card';
      const bookId = typeof book.path === 'string' && book.path ? book.path : book.id;
      if (bookId) {
        card.dataset.bookId = bookId;
      }

      const menuButton = document.createElement('button');
      menuButton.type = 'button';
      menuButton.className = 'card-menu-button';
      const bookLabel = book.title || book.path || 'book';
      menuButton.setAttribute('aria-label', `Options for ${bookLabel}`);
      menuButton.setAttribute('aria-haspopup', 'true');
      menuButton.setAttribute('aria-expanded', 'false');
      menuButton.innerText = '⋯';
      const menu = document.createElement('div');
      menu.className = 'card-menu hidden';
      menuButton.addEventListener('click', (event) => {
        event.stopPropagation();
        toggleCardMenu(menuButton, menu);
      });
      const addMenuItem = (label, className, handler) => {
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.className = className || '';
        button.addEventListener('click', (event) => {
          event.preventDefault();
          event.stopPropagation();
          closeCardMenu();
          handler();
        });
        menu.appendChild(button);
      };
      if (book.epub_path) {
        addMenuItem('Reprocess EPUB', '', () => {
          const prompt = `Reprocess "${book.title || bookLabel}" from its EPUB? This will replace chapter text, metadata, and cover. Existing audio stays until rebuilt.`;
          if (!window.confirm(prompt)) return;
          fetch(`/api/books/${encodeURIComponent(bookId)}/reprocess`, { method: 'POST' })
            .then(async res => {
              if (!res.ok) {
                const detail = await readErrorResponse(res);
                throw new Error(detail);
              }
              return res.json();
            })
            .then((payload) => {
              if (payload && payload.job) {
                const dedupedJobs = state.uploadJobs.filter(job => job && job.id !== payload.job.id);
                applyUploadJobs([payload.job, ...dedupedJobs]);
              }
              loadUploads();
              handlePromise(loadBooks(state.libraryPrefix || '', { skipHistory: true }));
            })
            .catch(err => {
              alert(`Failed to reprocess EPUB: ${err.message || err}`);
            });
        });
      }
      const deleteAction = document.createElement('button');
      deleteAction.type = 'button';
      deleteAction.className = 'danger';
      deleteAction.textContent = 'Delete';
      deleteAction.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        closeCardMenu();
        openDeleteModal(book);
      });
      menu.appendChild(deleteAction);
      card.appendChild(menuButton);
      card.appendChild(menu);

      if (book.cover_url) {
        const coverWrapper = document.createElement('div');
        coverWrapper.className = 'cover-wrapper';
        const cover = document.createElement('img');
        cover.className = 'cover';
        cover.src = book.cover_url;
        cover.alt = `${book.title} cover`;
        coverWrapper.appendChild(cover);
        const coverButton = document.createElement('button');
        coverButton.className = 'cover-link';
        coverButton.setAttribute('aria-label', `Open ${book.title}`);
        coverWrapper.appendChild(coverButton);
        coverWrapper.addEventListener('click', () => {
          handlePromise(openBook(book));
        });
        card.appendChild(coverWrapper);
      }

      const title = document.createElement('div');
      title.className = 'title';
      title.textContent = book.title;
      card.appendChild(title);

      if (book.author) {
        const author = document.createElement('div');
        author.className = 'author';
        author.textContent = book.author;
        card.appendChild(author);
      }

      const badgesWrap = document.createElement('div');
      badgesWrap.className = 'badges';
      if (featuredLabel) {
        badgesWrap.appendChild(badge(featuredLabel, 'muted'));
      }
      badgesWrap.appendChild(
        badge(
          `${book.completed_chapters}/${book.total_chapters} ready`,
          book.completed_chapters === book.total_chapters && book.total_chapters > 0 ? 'success' : ''
        )
      );
      if (book.pending_chapters > 0) {
        badgesWrap.appendChild(badge(`${book.pending_chapters} pending`, 'warning'));
      }
      if (book.total_chapters === 0) {
        badgesWrap.appendChild(badge('Empty', 'muted'));
      }
      card.appendChild(badgesWrap);

      if (!book.cover_url) {
        card.addEventListener('click', () => {
          handlePromise(openBook(book));
        });
      }
      return card;
    }

    function setVoiceSamplesStatus(message, isError = false) {
      if (!voiceSamplesStatus) return;
      voiceSamplesStatus.textContent = message || '';
      voiceSamplesStatus.style.color = isError ? '#f87171' : 'var(--muted)';
    }

    function isVoiceSamplesView() {
      return state.activeView === VIEW_SAMPLES;
    }

    function updateVoiceSamplesControls() {
      if (!voiceSamplesRefreshBtn) return;
      if (state.voiceRosterLoading) {
        voiceSamplesRefreshBtn.disabled = true;
        voiceSamplesRefreshBtn.textContent = 'Loading...';
        return;
      }
      voiceSamplesRefreshBtn.disabled = false;
      voiceSamplesRefreshBtn.textContent = state.voiceRosterLoaded
        ? 'Refresh voices'
        : 'Load voices';
    }

    function clearVoiceSamplesGrid() {
      if (!voiceSamplesGrid) return;
      const audioNodes = Array.from(
        voiceSamplesGrid.querySelectorAll('audio[data-blob-url]')
      );
      audioNodes.forEach(node => {
        const url = node.dataset.blobUrl;
        if (url) {
          URL.revokeObjectURL(url);
        }
      });
      voiceSamplesGrid.innerHTML = '';
    }

    function updateVoiceSamplesVisibility() {
      if (!voiceSamplesPage) return;
      const isSamplesView = isVoiceSamplesView();
      voiceSamplesPage.classList.toggle('hidden', !isSamplesView);
      if (!isSamplesView) {
        clearVoiceSamplesGrid();
        setVoiceSamplesStatus('');
      }
    }

    function updateVoiceSamplesLink() {
      if (!voiceSamplesLink) return;
      const isSamplesView = isVoiceSamplesView();
      voiceSamplesLink.textContent = isSamplesView ? 'Back to library' : 'Voice samples';
      voiceSamplesLink.setAttribute(
        'aria-label',
        isSamplesView ? 'Back to library' : 'Open voice samples'
      );
    }

    function applyViewMode() {
      const isSamplesView = isVoiceSamplesView();
      if (booksPanel) {
        booksPanel.classList.toggle('hidden', isSamplesView);
      }
      if (chaptersPanel && isSamplesView) {
        chaptersPanel.classList.add('hidden');
      }
      if (playerDock && isSamplesView) {
        playerDock.classList.add('hidden');
      }
      updateVoiceSamplesVisibility();
      updateVoiceSamplesLink();
    }

    async function loadVoiceRoster({ force = false } = {}) {
      if (state.voiceRosterLoading) return;
      if (state.voiceRosterLoaded && !force) {
        renderVoiceSamples();
        return;
      }
      state.voiceRosterLoading = true;
      state.voiceSampleError = null;
      updateVoiceSamplesControls();
      setVoiceSamplesStatus('Loading voices...');
      try {
        const query = force ? '?refresh=1' : '';
        const data = await fetchJSON(`/api/voice-samples/voices${query}`);
        const roster = Array.isArray(data?.characters) ? data.characters : [];
        state.voiceRoster = roster;
        state.voiceRosterLoaded = true;
        const defaults = data?.defaults && typeof data.defaults === 'object'
          ? data.defaults
          : {};
        state.voiceSampleDefaults = { ...VOICE_SAMPLE_DEFAULTS, ...defaults };
        if (typeof data?.sample_text === 'string') {
          state.voiceSampleText = data.sample_text;
        }
        if (!roster.length) {
          setVoiceSamplesStatus('No voices found.');
        } else if (data?.cached) {
          setVoiceSamplesStatus(`${roster.length} characters loaded (cached).`);
        } else {
          setVoiceSamplesStatus(`${roster.length} characters loaded.`);
        }
      } catch (err) {
        state.voiceSampleError = err;
        setVoiceSamplesStatus('Failed to load voice list.', true);
      } finally {
        state.voiceRosterLoading = false;
        updateVoiceSamplesControls();
      }
      renderVoiceSamples();
      loadVoiceSampleCache({ force });
    }

    function groupSamplesByVoice(samples) {
      const grouped = {};
      samples.forEach(sample => {
        if (!sample || typeof sample !== 'object') return;
        if (!Number.isFinite(sample.voice_id)) return;
        const key = String(sample.voice_id);
        if (!grouped[key]) {
          grouped[key] = [];
        }
        grouped[key].push(sample);
      });
      Object.values(grouped).forEach(list => {
        list.sort((a, b) => {
          const aTime = Number.isFinite(a.created_at) ? a.created_at : 0;
          const bTime = Number.isFinite(b.created_at) ? b.created_at : 0;
          return bTime - aTime;
        });
      });
      return grouped;
    }

    async function loadVoiceSampleCache({ force = false } = {}) {
      if (state.voiceSampleCacheLoading) return;
      if (state.voiceSampleCacheLoaded && !force) {
        updateAllSavedSamples();
        return;
      }
      state.voiceSampleCacheLoading = true;
      try {
        const data = await fetchJSON('/api/voice-samples/cache');
        const samples = Array.isArray(data?.samples) ? data.samples : [];
        state.voiceSampleCache = groupSamplesByVoice(samples);
        state.voiceSampleCacheLoaded = true;
        state.voiceSampleCacheCount = Number.isFinite(data?.count) ? data.count : samples.length;
      } catch (err) {
        state.voiceSampleCache = {};
        state.voiceSampleCacheLoaded = false;
        state.voiceSampleCacheCount = 0;
      } finally {
        state.voiceSampleCacheLoading = false;
      }
      updateAllSavedSamples();
    }

    function rememberVoiceSample(sample) {
      if (!sample || typeof sample !== 'object') return;
      if (!Number.isFinite(sample.voice_id)) return;
      const key = String(sample.voice_id);
      const list = Array.isArray(state.voiceSampleCache[key]) ? state.voiceSampleCache[key] : [];
      const existingIndex = list.findIndex(entry => entry && entry.id === sample.id);
      if (existingIndex >= 0) {
        list[existingIndex] = sample;
      } else {
        list.unshift(sample);
      }
      list.sort((a, b) => {
        const aTime = Number.isFinite(a.created_at) ? a.created_at : 0;
        const bTime = Number.isFinite(b.created_at) ? b.created_at : 0;
        return bTime - aTime;
      });
      state.voiceSampleCache[key] = list;
      state.voiceSampleCacheLoaded = true;
    }

    function removeVoiceSample(sample) {
      if (!sample || typeof sample !== 'object') return;
      if (!Number.isFinite(sample.voice_id)) return;
      const key = String(sample.voice_id);
      const list = Array.isArray(state.voiceSampleCache[key]) ? state.voiceSampleCache[key] : [];
      const filtered = list.filter(entry => entry && entry.id !== sample.id);
      state.voiceSampleCache[key] = filtered;
    }

    function updateAllSavedSamples() {
      if (!voiceSamplesGrid) return;
      const cards = Array.from(voiceSamplesGrid.querySelectorAll('.voice-sample-card'));
      cards.forEach(card => {
        const voiceId = card.dataset.voiceId;
        if (!voiceId) return;
        updateSavedSamplesForVoice(voiceId);
      });
    }

    function updateSavedSamplesForVoice(voiceId) {
      if (!voiceSamplesGrid) return;
      const card = voiceSamplesGrid.querySelector(`.voice-sample-card[data-voice-id="${voiceId}"]`);
      if (!card) return;
      const listNode = card.querySelector('.voice-sample-saved-list');
      if (!listNode) return;
      const samples = Array.isArray(state.voiceSampleCache[String(voiceId)])
        ? state.voiceSampleCache[String(voiceId)]
        : [];
      renderSavedSamplesList(listNode, samples);
    }

    function renderSavedSamplesList(listNode, samples) {
      if (!listNode) return;
      listNode.innerHTML = '';
      const hasSamples = Array.isArray(samples) && samples.length > 0;
      const wrapper = listNode.closest('.voice-sample-saved');
      const title = wrapper ? wrapper.querySelector('.voice-sample-saved-title') : null;
      if (title) {
        title.textContent = hasSamples
          ? `Saved samples (${samples.length})`
          : 'Saved samples';
      }
      if (!hasSamples) {
        const empty = document.createElement('div');
        empty.className = 'voice-sample-empty';
        empty.textContent = 'No saved samples yet.';
        listNode.appendChild(empty);
        return;
      }
      samples.forEach(sample => {
        if (!sample || typeof sample !== 'object') return;
        const item = document.createElement('div');
        item.className = 'voice-sample-saved-item';
        if (sample.id) {
          item.dataset.sampleId = String(sample.id);
        }
        const meta = document.createElement('div');
        meta.className = 'voice-sample-saved-meta';
        const text = document.createElement('div');
        text.className = 'voice-sample-text-preview';
        const preview = typeof sample.text === 'string'
          ? sample.text.split(String.fromCharCode(10)).filter(line => line.trim())[0] || ''
          : '';
        text.textContent = preview ? preview : 'Sample text';
        meta.appendChild(text);
        const chips = document.createElement('div');
        chips.className = 'voice-sample-chips';
        const addChip = (label, value) => {
          const chip = document.createElement('span');
          chip.className = 'voice-sample-chip';
          chip.textContent = `${label} ${value}`;
          chips.appendChild(chip);
        };
        if (Number.isFinite(sample.speed)) {
          addChip('spd', Number(sample.speed).toFixed(2));
        }
        if (Number.isFinite(sample.pitch)) {
          addChip('pit', Number(sample.pitch).toFixed(2));
        }
        if (Number.isFinite(sample.intonation)) {
          addChip('int', Number(sample.intonation).toFixed(2));
        }
        meta.appendChild(chips);
        item.appendChild(meta);

        const actions = document.createElement('div');
        actions.className = 'voice-sample-saved-actions';
        const playBtn = document.createElement('button');
        playBtn.type = 'button';
        playBtn.className = 'secondary voice-sample-play';
        playBtn.textContent = 'Play';
        playBtn.addEventListener('click', () => {
          if (typeof sample.url === 'string' && sample.url) {
            playCachedSample(sample.url);
          }
        });
        actions.appendChild(playBtn);
        const deleteBtn = document.createElement('button');
        deleteBtn.type = 'button';
        deleteBtn.className = 'secondary voice-sample-delete';
        deleteBtn.textContent = 'Delete';
        deleteBtn.addEventListener('click', () => {
          handlePromise(deleteVoiceSample(sample));
        });
        actions.appendChild(deleteBtn);
        item.appendChild(actions);
        listNode.appendChild(item);
      });
    }

    function encodeSamplePath(pathValue) {
      if (!pathValue) return '';
      return pathValue.split('/').map(part => encodeURIComponent(part)).join('/');
    }

    async function deleteVoiceSample(sample) {
      if (!sample || typeof sample !== 'object') return;
      if (!sample.path || typeof sample.path !== 'string') return;
      if (!window.confirm('Delete this saved sample?')) return;
      const target = encodeSamplePath(sample.path);
      const res = await fetch(`/api/voice-samples/cached/${target}`, { method: 'DELETE' });
      if (!res.ok) {
        const detail = await readErrorResponse(res);
        throw new Error(detail || `HTTP ${res.status}`);
      }
      removeVoiceSample(sample);
      if (Number.isFinite(sample.voice_id)) {
        updateSavedSamplesForVoice(String(sample.voice_id));
      } else {
        updateAllSavedSamples();
      }
    }

    function parseSampleScale(input, fallback, label) {
      if (!input) return fallback;
      const raw = input.value.trim();
      if (!raw) return fallback;
      const num = Number(raw);
      if (!Number.isFinite(num)) {
        throw new Error(`${label} must be numeric.`);
      }
      return num;
    }

    function playCachedSample(url) {
      if (!url || typeof url !== 'string') return;
      if (!voiceSamplePlayer) {
        voiceSamplePlayer = new Audio();
        voiceSamplePlayer.preload = 'none';
      } else {
        voiceSamplePlayer.pause();
        try {
          voiceSamplePlayer.currentTime = 0;
        } catch {
          // ignore reset errors
        }
      }
      voiceSamplePlayer.src = url;
      voiceSamplePlayer.play().catch(() => {});
    }

    function setVoiceSampleRowStatus(node, message, isError = false) {
      if (!node) return;
      node.textContent = message || '';
      node.style.color = isError ? '#f87171' : 'var(--muted)';
    }

    async function generateVoiceSample({
      voiceId,
      characterName,
      voiceName,
      textInput,
      speedInput,
      pitchInput,
      intonationInput,
      statusNode,
      buttonNode,
    }) {
      if (!Number.isFinite(voiceId)) {
        setVoiceSampleRowStatus(statusNode, 'Invalid voice id.', true);
        return;
      }
      const rawText = textInput ? textInput.value : '';
      if (!rawText.trim()) {
        setVoiceSampleRowStatus(statusNode, 'Sample text is required.', true);
        return;
      }
      const defaults = state.voiceSampleDefaults || VOICE_SAMPLE_DEFAULTS;
      let speed;
      let pitch;
      let intonation;
      try {
        speed = parseSampleScale(speedInput, defaults.speed, 'Speed');
        pitch = parseSampleScale(pitchInput, defaults.pitch, 'Pitch');
        intonation = parseSampleScale(intonationInput, defaults.intonation, 'Intonation');
      } catch (err) {
        setVoiceSampleRowStatus(statusNode, err.message || 'Invalid parameters.', true);
        return;
      }
      if (buttonNode) {
        buttonNode.disabled = true;
        buttonNode.textContent = 'Generating...';
      }
      setVoiceSampleRowStatus(statusNode, 'Generating...');
      try {
        const res = await fetch('/api/voice-samples/cache', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            speaker: voiceId,
            character: characterName,
            voice_name: voiceName,
            text: rawText,
            speed,
            pitch,
            intonation,
          }),
        });
        if (!res.ok) {
          const detail = await readErrorResponse(res);
          throw new Error(detail || `HTTP ${res.status}`);
        }
        const payload = await res.json();
        const sample = payload?.sample;
        if (sample) {
          rememberVoiceSample(sample);
          updateSavedSamplesForVoice(String(voiceId));
          if (sample.url) {
            playCachedSample(sample.url);
          }
        }
        setVoiceSampleRowStatus(
          statusNode,
          payload?.cached ? 'Loaded cached sample.' : 'Saved sample.'
        );
      } catch (err) {
        setVoiceSampleRowStatus(
          statusNode,
          err.message || 'Failed to generate sample.',
          true
        );
      } finally {
        if (buttonNode) {
          buttonNode.disabled = false;
          buttonNode.textContent = 'Generate sample';
        }
      }
    }

    function createVoiceSampleCard(voice, characterName) {
      if (!voice || typeof voice !== 'object') return null;
      const voiceId = Number.isFinite(voice.id) ? voice.id : null;
      const voiceName = typeof voice.name === 'string' && voice.name.trim()
        ? voice.name.trim()
        : (typeof voice.display_name === 'string' && voice.display_name.trim()
          ? voice.display_name.trim()
          : 'Voice');
      if (voiceId === null) return null;
      const defaults = state.voiceSampleDefaults || VOICE_SAMPLE_DEFAULTS;

      const card = document.createElement('article');
      card.className = 'card voice-sample-card';
      card.dataset.voiceId = String(voiceId);

      const header = document.createElement('div');
      header.className = 'voice-sample-header';
      const name = document.createElement('div');
      name.className = 'voice-sample-name';
      name.textContent = voiceName;
      header.appendChild(name);
      const idLabel = document.createElement('div');
      idLabel.className = 'voice-sample-id';
      idLabel.textContent = `#${voiceId}`;
      header.appendChild(idLabel);
      card.appendChild(header);

      const controls = document.createElement('div');
      controls.className = 'voice-sample-controls';

      const textLabel = document.createElement('label');
      textLabel.className = 'voice-field voice-sample-text';
      textLabel.append('Sample text');
      const textarea = document.createElement('textarea');
      textarea.rows = 4;
      textarea.value = state.voiceSampleText || '';
      textLabel.appendChild(textarea);
      controls.appendChild(textLabel);

      const params = document.createElement('div');
      params.className = 'voice-sample-params';

      const speedLabel = document.createElement('label');
      speedLabel.className = 'voice-field';
      speedLabel.append('Speed');
      const speedInput = document.createElement('input');
      speedInput.type = 'number';
      speedInput.step = '0.01';
      speedInput.value = Number.isFinite(defaults.speed) ? String(defaults.speed) : '';
      speedLabel.appendChild(speedInput);
      params.appendChild(speedLabel);

      const pitchLabel = document.createElement('label');
      pitchLabel.className = 'voice-field';
      pitchLabel.append('Pitch');
      const pitchInput = document.createElement('input');
      pitchInput.type = 'number';
      pitchInput.step = '0.01';
      pitchInput.value = Number.isFinite(defaults.pitch) ? String(defaults.pitch) : '';
      pitchLabel.appendChild(pitchInput);
      params.appendChild(pitchLabel);

      const intonationLabel = document.createElement('label');
      intonationLabel.className = 'voice-field';
      intonationLabel.append('Intonation');
      const intonationInput = document.createElement('input');
      intonationInput.type = 'number';
      intonationInput.step = '0.01';
      intonationInput.value = Number.isFinite(defaults.intonation)
        ? String(defaults.intonation)
        : '';
      intonationLabel.appendChild(intonationInput);
      params.appendChild(intonationLabel);

      controls.appendChild(params);
      card.appendChild(controls);

      const actions = document.createElement('div');
      actions.className = 'voice-sample-actions';
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = 'Generate sample';
      const status = document.createElement('span');
      status.className = 'voice-sample-status';
      actions.appendChild(button);
      actions.appendChild(status);
      card.appendChild(actions);

      const savedWrap = document.createElement('div');
      savedWrap.className = 'voice-sample-saved';
      const savedTitle = document.createElement('div');
      savedTitle.className = 'voice-sample-saved-title';
      savedTitle.textContent = 'Saved samples';
      const savedList = document.createElement('div');
      savedList.className = 'voice-sample-saved-list';
      savedWrap.appendChild(savedTitle);
      savedWrap.appendChild(savedList);
      card.appendChild(savedWrap);
      renderSavedSamplesList(
        savedList,
        Array.isArray(state.voiceSampleCache[String(voiceId)])
          ? state.voiceSampleCache[String(voiceId)]
          : []
      );

      button.addEventListener('click', () => {
        generateVoiceSample({
          voiceId,
          characterName,
          voiceName,
          textInput: textarea,
          speedInput,
          pitchInput,
          intonationInput,
          statusNode: status,
          buttonNode: button,
        });
      });

      return card;
    }

    function renderVoiceSamples() {
      if (!voiceSamplesPage || !voiceSamplesGrid) return;
      if (!isVoiceSamplesView()) {
        clearVoiceSamplesGrid();
        return;
      }
      if (!state.voiceRosterLoaded) {
        clearVoiceSamplesGrid();
        return;
      }
      const roster = Array.isArray(state.voiceRoster) ? state.voiceRoster : [];
      clearVoiceSamplesGrid();
      if (roster.length === 0) {
        return;
      }
      roster.forEach(entry => {
        if (!entry || typeof entry !== 'object') {
          return;
        }
        const name = typeof entry.name === 'string' && entry.name.trim()
          ? entry.name.trim()
          : 'Voice';
        const voices = Array.isArray(entry.voices) ? entry.voices : [];
        if (!voices.length) {
          return;
        }
        const group = document.createElement('details');
        group.className = 'voice-sample-group';
        group.open = true;
        const summary = document.createElement('summary');
        summary.textContent = name;
        group.appendChild(summary);
        const grid = document.createElement('div');
        grid.className = 'cards voice-sample-group-grid';
        voices.forEach(voice => {
          const card = createVoiceSampleCard(voice, name);
          if (card) {
            grid.appendChild(card);
          }
        });
        if (!grid.childElementCount) {
          return;
        }
        group.appendChild(grid);
        voiceSamplesGrid.appendChild(group);
      });
    }

    function renderCollections() {
      if (!collectionsGrid) return;
      collectionsGrid.innerHTML = '';
      const isRoot = !normalizeLibraryPath(state.libraryPrefix);
      const hasCollections = Array.isArray(state.collections) && state.collections.length > 0;
      const lastPlayedCard = isRoot && state.lastPlayedBook
        ? createBookCard(state.lastPlayedBook, { featuredLabel: 'Last Played' })
        : null;
      if (!hasCollections && !lastPlayedCard) {
        collectionsGrid.classList.add('hidden');
        return;
      }
      collectionsGrid.classList.remove('hidden');
      if (lastPlayedCard) {
        collectionsGrid.appendChild(lastPlayedCard);
      }
      state.collections.forEach(collection => {
        if (!collection || typeof collection !== 'object') {
          return;
        }
        const card = document.createElement('article');
        card.className = 'card collection-card';
        const collectionId = typeof collection.path === 'string' && collection.path
          ? collection.path
          : (collection.id || '');
        if (collectionId) {
          card.dataset.collectionId = collectionId;
        }
        const collage = document.createElement('div');
        collage.className = 'collection-covers';
        const sampleSlots = 9;
        const samples = Array.isArray(collection.cover_samples)
          ? collection.cover_samples.filter(item => typeof item === 'string' && item).slice(0, sampleSlots)
          : [];
        for (let idx = 0; idx < sampleSlots; idx += 1) {
          const tile = document.createElement('span');
          tile.className = 'collection-cover-tile';
          const sample = samples[idx];
          if (sample) {
            tile.style.backgroundImage = `url("${sample}")`;
          } else {
            tile.classList.add('collection-cover-placeholder');
          }
          collage.appendChild(tile);
        }
        card.appendChild(collage);
        const info = document.createElement('div');
        info.className = 'collection-info';
        const icon = document.createElement('div');
        icon.className = 'collection-icon';
        icon.textContent = '📁';
        const header = document.createElement('div');
        header.className = 'collection-meta-row';
        header.appendChild(icon);
        const name = document.createElement('div');
        name.className = 'collection-name';
        name.textContent = collection.name || collection.id || collectionId || 'Collection';
        header.appendChild(name);
        info.appendChild(header);
        const meta = document.createElement('div');
        meta.className = 'collection-meta';
        const count = typeof collection.book_count === 'number' ? collection.book_count : 0;
        meta.textContent = count === 1 ? '1 book' : `${count} books`;
        info.appendChild(meta);
        card.appendChild(info);
        card.addEventListener('click', () => {
          const targetPath = typeof collection.path === 'string' ? collection.path : (collection.id || '');
          if (state.currentBook) {
            closeBookView({ skipHistory: true });
          }
          handlePromise(loadBooks(targetPath || ''));
        });
        collectionsGrid.appendChild(card);
      });
    }

    function formatFileSize(bytes) {
      if (!Number.isFinite(bytes) || bytes <= 0) {
        return '';
      }
      if (bytes >= 1024 * 1024) {
        return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
      }
      if (bytes >= 1024) {
        return `${(bytes / 1024).toFixed(1)} kB`;
      }
      return `${bytes} B`;
    }

    function formatTimestamp(ts) {
      if (!Number.isFinite(ts)) return '';
      try {
        const date = new Date(ts * 1000);
        return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
      } catch {
        return '';
      }
    }

    function formatDateTime(ts) {
      if (!Number.isFinite(ts)) return '';
      try {
        const date = new Date(ts * 1000);
        return date.toLocaleString(undefined, {
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
        });
      } catch {
        return '';
      }
    }

    function syncPendingEpubBusy() {
      if (!state.epubBusy) {
        state.epubBusy = new Set();
      }
      const available = new Set(
        Array.isArray(state.pendingEpubs) ? state.pendingEpubs.map(epub => epub.path) : []
      );
      for (const path of Array.from(state.epubBusy)) {
        if (!available.has(path)) {
          state.epubBusy.delete(path);
        }
      }
    }

    function renderPendingEpubs() {
      if (!pendingEpubPanel || !pendingEpubList) return;
      if (!state.pendingEpubs.length) {
        pendingEpubPanel.classList.add('hidden');
        return;
      }
      pendingEpubPanel.classList.remove('hidden');
      pendingEpubPanel.open = false;
      pendingEpubList.innerHTML = '';
      state.pendingEpubs.forEach(epub => {
        if (!epub || typeof epub !== 'object') return;
        const path = epub.path || epub.filename;
        const busy = state.epubBusy.has(path);
        const item = document.createElement('div');
        item.className = 'epub-item';
        const info = document.createElement('div');
        info.className = 'epub-item-info';
        const name = document.createElement('div');
        name.className = 'epub-item-name';
        name.textContent = epub.filename || path;
        info.appendChild(name);
        const meta = document.createElement('div');
        meta.className = 'epub-item-meta';
        const details = [];
        if (epub.target_name) {
          details.push(`→ ${epub.target_name}`);
        } else if (epub.target_path) {
          details.push(`→ ${epub.target_path}`);
        }
        if (typeof epub.size === 'number') {
          const sizeLabel = formatFileSize(epub.size);
          if (sizeLabel) {
            details.push(sizeLabel);
          }
        }
        if (typeof epub.modified === 'number') {
          const dateLabel = formatTimestamp(epub.modified);
          if (dateLabel) {
            details.push(dateLabel);
          }
        }
        meta.textContent = details.join(' · ');
        info.appendChild(meta);
        item.appendChild(info);
        const button = document.createElement('button');
        button.type = 'button';
        button.className = busy ? 'secondary' : '';
        button.disabled = busy;
        button.textContent = busy ? 'Queued…' : 'Chapterize';
        button.addEventListener('click', () => {
          handlePromise(queueChapterizeEpubs([path]));
        });
        item.appendChild(button);
        pendingEpubList.appendChild(item);
      });
      if (pendingEpubAllBtn) {
        const anyIdle = state.pendingEpubs.some(epub => !state.epubBusy.has(epub.path || epub.filename));
        pendingEpubAllBtn.disabled = !anyIdle;
      }
    }

    async function queueChapterizeEpubs(paths) {
      const valid = Array.isArray(paths)
        ? paths
            .map(entry => (typeof entry === 'string' ? entry : null))
            .filter(Boolean)
        : [];
      if (!valid.length) return;
      if (!state.epubBusy) {
        state.epubBusy = new Set();
      }
      valid.forEach(path => state.epubBusy.add(path));
      renderPendingEpubs();
      try {
        const res = await fetch('/api/epubs/chapterize', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ paths: valid }),
        });
        if (!res.ok) {
          const detail = await res.text();
          throw new Error(detail || `HTTP ${res.status}`);
        }
        await loadBooks(state.libraryPrefix || '', { skipHistory: true });
        await loadUploads();
      } catch (err) {
        alert(`Failed to chapterize EPUBs: ${err.message || err}`);
      } finally {
        syncPendingEpubBusy();
        renderPendingEpubs();
      }
    }
    function setSortSelectVisibility(show) {
      if (!booksSortWrapper) return;
      booksSortWrapper.classList.toggle('hidden', !show);
    }

    function renderBooks() {
      if (!booksGrid) return;
      booksGrid.innerHTML = '';
      closeCardMenu();
      const isRecentView = state.libraryPrefix === RECENTLY_PLAYED_PREFIX;
      const hasRealCollections = Array.isArray(state.collections)
        ? state.collections.some(collection => collection && !collection.virtual)
        : false;
      const showUpload = !isRecentView && !hasRealCollections;
      const hasBooks = Array.isArray(state.books) && state.books.length > 0;
      setSortSelectVisibility(hasBooks && !isRecentView);
      if (!showUpload && !hasBooks) {
        booksGrid.classList.add('hidden');
        scrollToLastBook();
        return;
      }
      booksGrid.classList.remove('hidden');
      const uploadCard = showUpload ? ensureUploadCard() : null;
      if (uploadCard && showUpload) {
        booksGrid.appendChild(uploadCard);
      }
      if (!hasBooks) {
        const empty = document.createElement('article');
        empty.className = 'card empty-card';
        empty.textContent = isRecentView
          ? 'No recently played books yet.'
          : 'No books in this folder yet. Choose a collection or upload a book.';
        booksGrid.appendChild(empty);
        scrollToLastBook();
        return;
      }
      state.books.forEach(book => {
        const card = createBookCard(book);
        if (card) {
          booksGrid.appendChild(card);
        }
      });
      scrollToLastBook();
    }

    function chapterWasPlayed(chapter) {
      if (!chapter || !chapter.id) return false;
      const played = state.bookmarks.played;
      if (played instanceof Set) {
        return played.has(chapter.id);
      }
      if (Array.isArray(played)) {
        return played.includes(chapter.id);
      }
      return false;
    }

    function chapterStatusInfo(ch) {
      const status = ch.build_status;
      if (status) {
        if (status.state === 'queued') {
          return { label: 'Queued', className: 'warning' };
        }
        if (status.state === 'building') {
          const total = typeof status.chunk_count === 'number' && status.chunk_count > 0 ? status.chunk_count : null;
          const current = typeof status.chunk_index === 'number' && status.chunk_index > 0 ? status.chunk_index : 0;
          let label = 'Building...';
          if (total) {
            label = current > 0 ? `Building ${Math.min(current, total)}/${total}` : `Building 0/${total}`;
          } else if (current > 0) {
            label = `Building chunk ${current}`;
          }
          return { label, className: 'warning' };
        }
        if (status.state === 'aborting') {
          const label = status.was_queued ? 'Cancelling…' : 'Aborting…';
          return { label, className: 'warning' };
        }
        if (status.state === 'aborted') {
          return { label: 'Aborted', className: 'muted' };
        }
        if (status.state === 'error') {
          return { label: 'Failed', className: 'danger' };
        }
      }
      if (ch.mp3_exists) {
        return {
          label: chapterWasPlayed(ch) ? 'Played' : 'Ready',
          className: chapterWasPlayed(ch) ? 'played' : 'success',
        };
      }
      if (ch.has_cache) {
        return { label: 'Cached', className: 'muted' };
      }
      return { label: 'Pending', className: 'warning' };
    }

    function updateChapterStatusUI() {
      const nodes = chaptersList.querySelectorAll('.chapter');
      nodes.forEach(node => {
        const chapterId = node.dataset.chapterId;
        if (!chapterId) return;
        const chapter = state.chapters.find(ch => ch.id === chapterId);
        if (!chapter) return;
        const chapterIndex = state.chapters.indexOf(chapter);
        const statusInfo = chapterStatusInfo(chapter);
        const statusEl = node.querySelector('[data-role="status-label"]');
        if (statusEl) {
          statusEl.textContent = statusInfo.label;
          statusEl.className = statusInfo.className ? `badge ${statusInfo.className}` : 'badge';
        }
        const primaryBtn = node.querySelector('[data-role="primary-action"]');
        if (primaryBtn) {
          primaryBtn.textContent = chapterPrimaryLabel(chapter);
          const isBuilding = chapter.build_status && chapter.build_status.state === 'building';
          const isQueued = chapter.build_status && chapter.build_status.state === 'queued';
          const isAborting = chapter.build_status && chapter.build_status.state === 'aborting';
          primaryBtn.disabled = Boolean(isAborting);
          primaryBtn.classList.toggle('danger', Boolean(isBuilding || isQueued || isAborting));
          primaryBtn.onclick = () => {
            if (isBuilding || isQueued) {
              handlePromise(abortChapter(chapterIndex));
            } else if (chapter.mp3_exists) {
              handlePromise(playChapter(chapterIndex));
            } else {
              handlePromise(buildChapter(chapterIndex, { restart: false }));
            }
          };
        }
        const restartBtn = node.querySelector('[data-role="restart-action"]');
        if (restartBtn) {
          restartBtn.disabled = Boolean(
            chapter.build_status
            && (chapter.build_status.state === 'building'
              || chapter.build_status.state === 'queued'
              || chapter.build_status.state === 'aborting')
          );
        }
      });
    }

    function applyStatusUpdates(statusMap) {
      state.chapters.forEach(ch => {
        const nextStatus = statusMap[ch.id] || null;
        const isQueued = ch.build_status && ch.build_status.state === 'queued';
        const hasLocal = state.localBuilds && state.localBuilds.has(ch.id);
        if (nextStatus) {
          ch.build_status = nextStatus;
        } else if (isQueued || hasLocal) {
          // keep local state
        } else {
          ch.build_status = null;
        }
        if (nextStatus && typeof nextStatus.chunk_count === 'number' && !ch.total_chunks) {
          ch.total_chunks = nextStatus.chunk_count;
        }
      });
      updateChapterStatusUI();
      maybeStartNextBuild();
    }

    let statusRequestPending = false;

    async function refreshStatuses() {
      if (!state.currentBook || statusRequestPending) return;
      statusRequestPending = true;
      try {
        const data = await fetchJSON(`/api/books/${encodeURIComponent(state.currentBook.id)}/status`);
        applyStatusUpdates(data.status || {});
      } catch (err) {
        // ignore transient polling errors
      } finally {
        statusRequestPending = false;
      }
    }

    function startStatusPolling() {
      if (statusPollHandle || !state.currentBook) return;
      statusPollHandle = setInterval(refreshStatuses, 2500);
      refreshStatuses();
    }

    function stopStatusPolling() {
      if (statusPollHandle) {
        clearInterval(statusPollHandle);
        statusPollHandle = null;
      }
      statusRequestPending = false;
    }

    function chapterPrimaryLabel(ch) {
      if (ch.build_status && ch.build_status.state === 'queued') {
        return 'Cancel';
      }
      if (ch.build_status && ch.build_status.state === 'building') {
        return 'Abort';
      }
      if (ch.build_status && ch.build_status.state === 'aborting') {
        return ch.build_status.was_queued ? 'Cancelling…' : 'Aborting…';
      }
      return ch.mp3_exists ? 'Play' : 'Build';
    }

    function chapterTextPath(chapter) {
      if (!chapter) {
        return null;
      }
      const chapterId = typeof chapter.id === 'string' ? chapter.id : null;
      if (!chapterId) {
        return null;
      }
      const bookId = state.currentBook && typeof state.currentBook.id === 'string'
        ? state.currentBook.id
        : '';
      if (!bookId) {
        return chapterId;
      }
      return `${bookId.replace(/\/+$/, '')}/${chapterId}`;
    }

    function openChapterInReader(chapter) {
      if (!state.readerUrl) {
        return;
      }
      const relPath = chapterTextPath(chapter);
      if (!relPath) {
        return;
      }
      let target = null;
      try {
        const parsed = new URL(state.readerUrl, window.location.href);
        parsed.searchParams.set('view', 'transformed-only');
        parsed.searchParams.set('original', 'off');
        parsed.hash = `chapter=${encodeURIComponent(relPath)}`;
        target = parsed.toString();
      } catch (error) {
        const normalized = state.readerUrl.endsWith('/') ? state.readerUrl : `${state.readerUrl}/`;
        const joiner = normalized.includes('?') ? '&' : '?';
        target = `${normalized}${joiner}view=transformed-only&original=off#chapter=${encodeURIComponent(relPath)}`;
      }
      window.open(target, '_blank', 'noopener');
    }

    function setChapterStatusLabel(chapterId, label, className = '') {
      if (!chapterId) return;
      const wrapper = chaptersList.querySelector(`[data-chapter-id="${chapterId}"]`);
      if (!wrapper) return;
      const statusEl = wrapper.querySelector('[data-role="status-label"]');
      if (statusEl) {
        statusEl.textContent = label;
        statusEl.className = className || 'badge';
      }
    }

    function updateChapterHighlight() {
      const nodes = chaptersList.querySelectorAll('.chapter');
      let docked = false;
      nodes.forEach((node, idx) => {
        if (idx === state.currentChapterIndex) {
          node.classList.add('playing');
          if (playerDock && !docked) {
            const chapterBookmarks = node.querySelector('.bookmark-list');
            if (chapterBookmarks) {
              chapterBookmarks.parentNode.insertBefore(playerDock, chapterBookmarks);
            } else {
              node.appendChild(playerDock);
            }
            playerDock.classList.remove('hidden');
            docked = true;
          }
        } else {
          node.classList.remove('playing');
        }
      });
      if (playerDock && !docked) {
        playerDock.classList.add('hidden');
        chaptersPanel.appendChild(playerDock);
      }
      if (state.currentChapterIndex < 0) {
        updatePlayerDetails(null);
      }
      updateTransportAvailability();
    }

    function renderChapters(summary) {
      chaptersList.innerHTML = '';
      chaptersTitle.textContent = state.currentBook.title;
      chaptersMetrics.innerHTML = '';
      chaptersMetrics.appendChild(
        badge(
          `${summary.completed}/${summary.total} ready`,
          summary.completed === summary.total && summary.total > 0 ? 'success' : ''
        )
      );
      if (summary.pending > 0) {
        chaptersMetrics.appendChild(badge(`${summary.pending} pending`, 'warning'));
      }

      state.chapters.forEach((ch, index) => {
        const wrapper = document.createElement('article');
        wrapper.className = 'chapter';
        wrapper.dataset.chapterId = ch.id;

        const header = document.createElement('div');
        header.className = 'chapter-header';
        const name = document.createElement('div');
        name.className = 'name';
        const trackLabel = formatTrackNumber(ch.track_number);
        const displayTitle = chapterDisplayTitle(ch) || ch.id;
        const fullTitle = chapterTitleText(ch) || ch.id;
        name.textContent = trackLabel ? `${trackLabel} ${displayTitle}` : displayTitle;
        name.title = trackLabel ? `${trackLabel} ${fullTitle}` : fullTitle;
        header.appendChild(name);
        wrapper.appendChild(header);

        const footer = document.createElement('div');
        footer.className = 'chapter-footer';
        const statusBadges = document.createElement('div');
        statusBadges.className = 'badges';
        const statusInfo = chapterStatusInfo(ch);
        const statusSpan = document.createElement('span');
        statusSpan.dataset.role = 'status-label';
        statusSpan.className = statusInfo.className ? `badge ${statusInfo.className}` : 'badge';
        statusSpan.textContent = statusInfo.label;
        statusBadges.appendChild(statusSpan);
        if (ch.mp3_exists && Number.isFinite(ch.mp3_mtime)) {
          const builtBadge = document.createElement('span');
          builtBadge.className = 'badge muted';
          builtBadge.textContent = `Built ${formatDateTime(ch.mp3_mtime)}`;
          statusBadges.appendChild(builtBadge);
        }

        footer.appendChild(statusBadges);

        const buttons = document.createElement('div');
        buttons.className = 'badges controls';
        const playBtn = document.createElement('button');
        playBtn.textContent = chapterPrimaryLabel(ch);
        const isBuilding = ch.build_status && ch.build_status.state === 'building';
        const isQueued = ch.build_status && ch.build_status.state === 'queued';
        const isAborting = ch.build_status && ch.build_status.state === 'aborting';
        playBtn.disabled = Boolean(isAborting);
        playBtn.classList.toggle('danger', Boolean(isBuilding || isQueued || isAborting));
        playBtn.onclick = () => {
          if (isBuilding || isQueued) {
            handlePromise(abortChapter(index));
          } else if (ch.mp3_exists) {
            handlePromise(playChapter(index));
          } else {
            handlePromise(buildChapter(index, { restart: false }));
          }
        };
        buttons.appendChild(playBtn);

        const restartBtn = document.createElement('button');
        restartBtn.dataset.role = 'restart-action';
        restartBtn.textContent = 'Rebuild';
        restartBtn.className = 'secondary';
        restartBtn.disabled = Boolean(
          ch.build_status
          && (ch.build_status.state === 'building'
            || ch.build_status.state === 'queued'
            || ch.build_status.state === 'aborting')
        );
        restartBtn.onclick = () => {
          const chapter = state.chapters[index];
          const label = chapter ? chapter.title : 'this chapter';
          const confirmText = `Rebuild audio for ${label}? Existing MP3 will be overwritten.`;
          if (!window.confirm(confirmText)) {
            return;
          }
          handlePromise(buildChapter(index, { restart: true }));
        };
        buttons.appendChild(restartBtn);
        const refineBtn = document.createElement('button');
        refineBtn.textContent = 'Refine';
        refineBtn.className = 'secondary';
        refineBtn.disabled = !state.readerUrl;
        refineBtn.title = state.readerUrl ? 'Open nk Reader for this chapter' : 'Reader unavailable';
        refineBtn.onclick = () => {
          openChapterInReader(ch);
        };
        buttons.appendChild(refineBtn);
        footer.appendChild(buttons);

        wrapper.appendChild(footer);

        const bookmarkContainer = document.createElement('div');
        bookmarkContainer.className = 'bookmark-list';
        bookmarkContainer.dataset.role = 'chapter-bookmarks';
        bookmarkContainer.dataset.chapterId = ch.id;
        renderBookmarkList(bookmarkContainer, bookmarksForChapter(ch.id), ch);
        wrapper.appendChild(bookmarkContainer);
        chaptersList.appendChild(wrapper);
      });
      updateChapterHighlight();
      updateChapterStatusUI();
      updateTransportAvailability();
    }

    function summaryForChapters() {
      const total = state.chapters.length;
      const completed = state.chapters.filter(ch => ch.mp3_exists).length;
      return {
        total,
        completed,
        pending: total - completed,
      };
    }

    function normalizedSortMode(mode) {
      return LIBRARY_SORT_OPTIONS.includes(mode) ? mode : 'author';
    }

    function libraryCacheKey(prefix, sortMode) {
      return `${normalizeLibraryPath(prefix)}::${normalizedSortMode(sortMode)}`;
    }

    function normalizeLibraryPayload(payload, fallbackPrefix = '') {
      const canonicalPrefix = normalizeLibraryPath(
        typeof payload?.prefix === 'string' ? payload.prefix : fallbackPrefix
      );
      return {
        prefix: canonicalPrefix,
        parent_prefix: normalizeLibraryPath(payload?.parent_prefix),
        collections: Array.isArray(payload?.collections) ? payload.collections : [],
        books: Array.isArray(payload?.books) ? payload.books : [],
        pending_epubs: Array.isArray(payload?.pending_epubs) ? payload.pending_epubs : [],
        last_played_book: payload && typeof payload.last_played_book === 'object' && payload.last_played_book
          ? payload.last_played_book
          : null,
      };
    }

    function readLibraryCache(prefix, sortMode) {
      return libraryCache.get(libraryCacheKey(prefix, sortMode)) || null;
    }

    function rememberLibraryPayload(payload, sortMode, fallbackPrefix = '') {
      if (!payload) return null;
      const entry = normalizeLibraryPayload(payload, fallbackPrefix);
      const key = libraryCacheKey(entry.prefix, sortMode);
      libraryCache.set(key, entry);
      if (libraryCache.size > LIBRARY_CACHE_LIMIT) {
        const oldest = libraryCache.keys().next();
        if (!oldest.done) {
          libraryCache.delete(oldest.value);
        }
      }
      return entry;
    }

    function applyLibraryPayload(payload, options = {}) {
      if (!payload) return;
      const historyOptions =
        options && typeof options === 'object' ? options : {};
      const { skipHistory = false, replaceHistory = false } = historyOptions;
      state.libraryPrefix = payload.prefix || '';
      state.parentPrefix = payload.parent_prefix || '';
      state.collections = Array.isArray(payload.collections) ? payload.collections : [];
      state.books = Array.isArray(payload.books) ? payload.books : [];
      state.pendingEpubs = Array.isArray(payload.pending_epubs) ? payload.pending_epubs : [];
      state.lastPlayedBook = payload.last_played_book || null;
      syncPendingEpubBusy();
      renderLibraryNav();
      updateVoiceSamplesVisibility();
      updateVoiceSamplesControls();
      renderVoiceSamples();
      renderCollections();
      renderPendingEpubs();
      renderBooks();
      if (!skipHistory) {
        updateLocationFromState({ replace: replaceHistory });
      }
    }

    async function loadBooks(nextPrefix = undefined, options = {}) {
      const navOptions =
        options && typeof options === 'object' ? options : {};
      const { skipHistory = false, replaceHistory = false } = navOptions;
      let targetPrefix = '';
      if (typeof nextPrefix === 'string') {
        targetPrefix = nextPrefix;
      } else if (typeof state.libraryPrefix === 'string') {
        targetPrefix = state.libraryPrefix;
      }
      const sortMode = normalizedSortMode(state.librarySortOrder || 'author');
      const cachedEntry = readLibraryCache(targetPrefix, sortMode);
      if (cachedEntry) {
        applyLibraryPayload(cachedEntry, { skipHistory, replaceHistory });
      }
      const params = new URLSearchParams();
      params.set('sort', sortMode);
      if (targetPrefix) {
        params.set('prefix', targetPrefix);
      }
      const requestId = ++libraryRequestToken;
      let data;
      try {
        data = await fetchJSON(`/api/books?${params.toString()}`);
      } catch (err) {
        if (requestId !== libraryRequestToken) {
          return;
        }
        throw err;
      }
      if (requestId !== libraryRequestToken) {
        return;
      }
      const entry = rememberLibraryPayload(data, sortMode, targetPrefix);
      const historyOptions = cachedEntry
        ? { skipHistory: true }
        : { skipHistory, replaceHistory };
      applyLibraryPayload(entry, historyOptions);
    }

    async function openBook(book, options = {}) {
      const navOptions =
        options && typeof options === 'object' ? options : {};
      const { skipHistory = false, replaceHistory = false } = navOptions;
      lastOpenedBookId = book.id;
      const loaded = await loadChapters(book);
      const panel = document.getElementById('chapters-panel');
      if (panel) {
        panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
      if (!skipHistory && loaded !== false) {
        updateLocationFromState({ replace: replaceHistory });
      }
    }

    function findBookById(bookId) {
      const target = normalizeLibraryPath(bookId);
      if (!target || !Array.isArray(state.books)) {
        return null;
      }
      return state.books.find(book => {
        if (!book || typeof book !== 'object') {
          return false;
        }
        const candidate = normalizeLibraryPath(book.id || book.path);
        return candidate === target;
      }) || null;
    }

    async function openBookById(bookId, options = {}) {
      const book = findBookById(bookId);
      if (!book) {
        return false;
      }
      await openBook(book, options);
      return true;
    }

    async function loadChapters(book, options = {}) {
      const { preserveSelection = false } = options;
      state.currentBook = book;
      lastOpenedBookId = book.id;
      if (state.localBuilds) {
        state.localBuilds.clear();
      } else {
        state.localBuilds = new Set();
      }
      state.buildQueue = [];
      state.activeBuild = null;
      if (!preserveSelection) {
        state.autoAdvance = false;
        state.currentChapterIndex = -1;
        stopStatusPolling();
        setBookmarks({ manual: [], last_played: null });
        lastPlaySyncAt = 0;
      }
      try {
        const data = await fetchJSON(`/api/books/${encodeURIComponent(book.id)}/chapters`);
        state.chapters = data.chapters;
        state.media = data.media || null;
        if (state.currentBook && data.media) {
          if (data.media.album) {
            state.currentBook.title = data.media.album;
          }
          if (data.media.artist) {
            state.currentBook.author = data.media.artist;
          }
          if (data.media.cover_url) {
            state.currentBook.cover_url = data.media.cover_url;
          }
        }
        const defaultsPayload = data.media?.tts_defaults;
        if (defaultsPayload) {
          applyVoiceDefaults(defaultsPayload.effective || DEFAULT_VOICE, defaultsPayload.saved || {});
        } else {
          applyVoiceDefaults(DEFAULT_VOICE, {});
        }
        if (data.bookmarks) {
          setBookmarks(data.bookmarks);
        } else {
          setBookmarks({ manual: [], last_played: null });
        }
        renderChapters(data.summary);
        chaptersPanel.classList.remove('hidden');
        startStatusPolling();
        return true;
      } catch (err) {
        alert(`Failed to load chapters: ${err.message}`);
        return false;
      }
    }

    function currentServerBuildingId() {
      const entry = state.chapters.find(ch => ch.build_status && ch.build_status.state === 'building');
      return entry ? entry.id : null;
    }

    function dequeueBuild(chapterId) {
      if (!Array.isArray(state.buildQueue) || !chapterId) return false;
      const before = state.buildQueue.length;
      state.buildQueue = state.buildQueue.filter(entry => entry && entry.id !== chapterId);
      return before !== state.buildQueue.length;
    }

    function enqueueBuild(index, { restart = false } = {}) {
      if (!state.currentBook) return;
      const chapter = state.chapters[index];
      if (!chapter) return;
      if (chapter.build_status && (chapter.build_status.state === 'building' || chapter.build_status.state === 'aborting')) {
        return;
      }
      if (!Array.isArray(state.buildQueue)) {
        state.buildQueue = [];
      }
      if (!state.buildQueue.some(entry => entry && entry.id === chapter.id)) {
        state.buildQueue.push({ id: chapter.id, restart: Boolean(restart) });
      }
      chapter.build_status = { state: 'queued' };
      setChapterStatusLabel(chapter.id, 'Queued', 'badge warning');
      renderChapters(summaryForChapters());
      maybeStartNextBuild();
    }

    function buildChapter(index, options = {}) {
      enqueueBuild(index, options);
    }

    function finishActiveBuild() {
      state.activeBuild = null;
      maybeStartNextBuild();
    }

    async function startBuildNow(queueEntry) {
      if (!state.currentBook) return;
      const chapterIndex = state.chapters.findIndex(ch => ch.id === queueEntry.id);
      const chapter = chapterIndex >= 0 ? state.chapters[chapterIndex] : null;
      if (!chapter) {
        finishActiveBuild();
        return;
      }
      state.activeBuild = chapter.id;
      const params = new URLSearchParams();
      if (queueEntry.restart) params.set('restart', '1');
      chapter.build_status = { state: 'building' };
      if (state.localBuilds) {
        state.localBuilds.add(chapter.id);
      }
      setChapterStatusLabel(chapter.id, queueEntry.restart ? 'Rebuilding…' : 'Building…', 'badge warning');
      renderChapters(summaryForChapters());
      if (state.currentChapterIndex === chapterIndex) {
        statusLine.textContent = queueEntry.restart ? 'Rebuilding audio...' : 'Building audio...';
      }
      try {
        const res = await fetch(
          `/api/books/${encodeURIComponent(state.currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/prepare?${params.toString()}`,
          { method: 'POST' }
        );
        if (!res.ok) {
          const detail = await readErrorResponse(res);
          if (res.status === 409 && detail.toLowerCase().includes('aborted')) {
            chapter.build_status = null;
            if (state.currentChapterIndex === chapterIndex) {
              statusLine.textContent = 'Build aborted.';
            }
            finishActiveBuild();
            return;
          }
          if (res.status === 409 && detail.toLowerCase().includes('already building')) {
            chapter.build_status = { state: 'queued' };
            state.activeBuild = null;
            if (!state.buildQueue.some(entry => entry && entry.id === chapter.id)) {
              state.buildQueue.unshift(queueEntry);
            }
            refreshStatuses();
            return;
          }
          chapter.build_status = null;
          if (state.currentChapterIndex !== chapterIndex) {
            setChapterStatusLabel(chapter.id, 'Failed', 'badge danger');
          }
          throw new Error(detail);
        }
        const result = await res.json();
        if (state.localBuilds) {
          state.localBuilds.delete(chapter.id);
        }
        chapter.build_status = null;
        chapter.mp3_exists = true;
        if (typeof result.mp3_mtime === 'number') {
          chapter.mp3_mtime = result.mp3_mtime;
        } else {
          chapter.mp3_mtime = Date.now() / 1000;
        }
        if (typeof result.total_chunks === 'number') {
          chapter.total_chunks = result.total_chunks;
        }
        chapter.has_cache = true;
        renderChapters(summaryForChapters());
        if (state.currentChapterIndex === chapterIndex) {
          statusLine.textContent = 'Build finished. Tap Play to listen.';
        }
      } catch (err) {
        if (state.localBuilds) {
          state.localBuilds.delete(chapter.id);
        }
        chapter.build_status = null;
        renderChapters(summaryForChapters());
        throw err;
      } finally {
        finishActiveBuild();
      }
    }

    function maybeStartNextBuild() {
      if (!state.currentBook) return;
      if (state.activeBuild) return;
      if (!Array.isArray(state.buildQueue) || !state.buildQueue.length) return;
      const serverBuilding = currentServerBuildingId();
      if (serverBuilding) {
        return;
      }
      const next = state.buildQueue.shift();
      if (!next) return;
      startBuildNow(next);
    }

    async function abortChapter(index) {
      if (!state.currentBook) return;
      const chapter = state.chapters[index];
      if (!chapter) return;
      const wasQueued = Boolean(chapter.build_status && chapter.build_status.state === 'queued');
      if (wasQueued) {
        dequeueBuild(chapter.id);
        chapter.build_status = null;
        setChapterStatusLabel(chapter.id, 'Pending', 'badge warning');
        renderChapters(summaryForChapters());
        if (state.currentChapterIndex === index) {
          statusLine.textContent = 'Build cancelled.';
        }
        maybeStartNextBuild();
        return;
      }
      const res = await fetch(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/abort`,
        { method: 'POST' }
      );
      if (!res.ok) {
        const detail = await readErrorResponse(res);
        if (res.status === 409) {
          chapter.build_status = null;
          renderChapters(summaryForChapters());
          refreshStatuses();
          return;
        }
        throw new Error(detail);
      }
      setChapterStatusLabel(chapter.id, 'Aborting…', 'badge warning');
      chapter.build_status = { state: 'aborting', was_queued: wasQueued };
      renderChapters(summaryForChapters());
      if (state.localBuilds) {
        state.localBuilds.delete(chapter.id);
      }
      if (state.activeBuild === chapter.id) {
        state.activeBuild = null;
      }
      if (state.currentChapterIndex === index) {
        statusLine.textContent = 'Aborting build...';
      }
      refreshStatuses();
      maybeStartNextBuild();
    }

    async function playChapter(index, { resumeTime = null } = {}) {
      if (!state.chapters.length || !state.currentBook) return;
      const chapter = state.chapters[index];
      if (!chapter) return;
      if (!chapter.mp3_exists) {
        statusLine.textContent = 'Audio not built yet.';
        alert('Build this chapter before playing.');
        state.autoAdvance = false;
        return;
      }
      state.autoAdvance = true;
      state.currentChapterIndex = index;
      updateChapterHighlight();
      updatePlayerDetails(chapter);
      updateBookmarkUI();
      statusLine.textContent = 'Loading audio...';

      const playUrl = `/api/books/${encodeURIComponent(state.currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/stream?ts=${Date.now()}`;
      player.pause();
      player.src = playUrl;
      player.load();
      resetProgressUI();
      updateMediaSession(chapter);
      if (Number.isFinite(resumeTime) && resumeTime >= 0) {
        ensureSeekAfterLoad(resumeTime, `Resumed at ${formatTimecode(resumeTime)}.`);
      }
      try {
        await player.play();
        if (Number.isFinite(resumeTime) && resumeTime > 0) {
          enforceResumeTarget(resumeTime);
        }
        statusLine.textContent = 'Playing';
      } catch {
        statusLine.textContent = 'Tap play to start audio.';
      }
    }

    function findPlayableChapterIndex(restart) {
      if (!state.chapters.length) return -1;
      if (!restart && state.currentChapterIndex >= 0) {
        for (let idx = state.currentChapterIndex; idx < state.chapters.length; idx++) {
          if (state.chapters[idx]?.mp3_exists) {
            return idx;
          }
        }
      }
      for (let idx = 0; idx < state.chapters.length; idx++) {
        if (state.chapters[idx]?.mp3_exists) {
          return idx;
        }
      }
      return -1;
    }

    async function resumeLastPlay() {
      const last = state.bookmarks.lastPlayed;
      if (
        !last ||
        typeof last.chapter !== 'string' ||
        !Number.isFinite(last.time)
      ) {
        alert('No last play saved yet.');
        return;
      }
      const index = state.chapters.findIndex(ch => ch.id === last.chapter);
      if (index === -1) {
        alert('Last played chapter is no longer available.');
        return;
      }
      await playChapter(index, { resumeTime: last.time });
      const targetChapter = chaptersList.querySelector(
        `[data-chapter-id="${state.chapters[index].id}"]`
      );
      if (targetChapter) {
        targetChapter.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }

    async function playBook(restart = false) {
      if (!state.chapters.length) return;
      const index = findPlayableChapterIndex(restart);
      if (index === -1) {
        alert('Build at least one chapter before playing.');
        return;
      }
      await playChapter(index);
    }

    function closeBookView(options = {}) {
      const navOptions =
        options && typeof options === 'object' ? options : {};
      const { skipHistory = false, replaceHistory = false } = navOptions;
      const hadSelection = Boolean(state.currentBook) || state.chapters.length > 0;
      stopStatusPolling();
      chaptersPanel.classList.add('hidden');
      state.chapters = [];
      state.currentBook = null;
      state.autoAdvance = false;
      state.currentChapterIndex = -1;
      state.media = null;
      setBookmarks({ manual: [], last_played: null });
      if (bookmarkPanel) {
        bookmarkPanel.classList.add('hidden');
      }
      player.pause();
      statusLine.textContent = 'Idle';
      player.removeAttribute('src');
      player.load();
      resetProgressUI();
      updatePlayToggleState();
      updateTransportAvailability();
      if (playerDock) {
        playerDock.classList.add('hidden');
        chaptersPanel.appendChild(playerDock);
      }
      updatePlayerDetails(null);
      applyVoiceDefaults(DEFAULT_VOICE, {});
      renderBooks();
      if (!skipHistory && hadSelection) {
        updateLocationFromState({ replace: replaceHistory });
      }
    }

    if (playerSeek) {
      const extractSeekValue = (event) => {
        if (event && event.target && typeof event.target.value !== 'undefined') {
          return Number(event.target.value);
        }
        return Number(playerSeek.value || 0);
      };
      const startScrub = () => {
        if (!playerSeek.disabled) {
          isScrubbing = true;
        }
      };
      const finishScrub = (event) => {
        if (playerSeek.disabled) return;
        commitSeekFromValue(extractSeekValue(event));
      };
      playerSeek.addEventListener('input', (event) => {
        if (playerSeek.disabled) return;
        isScrubbing = true;
        previewProgressFromValue(extractSeekValue(event));
      });
      playerSeek.addEventListener('change', (event) => {
        if (playerSeek.disabled) return;
        commitSeekFromValue(extractSeekValue(event));
      });
      if (typeof window !== 'undefined' && 'PointerEvent' in window) {
        playerSeek.addEventListener('pointerdown', startScrub);
        playerSeek.addEventListener('pointerup', finishScrub);
        playerSeek.addEventListener('pointercancel', () => {
          if (isScrubbing) {
            isScrubbing = false;
            updateProgressUI();
          }
        });
      } else {
        playerSeek.addEventListener('mousedown', startScrub);
        playerSeek.addEventListener('mouseup', finishScrub);
        playerSeek.addEventListener('touchstart', startScrub);
        playerSeek.addEventListener('touchend', finishScrub);
        playerSeek.addEventListener('touchcancel', () => {
          if (isScrubbing) {
            isScrubbing = false;
            updateProgressUI();
          }
        });
      }
      playerSeek.addEventListener('blur', () => {
        if (isScrubbing) {
          isScrubbing = false;
          updateProgressUI();
        }
      });
    }

    if (playerPlayToggle) {
      playerPlayToggle.addEventListener('click', () => {
        if (playerPlayToggle.disabled || !player) return;
        if (!player.getAttribute('src')) return;
        if (player.paused || player.ended) {
          const attempt = player.play();
          if (attempt && typeof attempt.catch === 'function') {
            attempt.catch(() => {
              statusLine.textContent = 'Tap play to start audio.';
            });
          }
        } else {
          player.pause();
        }
      });
    }
    if (playerRewindBtn) {
      playerRewindBtn.addEventListener('click', () => {
        if (playerRewindBtn.disabled) return;
        nudgePlayback(-SEEK_STEP);
        statusLine.textContent = `Rewound ${SEEK_STEP}s.`;
      });
    }
    if (playerForwardBtn) {
      playerForwardBtn.addEventListener('click', () => {
        if (playerForwardBtn.disabled) return;
        nudgePlayback(SEEK_STEP);
        statusLine.textContent = `Skipped ${SEEK_STEP}s.`;
      });
    }
    if (playerPrevBtn) {
      playerPrevBtn.addEventListener('click', () => {
        if (playerPrevBtn.disabled) return;
        const prevIndex = adjacentPlayableIndex(-1);
        const playbackPosition =
          player && Number.isFinite(player.currentTime) ? player.currentTime : 0;
        const shouldRestart =
          prevIndex === -1 ||
          playbackPosition >= PREVIOUS_CHAPTER_RESTART_THRESHOLD;
        if (shouldRestart) {
          const restarted = restartCurrentChapter();
          if (restarted || prevIndex === -1) {
            return;
          }
        }
        handlePromise(playChapter(prevIndex));
      });
    }
    if (playerNextBtn) {
      playerNextBtn.addEventListener('click', () => {
        if (playerNextBtn.disabled) return;
        const nextIndex = adjacentPlayableIndex(1);
        if (nextIndex === -1) return;
        handlePromise(playChapter(nextIndex));
      });
    }
    if (playerSpeedBtn) {
      playerSpeedBtn.addEventListener('click', () => {
        cyclePlaybackRate();
      });
    }
    if (playerAirPlayBtn) {
      const canAirPlay = Boolean(player && typeof player.webkitShowPlaybackTargetPicker === 'function');
      if (!canAirPlay) {
        playerAirPlayBtn.disabled = true;
        playerAirPlayBtn.title = 'AirPlay is only available in Safari.';
      } else {
        playerAirPlayBtn.addEventListener('click', () => {
          try {
            player.webkitShowPlaybackTargetPicker();
          } catch {
            statusLine.textContent = 'Unable to open AirPlay picker.';
          }
        });
      }
    }

    player.addEventListener('playing', () => {
      updatePlayToggleState();
      statusLine.textContent = 'Playing';
    });
    player.addEventListener('waiting', () => {
      statusLine.textContent = 'Buffering...';
    });
    player.addEventListener('timeupdate', () => {
      updateProgressUI();
      if (player.paused || player.seeking) return;
      scheduleLastPlaySync(player.currentTime);
    });
    player.addEventListener('pause', () => {
      updatePlayToggleState();
      updateProgressUI();
      if (!player.ended) {
        scheduleLastPlaySync(player.currentTime);
        statusLine.textContent = 'Paused';
      }
    });
    player.addEventListener('ended', () => {
      updatePlayToggleState();
      updateProgressUI();
      statusLine.textContent = 'Finished';
      recordCompletionProgress();
      renderChapters(summaryForChapters());
      if (state.autoAdvance) {
        const nextIndex = state.currentChapterIndex + 1;
        const nextChapter = state.chapters[nextIndex];
        if (nextChapter && nextChapter.mp3_exists) {
          handlePromise(playChapter(nextIndex));
          return;
        }
        state.autoAdvance = false;
        if (nextChapter && !nextChapter.mp3_exists) {
          statusLine.textContent = 'Next chapter not built yet.';
        }
      }
    });
    player.addEventListener('error', () => {
      statusLine.textContent = 'Playback error';
      updatePlayToggleState();
    });
    player.addEventListener('loadedmetadata', updateProgressUI);
    player.addEventListener('durationchange', updateProgressUI);
    player.addEventListener('seeked', () => {
      isScrubbing = false;
      updateProgressUI();
    });
    player.addEventListener('emptied', () => {
      resetProgressUI();
      updatePlayToggleState();
    });

    backButton.onclick = () => {
      closeBookView();
    };

    if (voiceSamplesRefreshBtn) {
      voiceSamplesRefreshBtn.onclick = () => {
        loadVoiceRoster({ force: state.voiceRosterLoaded });
      };
    }
    if (voiceSamplesLink) {
      voiceSamplesLink.onclick = () => {
        const search = window.location.search || '';
        const hash = window.location.hash || '';
        const target = isVoiceSamplesView()
          ? `/${search}${hash}`
          : `/voice-samples${search}${hash}`;
        window.location.href = target;
      };
    }

    voiceSaveBtn.onclick = () => {
      try {
        const payload = gatherVoicePayload();
        persistVoiceDefaults(payload).catch(err => {
          setVoiceStatus(err.message || 'Failed to save defaults.', true);
        });
      } catch (err) {
        setVoiceStatus(err.message || 'Invalid input', true);
      }
    };

    voiceResetBtn.onclick = () => {
      const payload = {
        speaker: null,
        speed: null,
        pitch: null,
        intonation: null,
      };
      persistVoiceDefaults(payload).catch(err => {
        setVoiceStatus(err.message || 'Failed to reset defaults.', true);
      });
    };

    window.addEventListener('popstate', () => {
      handlePromise(applyLibraryLocationFromUrl());
    });

    if (bookmarkAddBtn) {
      bookmarkAddBtn.onclick = () => {
        const chapter = currentChapter();
        if (!chapter) {
          alert('Select a chapter before adding bookmarks.');
          return;
        }
        if (!Number.isFinite(player.currentTime) || player.currentTime < 1) {
          alert('Play the audio to the desired position before bookmarking.');
          return;
        }
        handlePromise(createBookmarkForCurrent(player.currentTime, null));
      };
    }

    document.addEventListener('click', event => {
      const btn = event.target && event.target.closest('button[data-bookmark-id]');
      if (!btn) return;
      const bookmarkId = btn.dataset.bookmarkId || '';
      if (!bookmarkId) return;
      const entry = (state.bookmarks.manual || []).find(bm => bm && bm.id === bookmarkId);
      if (!entry) {
        statusLine.textContent = 'Bookmark not found.';
        return;
      }
      handlePromise(playBookmark(entry));
    });

    playBookBtn.onclick = () => handlePromise(resumeLastPlay());
    restartBookBtn.onclick = () => handlePromise(playBook(true));

    ensureUploadCard();
    renderUploadJobs();
    loadUploads();

    setBookmarks({ manual: [], last_played: null });

    applyViewMode();
    updateVoiceSamplesControls();
    renderVoiceSamples();

    if (isVoiceSamplesView()) {
      loadVoiceRoster();
    } else {
      renderLibraryNav();
      renderCollections();
      renderPendingEpubs();

      const initialLoadOptions = initialBookId
        ? { skipHistory: true }
        : { replaceHistory: true };
      loadBooks(initialLibraryPrefix || undefined, initialLoadOptions)
        .then(() => {
          if (!initialBookId) {
            return null;
          }
          return openBookById(initialBookId, { replaceHistory: true }).then(opened => {
            if (!opened) {
              updateLocationFromState({ replace: true });
            }
            return opened;
          });
        })
        .catch(err => {
          booksGrid.innerHTML = `<div style="color:var(--danger)">Failed to load books: ${err.message}</div>`;
        });
    }
  </script>
</body>
</html>
"""


def _normalize_library_path(value: str | None) -> str:
    if not value:
        return ""
    cleaned = str(value).replace("\\", "/").strip()
    if not cleaned:
        return ""
    cleaned = cleaned.strip("/")
    if not cleaned:
        return ""
    parts: list[str] = []
    for part in cleaned.split("/"):
        stripped = part.strip()
        if not stripped or stripped in {".", ".."}:
            continue
        parts.append(stripped)
    return "/".join(parts)


def _relative_library_path(
    root: Path,
    path: Path,
    *,
    allow_root: bool = False,
) -> str:
    resolved = path.expanduser().resolve()
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError("Path escapes the library root.") from exc
    if not relative.parts:
        if allow_root:
            return ""
        raise ValueError("Path resolves to the library root.")
    return relative.as_posix()


def _is_book_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    try:
        for entry in path.iterdir():
            if (
                entry.is_file()
                and entry.suffix == ".txt"
                and not entry.name.endswith(".original.txt")
            ):
                return True
    except OSError:
        return False
    return False


def _cover_url_for_book_dir(root: Path, book_dir: Path) -> str | None:
    metadata = load_book_metadata(book_dir)
    cover_path = None
    if metadata and metadata.cover_path and metadata.cover_path.exists():
        cover_path = metadata.cover_path
    if cover_path is None:
        cover_path = _fallback_cover_path(book_dir)
    if cover_path is None or not cover_path.exists():
        return None
    try:
        book_id = _relative_library_path(root, book_dir)
    except ValueError:
        return None
    return _cover_url(book_id, cover_path)


def _parse_voice_sample_filename(filename: str) -> tuple[int | None, str]:
    if not filename:
        return None, ""
    stem = filename[:-4] if filename.lower().endswith(".mp3") else filename
    if "-" in stem:
        prefix, remainder = stem.split("-", 1)
        if prefix.isdigit():
            return int(prefix), remainder or stem
    return None, stem


def _list_voice_samples(root: Path) -> list[dict[str, object]]:
    samples_dir = root / VOICE_SAMPLES_DIR
    if not samples_dir.exists() or not samples_dir.is_dir():
        return []
    try:
        entries = list(samples_dir.iterdir())
    except OSError:
        return []
    payload: list[dict[str, object]] = []
    for entry in entries:
        if not entry.is_file():
            continue
        if entry.suffix.lower() != ".mp3":
            continue
        sample_id, name = _parse_voice_sample_filename(entry.name)
        payload.append(
            {
                "id": sample_id,
                "name": name,
                "filename": entry.name,
                "url": f"/api/voice-samples/{quote(entry.name)}",
            }
        )
    payload.sort(
        key=lambda item: (
            item["id"] is None,
            item["id"] or 0,
            str(item["name"] or "").casefold(),
        )
    )
    return payload


def _voice_roster_cache_path(root: Path) -> Path:
    return root / VOICE_SAMPLES_DIR / VOICE_SAMPLES_ROSTER_FILENAME


def _voice_sample_index_path(character_dir: Path) -> Path:
    return character_dir / VOICE_SAMPLES_INDEX_FILENAME


def _read_voice_sample_index(
    index_path: Path,
) -> tuple[str | None, list[dict[str, object]]]:
    try:
        raw = index_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, []
    except OSError:
        return None, []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None, []
    if not isinstance(payload, dict):
        return None, []
    samples = payload.get("samples")
    if not isinstance(samples, list):
        return None, []
    character = payload.get("character")
    if not isinstance(character, str) or not character.strip():
        character = None
    return character, [entry for entry in samples if isinstance(entry, dict)]


def _write_voice_sample_index(
    index_path: Path,
    *,
    character: str | None,
    samples: list[dict[str, object]],
) -> None:
    payload = {
        "character": character,
        "updated_at": time.time(),
        "samples": samples,
    }
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _voice_sample_character_slug(character: str | None, speaker_id: int) -> str:
    if isinstance(character, str) and character.strip():
        slug = sanitize_voice_sample_name(character)
    else:
        slug = ""
    if not slug:
        slug = f"voice-{speaker_id}"
    return slug


def _voice_sample_signature(
    speaker_id: int,
    text: str,
    speed: float,
    pitch: float,
    intonation: float,
) -> str:
    payload = {
        "speaker": speaker_id,
        "text": text,
        "speed": speed,
        "pitch": pitch,
        "intonation": intonation,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _format_cached_sample_filename(
    speaker_id: int,
    speed: float,
    pitch: float,
    intonation: float,
    digest: str,
    *,
    width: int = 3,
) -> str:
    speed_label = f"{speed:.2f}"
    pitch_label = f"{pitch:.2f}"
    intonation_label = f"{intonation:.2f}"
    return (
        f"{speaker_id:0{width}d}-spd{speed_label}"
        f"-pit{pitch_label}-int{intonation_label}-{digest}.wav"
    )


def _resolve_cached_voice_sample(root: Path, sample_path: str) -> Path:
    if not sample_path:
        raise HTTPException(status_code=404, detail="Sample not found")
    samples_dir = (root / VOICE_SAMPLES_DIR).resolve()
    candidate = (samples_dir / sample_path).resolve()
    try:
        candidate.relative_to(samples_dir)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Sample not found") from exc
    if (
        not candidate.exists()
        or not candidate.is_file()
        or candidate.suffix.lower() != ".wav"
    ):
        raise HTTPException(status_code=404, detail="Sample not found")
    return candidate


def _list_cached_voice_samples(root: Path) -> list[dict[str, object]]:
    samples_dir = root / VOICE_SAMPLES_DIR
    if not samples_dir.exists() or not samples_dir.is_dir():
        return []
    payload: list[dict[str, object]] = []
    try:
        entries = list(samples_dir.iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.is_dir():
            continue
        index_path = _voice_sample_index_path(entry)
        character_name, samples = _read_voice_sample_index(index_path)
        if not samples:
            continue
        for sample in samples:
            filename = sample.get("filename")
            if not isinstance(filename, str) or not filename:
                continue
            file_path = entry / filename
            if not file_path.exists() or file_path.suffix.lower() != ".wav":
                continue
            sample_path = f"{entry.name}/{filename}"
            payload.append(
                {
                    "id": sample.get("id"),
                    "voice_id": sample.get("voice_id"),
                    "voice_name": sample.get("voice_name"),
                    "text": sample.get("text"),
                    "speed": sample.get("speed"),
                    "pitch": sample.get("pitch"),
                    "intonation": sample.get("intonation"),
                    "created_at": sample.get("created_at"),
                    "character": sample.get("character") or character_name,
                    "path": sample_path,
                    "filename": filename,
                    "url": f"/api/voice-samples/cached/{quote(entry.name)}/{quote(filename)}",
                }
            )
    payload.sort(
        key=lambda item: (
            item.get("created_at") is None,
            -(item.get("created_at") or 0),
            str(item.get("character") or "").casefold(),
            int(item.get("voice_id"))
            if isinstance(item.get("voice_id"), int)
            else 0,
        )
    )
    return payload


def _read_voice_roster_cache(
    cache_path: Path,
    *,
    engine_url: str | None = None,
) -> list[dict[str, object]] | None:
    try:
        raw = cache_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    if engine_url:
        cached_engine = payload.get("engine_url")
        if isinstance(cached_engine, str) and cached_engine != engine_url:
            return None
    roster = payload.get("characters")
    if not isinstance(roster, list):
        return None
    return roster


def _write_voice_roster_cache(
    cache_path: Path,
    *,
    engine_url: str,
    roster: list[dict[str, object]],
) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "engine_url": engine_url,
        "updated_at": time.time(),
        "characters": roster,
    }
    cache_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _voice_sample_defaults(config: PlayerConfig) -> dict[str, float]:
    return {
        "speed": VOICE_SAMPLE_DEFAULT_SPEED,
        "pitch": VOICE_SAMPLE_DEFAULT_PITCH,
        "intonation": VOICE_SAMPLE_DEFAULT_INTONATION,
    }


def _list_voice_roster(
    config: PlayerConfig,
    lock: threading.Lock,
) -> list[dict[str, object]]:
    with lock:
        runtime_hint = config.engine_runtime or discover_voicevox_runtime(
            config.engine_url
        )
        env_override, thread_override = _engine_thread_overrides(config.engine_threads)
        with managed_voicevox_runtime(
            runtime_hint,
            config.engine_url,
            readiness_timeout=config.engine_wait,
            extra_env=env_override,
            cpu_threads=thread_override,
        ):
            client = VoiceVoxClient(
                base_url=config.engine_url,
                timeout=60.0,
            )
            try:
                payload = client.list_speakers()
                return voice_roster_from_payload(payload)
            finally:
                client.close()


def _synthesize_voice_sample(
    config: PlayerConfig,
    lock: threading.Lock,
    *,
    speaker_id: int,
    text: str,
    speed: float,
    pitch: float,
    intonation: float,
) -> bytes:
    def _modify_query(payload: dict[str, object]) -> None:
        payload["speedScale"] = float(speed)
        payload["pitchScale"] = float(pitch)
        payload["intonationScale"] = float(intonation)

    with lock:
        runtime_hint = config.engine_runtime or discover_voicevox_runtime(
            config.engine_url
        )
        env_override, thread_override = _engine_thread_overrides(config.engine_threads)
        with managed_voicevox_runtime(
            runtime_hint,
            config.engine_url,
            readiness_timeout=config.engine_wait,
            extra_env=env_override,
            cpu_threads=thread_override,
        ):
            client = VoiceVoxClient(
                base_url=config.engine_url,
                speaker_id=speaker_id,
                timeout=60.0,
            )
            try:
                return client.synthesize_wav(text, modify_query=_modify_query)
            finally:
                client.close()


def _generate_voice_samples(
    config: PlayerConfig,
    output_dir: Path,
    lock: threading.Lock,
    *,
    overwrite: bool = False,
    sample_lines: list[str] | None = None,
) -> dict[str, object]:
    sample_text = build_sample_text(sample_lines)
    output_dir.mkdir(parents=True, exist_ok=True)

    with lock:
        runtime_hint = config.engine_runtime or discover_voicevox_runtime(
            config.engine_url
        )
        env_override, thread_override = _engine_thread_overrides(config.engine_threads)
        with managed_voicevox_runtime(
            runtime_hint,
            config.engine_url,
            readiness_timeout=config.engine_wait,
            extra_env=env_override,
            cpu_threads=thread_override,
        ):
            client = VoiceVoxClient(
                base_url=config.engine_url,
                timeout=60.0,
            )
            try:
                voices = voice_samples_from_payload(client.list_speakers())
                if not voices:
                    raise ValueError(
                        f"No VoiceVox speakers found at {config.engine_url}."
                    )
                max_id = max(speaker_id for speaker_id, _ in voices)
                width = max(3, len(str(max_id)))
                generated = 0
                skipped = 0
                for speaker_id, name in voices:
                    filename = format_voice_sample_filename(
                        speaker_id,
                        name,
                        width=width,
                    )
                    output_path = output_dir / filename
                    if output_path.exists() and not overwrite:
                        skipped += 1
                        continue
                    client.speaker_id = speaker_id
                    wav_bytes = client.synthesize_wav(sample_text)
                    wav_bytes_to_mp3(
                        wav_bytes,
                        output_path,
                        ffmpeg_path=config.ffmpeg_path,
                        overwrite=overwrite,
                    )
                    generated += 1
            finally:
                client.close()
    return {
        "count": len(voices),
        "generated": generated,
        "skipped": skipped,
        "output_dir": str(output_dir),
    }


def _scan_collection(
    root: Path,
    start: Path,
    *,
    cover_limit: int = 4,
) -> tuple[int, list[str]]:
    count = 0
    cover_samples: list[str] = []
    stack: list[Path] = [start]
    visited: set[Path] = set()
    while stack:
        current = stack.pop()
        try:
            resolved = current.resolve()
        except OSError:
            continue
        if resolved in visited:
            continue
        visited.add(resolved)
        try:
            entries = sorted(current.iterdir(), key=lambda p: p.name.casefold())
        except OSError:
            continue
        for entry in entries:
            if not entry.is_dir():
                continue
            try:
                entry.resolve().relative_to(root)
            except ValueError:
                continue
            if _is_book_dir(entry):
                count += 1
                if len(cover_samples) < cover_limit:
                    cover_url = _cover_url_for_book_dir(root, entry)
                    if cover_url:
                        cover_samples.append(cover_url)
            else:
                stack.append(entry)
    return count, cover_samples


def _list_collections(root: Path, base_dir: Path) -> list[dict[str, object]]:
    try:
        children = list(base_dir.iterdir())
    except OSError:
        return []
    entries: list[tuple[str, dict[str, object]]] = []
    for child in children:
        if not child.is_dir():
            continue
        if _is_book_dir(child):
            continue
        try:
            relative_id = _relative_library_path(root, child)
        except ValueError:
            continue
        book_count, cover_samples = _scan_collection(root, child, cover_limit=9)
        if book_count <= 0:
            continue
        payload = {
            "id": child.name,
            "name": child.name,
            "path": relative_id,
            "book_count": book_count,
            "cover_samples": cover_samples,
        }
        entries.append((child.name.casefold(), payload))
    entries.sort(key=lambda item: item[0])
    return [payload for _, payload in entries]


_INVALID_DIR_CHARS = set('<>:"/\\|?*')


def _sanitize_dir_fragment(name: str) -> str:
    if not isinstance(name, str):
        name = ""
    candidate = name.strip()
    if not candidate:
        candidate = "book"
    chars: list[str] = []
    for ch in candidate:
        if ch in _INVALID_DIR_CHARS:
            chars.append("_")
        elif ord(ch) < 32:
            continue
        else:
            chars.append(ch)
    sanitized = "".join(chars).strip(" .")
    if not sanitized:
        sanitized = "book"
    return sanitized[:120]


def _list_books(base_dir: Path, sort_mode: str) -> list[BookListing]:
    books: list[BookListing] = []
    for listing in list_books_sorted(base_dir, mode=sort_mode):
        path = listing.path
        if path.is_dir() and _is_book_dir(path):
            books.append(listing)
    return books


def _recently_played_books(
    root: Path,
    *,
    limit: int | None = None,
) -> list[tuple[Path, float]]:
    stack: list[Path] = [root]
    visited: set[Path] = set()
    entries: list[tuple[Path, float]] = []
    while stack:
        current = stack.pop()
        try:
            resolved = current.resolve()
        except OSError:
            continue
        if resolved in visited:
            continue
        visited.add(resolved)
        try:
            children = list(current.iterdir())
        except OSError:
            continue
        for child in children:
            if not child.is_dir():
                continue
            if _is_book_dir(child):
                bookmark_path = _bookmark_file(child)
                try:
                    stat = bookmark_path.stat()
                except OSError:
                    continue
                mtime = getattr(stat, "st_mtime", None)
                if not isinstance(mtime, (int, float)):
                    continue
                entries.append((child, float(mtime)))
                continue
            stack.append(child)
    entries.sort(key=lambda item: item[1], reverse=True)
    if limit is not None:
        return entries[:limit]
    return entries


def _epub_target_dir(root: Path, epub_path: Path) -> Path:
    parent = epub_path.parent
    try:
        parent.resolve().relative_to(root)
    except ValueError:
        parent = root
    sanitized = _sanitize_dir_fragment(epub_path.stem)
    candidate = parent / sanitized
    try:
        candidate.resolve().relative_to(root)
    except ValueError:
        candidate = root / sanitized
    return candidate


def _list_pending_epubs(root: Path, base_dir: Path) -> list[dict[str, object]]:
    try:
        children = sorted(
            base_dir.iterdir(),
            key=lambda entry: entry.name.casefold(),
        )
    except OSError:
        return []
    pending: list[dict[str, object]] = []
    for entry in children:
        if not entry.is_file() or entry.suffix.lower() != ".epub":
            continue
        try:
            rel_path = _relative_library_path(root, entry, allow_root=True)
        except ValueError:
            continue
        target_dir = _epub_target_dir(root, entry)
        if target_dir.exists() and _is_book_dir(target_dir):
            continue
        try:
            stat = entry.stat()
            modified = getattr(stat, "st_mtime", None)
            size = getattr(stat, "st_size", None)
        except OSError:
            modified = None
            size = None
        try:
            target_rel = _relative_library_path(root, target_dir, allow_root=True)
        except ValueError:
            target_rel = target_dir.name
        pending.append(
            {
                "path": rel_path,
                "filename": entry.name,
                "target_path": target_rel,
                "target_name": target_dir.name,
                "modified": modified,
                "size": size,
            }
        )
    return pending


def _list_chapters(book_dir: Path) -> list[Path]:
    return [
        p
        for p in sorted(book_dir.glob("*.txt"))
        if p.is_file()
        and not is_original_text_file(p)
        and not p.name.endswith(".partial.txt")
    ]


def _bookmark_file(book_dir: Path) -> Path:
    return book_dir / BOOKMARKS_FILENAME


def _empty_bookmark_state() -> dict[str, object]:
    return {
        "version": BOOKMARK_STATE_VERSION,
        "manual": [],
        "last_played": None,
        "played": [],
    }


def _load_bookmark_state(book_dir: Path) -> dict[str, object]:
    path = _bookmark_file(book_dir)
    if not path.exists():
        return _empty_bookmark_state()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _empty_bookmark_state()
    if not isinstance(raw, dict):
        return _empty_bookmark_state()
    manual_entries: list[dict[str, object]] = []
    seen_ids: set[str] = set()
    manual_payload = raw.get("manual")
    if isinstance(manual_payload, list):
        for entry in manual_payload:
            if not isinstance(entry, dict):
                continue
            chapter = entry.get("chapter")
            time_value = entry.get("time")
            if not isinstance(chapter, str):
                continue
            if not isinstance(time_value, (int, float)):
                continue
            entry_id = entry.get("id")
            if not isinstance(entry_id, str) or not entry_id.strip():
                entry_id = uuid.uuid4().hex
            if entry_id in seen_ids:
                continue
            seen_ids.add(entry_id)
            label = entry.get("label")
            if not isinstance(label, str):
                label = None
            created_at = entry.get("created_at")
            updated_at = entry.get("updated_at")
            manual_entries.append(
                {
                    "id": entry_id,
                    "chapter": chapter,
                    "time": float(time_value),
                    "label": label,
                    "created_at": created_at
                    if isinstance(created_at, (int, float))
                    else None,
                    "updated_at": updated_at
                    if isinstance(updated_at, (int, float))
                    else None,
                }
            )
    last_payload = None
    last_entry = raw.get("last_played")
    if isinstance(last_entry, dict):
        chapter = last_entry.get("chapter")
        time_value = last_entry.get("time")
        if isinstance(chapter, str) and isinstance(time_value, (int, float)):
            updated_at = last_entry.get("updated_at")
            last_payload = {
                "chapter": chapter,
                "time": float(time_value),
                "updated_at": updated_at
                if isinstance(updated_at, (int, float))
                else None,
            }
    played_entries: list[str] = []
    played_payload = raw.get("played")
    if isinstance(played_payload, list):
        seen_chapters: set[str] = set()
        for entry in played_payload:
            if not isinstance(entry, str):
                continue
            chapter = entry.strip()
            if not chapter or chapter in seen_chapters:
                continue
            seen_chapters.add(chapter)
            played_entries.append(chapter)
    return {
        "version": BOOKMARK_STATE_VERSION,
        "manual": manual_entries,
        "last_played": last_payload,
        "played": played_entries,
    }


def _save_bookmark_state(book_dir: Path, state: dict[str, object]) -> None:
    path = _bookmark_file(book_dir)
    path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _bookmarks_payload(book_dir: Path) -> dict[str, object]:
    state = _load_bookmark_state(book_dir)
    manual_entries = sorted(
        state.get("manual", []),
        key=lambda entry: (
            entry.get("chapter") or "",
            entry.get("time") if isinstance(entry.get("time"), (int, float)) else 0.0,
        ),
    )
    last_payload = state.get("last_played")
    if not (
        isinstance(last_payload, dict)
        and isinstance(last_payload.get("chapter"), str)
        and isinstance(last_payload.get("time"), (int, float))
    ):
        last_payload = None
    else:
        last_payload = {
            "chapter": last_payload["chapter"],
            "time": float(last_payload["time"]),
            "updated_at": last_payload.get("updated_at"),
        }
    played_entries = [
        entry
        for entry in state.get("played", [])
        if isinstance(entry, str) and entry
    ]
    return {
        "manual": manual_entries,
        "last_played": last_payload,
        "played": played_entries,
    }


def _ensure_chapter_for_bookmark(book_dir: Path, chapter_id: str) -> None:
    chapter_path = book_dir / chapter_id
    if (
        not chapter_path.exists()
        or not chapter_path.is_file()
        or chapter_path.suffix.lower() != ".txt"
        or chapter_path.name.endswith(".original.txt")
    ):
        raise HTTPException(status_code=404, detail="Chapter not found for bookmark.")


def _append_manual_bookmark(
    book_dir: Path,
    chapter_id: str,
    time_value: float,
    label: str | None = None,
) -> None:
    state = _load_bookmark_state(book_dir)
    now = time.time()
    entry = {
        "id": uuid.uuid4().hex,
        "chapter": chapter_id,
        "time": float(time_value),
        "label": label,
        "created_at": now,
        "updated_at": now,
    }
    manual_entries = state.get("manual")
    if not isinstance(manual_entries, list):
        manual_entries = []
    manual_entries.append(entry)
    state["manual"] = manual_entries
    _save_bookmark_state(book_dir, state)


def _remove_manual_bookmark(book_dir: Path, bookmark_id: str) -> bool:
    state = _load_bookmark_state(book_dir)
    manual_entries = state.get("manual")
    if not isinstance(manual_entries, list):
        return False
    filtered = [entry for entry in manual_entries if entry.get("id") != bookmark_id]
    if len(filtered) == len(manual_entries):
        return False
    state["manual"] = filtered
    _save_bookmark_state(book_dir, state)
    return True


def _update_manual_bookmark_label(
    book_dir: Path,
    bookmark_id: str,
    label: str | None,
) -> bool:
    state = _load_bookmark_state(book_dir)
    manual_entries = state.get("manual")
    if not isinstance(manual_entries, list):
        return False
    updated = False
    now = time.time()
    for entry in manual_entries:
        if entry.get("id") == bookmark_id:
            entry["label"] = label
            entry["updated_at"] = now
            updated = True
            break
    if updated:
        _save_bookmark_state(book_dir, state)
    return updated


def _update_last_played(book_dir: Path, chapter_id: str, time_value: float) -> None:
    state = _load_bookmark_state(book_dir)
    state["last_played"] = {
        "chapter": chapter_id,
        "time": float(time_value),
        "updated_at": time.time(),
    }
    _save_bookmark_state(book_dir, state)


def _mark_chapter_played(book_dir: Path, chapter_id: str) -> None:
    state = _load_bookmark_state(book_dir)
    played_entries = state.get("played")
    if not isinstance(played_entries, list):
        played_entries = []
    if chapter_id in played_entries:
        return
    played_entries.append(chapter_id)
    state["played"] = played_entries
    _save_bookmark_state(book_dir, state)


def _clear_last_played_entry(book_dir: Path) -> bool:
    state = _load_bookmark_state(book_dir)
    if not state.get("last_played"):
        return False
    state["last_played"] = None
    _save_bookmark_state(book_dir, state)
    return True


def _safe_read_int(path: Path) -> int | None:
    try:
        content = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    try:
        return int(content)
    except ValueError:
        return None


def _engine_thread_overrides(
    threads: int | None,
) -> tuple[dict[str, str] | None, int | None]:
    if threads is None or threads <= 0:
        return None, None
    clamped = max(1, int(threads))
    env = {
        "VOICEVOX_CPU_NUM_THREADS": str(clamped),
        "RAYON_NUM_THREADS": str(clamped),
    }
    return env, clamped


def _read_original_title_from_file(chapter_path: Path) -> str | None:
    original_path = chapter_path.with_suffix(".original.txt")
    try:
        with original_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if stripped:
                    return stripped
    except FileNotFoundError:
        return None
    except OSError:
        return None
    return None


def _populate_target_metadata(
    target: TTSTarget,
    book_path: Path,
    metadata: LoadedBookMetadata | None,
) -> None:
    book_title = book_path.name
    book_author = book_title
    cover_path = None
    if metadata:
        if metadata.title:
            book_title = metadata.title
        if metadata.author:
            book_author = metadata.author
        cover_path = metadata.cover_path
    if cover_path is None:
        cover_path = _fallback_cover_path(book_path)
    if cover_path is not None and cover_path.exists():
        target.cover_image = cover_path
    target.book_title = book_title
    target.book_author = book_author
    chapter_meta = metadata.chapters.get(target.source.name) if metadata else None
    if chapter_meta:
        if chapter_meta.title:
            target.chapter_title = chapter_meta.title
        if chapter_meta.original_title:
            target.original_title = chapter_meta.original_title
        if chapter_meta.index is not None:
            target.track_number = chapter_meta.index
        if metadata and metadata.chapters:
            target.track_total = len(metadata.chapters)
    else:
        if not target.track_total:
            all_chapters = _list_chapters(book_path)
            if all_chapters:
                target.track_total = len(all_chapters)
        if target.track_number is None:
            target.track_number = _parse_track_number_from_name(target.source.stem)
    if not target.chapter_title:
        target.chapter_title = target.source.stem
    if not target.original_title:
        target.original_title = _read_original_title_from_file(target.source)
    if not target.original_title:
        target.original_title = target.chapter_title


def _chapter_state(
    chapter_path: Path,
    config: PlayerConfig,
    index: int,
    *,
    chapter_meta: ChapterMetadata | None = None,
    build_status: dict[str, object] | None = None,
) -> dict[str, object]:
    target = TTSTarget(
        source=chapter_path,
        text_path=None,
        output=chapter_path.with_suffix(".mp3"),
    )
    cache_dir = _target_cache_dir(config.cache_dir, target)
    total_chunks = _safe_read_int(cache_dir / ".complete")
    track_number = index
    if chapter_meta and chapter_meta.index is not None:
        track_number = chapter_meta.index
    title = chapter_path.stem
    original_title = None
    if chapter_meta:
        if chapter_meta.title:
            title = chapter_meta.title
        if chapter_meta.original_title:
            original_title = chapter_meta.original_title
    if not original_title:
        original_title = _read_original_title_from_file(chapter_path)
    state: dict[str, object] = {
        "id": chapter_path.name,
        "title": title,
        "original_title": original_title,
        "index": index,
        "track_number": track_number,
        "mp3_exists": target.output.exists(),
        "has_cache": cache_dir.exists(),
        "total_chunks": total_chunks,
    }
    if state["mp3_exists"]:
        try:
            state["mp3_mtime"] = target.output.stat().st_mtime
        except OSError:
            state["mp3_mtime"] = None
    if build_status:
        state["build_status"] = build_status
    return state


def _fallback_cover_path(book_dir: Path) -> Path | None:
    for ext in COVER_EXTENSIONS:
        candidate = book_dir / f"cover{ext}"
        if candidate.exists():
            return candidate
    return None


def _book_media_info(
    book_dir: Path,
    config: PlayerConfig,
    metadata: LoadedBookMetadata | None = None,
) -> tuple[
    LoadedBookMetadata | None,
    str,
    str | None,
    Path | None,
    dict[str, float | int],
    dict[str, float],
]:
    metadata = metadata or load_book_metadata(book_dir)
    title = metadata.title if metadata and metadata.title else book_dir.name
    author = metadata.author if metadata else None
    cover_path = None
    if metadata and metadata.cover_path and metadata.cover_path.exists():
        cover_path = metadata.cover_path
    if cover_path is None:
        cover_path = _fallback_cover_path(book_dir)
    saved_defaults, effective_defaults = _tts_defaults_payload(metadata, config)
    return metadata, title, author, cover_path, saved_defaults, effective_defaults


def _cover_url(book_id: str, cover_path: Path | None) -> str | None:
    if not cover_path or not cover_path.exists():
        return None
    try:
        mtime = int(cover_path.stat().st_mtime)
    except OSError:
        mtime = 0
    encoded_id = quote(book_id, safe="/")
    return f"/api/books/{encoded_id}/cover?ts={mtime}"


def _voice_settings_for_book(
    config: PlayerConfig,
    metadata: LoadedBookMetadata | None,
) -> dict[str, float | int | None]:
    defaults = metadata.tts_defaults if metadata else None

    def _pick(
        attr: str,
        cfg_value: float | int | None,
        fallback: float | int,
    ) -> float | int:
        if defaults:
            value = getattr(defaults, attr, None)
            if value is not None:
                return value
        if cfg_value is not None:
            return cfg_value
        return fallback

    speaker_value = _pick("speaker", config.speaker, DEFAULT_SPEAKER_ID)
    if speaker_value is None:
        speaker_value = DEFAULT_SPEAKER_ID
    return {
        "speaker": int(speaker_value),
        "speed": _pick("speed", config.speed_scale, DEFAULT_SPEED_SCALE),
        "pitch": _pick("pitch", config.pitch_scale, DEFAULT_PITCH_SCALE),
        "intonation": _pick(
            "intonation",
            config.intonation_scale,
            DEFAULT_INTONATION_SCALE,
        ),
    }


def _tts_defaults_payload(
    metadata: LoadedBookMetadata | None,
    config: PlayerConfig,
) -> tuple[dict[str, float | int], dict[str, float | int | None]]:
    saved: dict[str, float | int] = {}
    defaults = metadata.tts_defaults if metadata else None
    if defaults:
        if defaults.speaker is not None:
            saved["speaker"] = defaults.speaker
        if defaults.speed is not None:
            saved["speed"] = defaults.speed
        if defaults.pitch is not None:
            saved["pitch"] = defaults.pitch
        if defaults.intonation is not None:
            saved["intonation"] = defaults.intonation
    effective = _voice_settings_for_book(config, metadata)
    return saved, effective


def _synthesize_sequence(
    config: PlayerConfig,
    targets: list[TTSTarget],
    lock: threading.Lock,
    *,
    force_indices: frozenset[int] | None = None,
    progress_handler: Callable[[TTSTarget, dict[str, object]], None] | None = None,
    voice_settings: dict[str, float | int | None] | None = None,
    cancel_event: threading.Event | None = None,
) -> int:
    if not targets:
        return 0
    force_set = frozenset() if force_indices is None else force_indices
    work_plan: list[tuple[int, TTSTarget, bool]] = []
    for idx, target in enumerate(targets):
        force = idx in force_set
        if force or not target.output.exists():
            work_plan.append((idx, target, force))
    if not work_plan:
        return 0

    if cancel_event and cancel_event.is_set():
        raise KeyboardInterrupt

    with lock:
        runtime_hint = config.engine_runtime or discover_voicevox_runtime(
            config.engine_url
        )
        env_override, thread_override = _engine_thread_overrides(config.engine_threads)
        with managed_voicevox_runtime(
            runtime_hint,
            config.engine_url,
            readiness_timeout=config.engine_wait,
            extra_env=env_override,
            cpu_threads=thread_override,
        ):
            voice = voice_settings or {}
            speaker_value = voice.get("speaker", config.speaker)
            if isinstance(speaker_value, float):
                speaker_value = int(speaker_value)
            elif not isinstance(speaker_value, int):
                speaker_value = config.speaker
            client = VoiceVoxClient(
                base_url=config.engine_url,
                speaker_id=speaker_value,
                timeout=60.0,
                post_phoneme_length=config.pause,
                speed_scale=voice.get("speed", config.speed_scale),
                pitch_scale=voice.get("pitch", config.pitch_scale),
                intonation_scale=voice.get("intonation", config.intonation_scale),
            )
            try:
                total = len(work_plan)
                for order, (_, target, force) in enumerate(work_plan, start=1):
                    if cancel_event and cancel_event.is_set():
                        raise KeyboardInterrupt
                    if force:
                        target.output.unlink(missing_ok=True)
                        cache_dir = _target_cache_dir(config.cache_dir, target)
                        shutil.rmtree(cache_dir, ignore_errors=True)
                    progress_callback = None
                    if progress_handler is not None:

                        def _adapter(
                            event: dict[str, object], current_target=target
                        ) -> None:
                            progress_handler(current_target, event)

                        progress_callback = _adapter
                    try:
                        _synthesize_target_with_client(
                            target,
                            client,
                            index=order,
                            total=total,
                            ffmpeg_path=config.ffmpeg_path,
                            overwrite=False,
                            progress=progress_callback,
                            cache_base=config.cache_dir,
                            keep_cache=config.keep_cache,
                            cancel_event=cancel_event,
                        )
                    except Exception as exc:
                        if progress_handler is not None:
                            progress_handler(
                                target, {"event": "target_error", "error": str(exc)}
                            )
                        raise
            finally:
                client.close()
    return len(work_plan)


def create_app(config: PlayerConfig, *, reader_url: str | None = None) -> FastAPI:
    root = config.root.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Books root not found: {root}")

    app = FastAPI(title="nk VoiceVox")
    app.state.config = config
    app.state.root = root
    app.state.reader_url = reader_url
    app.state.voicevox_lock = threading.Lock()
    upload_manager = UploadManager(root)
    app.state.upload_manager = upload_manager
    app.add_event_handler("shutdown", upload_manager.shutdown)

    status_lock = threading.Lock()
    chapter_status: dict[str, dict[str, dict[str, object]]] = {}
    bookmark_lock = threading.Lock()
    build_lock = threading.Lock()
    active_build_jobs: dict[tuple[str, str], dict[str, object]] = {}

    def _epub_source_for_book(
        book_dir: Path, metadata: LoadedBookMetadata | None
    ) -> Path | None:
        candidates: list[Path] = []
        seen: set[Path] = set()
        book_name = book_dir.name.casefold()

        def _add(path: Path) -> None:
            try:
                resolved = path.resolve()
            except OSError:
                return
            if resolved in seen:
                return
            seen.add(resolved)
            candidates.append(resolved)

        # Explicit metadata hint
        if metadata and isinstance(metadata.source_epub, str) and metadata.source_epub:
            hint_path = Path(metadata.source_epub)
            if hint_path.is_absolute():
                _add(hint_path)
            else:
                _add(book_dir / hint_path)
                _add(book_dir.parent / hint_path)

        # Any epub inside the book directory
        try:
            for child in book_dir.iterdir():
                if child.is_file() and child.suffix.lower() == ".epub":
                    _add(child)
        except OSError:
            pass

        # A sibling epub with the same stem as the book directory
        try:
            for child in book_dir.parent.iterdir():
                if not child.is_file():
                    continue
                if child.suffix.lower() != ".epub":
                    continue
                if child.stem.casefold() != book_name:
                    continue
                _add(child)
        except OSError:
            pass

        for candidate in candidates:
            try:
                if candidate.exists() and candidate.is_file():
                    return candidate
            except OSError:
                continue
        return None

    def _resolve_prefix(prefix: str | None) -> tuple[str, Path]:
        normalized = _normalize_library_path(prefix)
        candidate = (root if not normalized else root / normalized).resolve()
        try:
            canonical = _relative_library_path(root, candidate, allow_root=True)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail="Folder not found") from exc
        if not candidate.exists() or not candidate.is_dir():
            raise HTTPException(status_code=404, detail="Folder not found")
        return canonical, candidate

    def _resolve_book(book_id: str) -> tuple[str, Path]:
        normalized = _normalize_library_path(book_id)
        if not normalized:
            raise HTTPException(status_code=404, detail="Book not found")
        candidate = (root / normalized).resolve()
        try:
            canonical = _relative_library_path(root, candidate)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail="Book not found") from exc
        if (
            not candidate.exists()
            or not candidate.is_dir()
            or not _is_book_dir(candidate)
        ):
            raise HTTPException(status_code=404, detail="Book not found")
        return canonical, candidate

    def _resolve_voice_sample(sample_name: str) -> Path:
        if not sample_name:
            raise HTTPException(status_code=404, detail="Sample not found")
        samples_dir = (root / VOICE_SAMPLES_DIR).resolve()
        candidate = (samples_dir / sample_name).resolve()
        try:
            candidate.relative_to(samples_dir)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail="Sample not found") from exc
        if (
            not candidate.exists()
            or not candidate.is_file()
            or candidate.suffix.lower() != ".mp3"
        ):
            raise HTTPException(status_code=404, detail="Sample not found")
        return candidate

    def _book_id_from_target(target: TTSTarget) -> str:
        try:
            return _relative_library_path(root, target.source.parent)
        except ValueError:
            return target.source.parent.name

    def _book_payload(
        book_dir: Path,
        *,
        metadata: LoadedBookMetadata | None = None,
    ) -> dict[str, object] | None:
        try:
            book_id = _relative_library_path(root, book_dir)
        except ValueError:
            return None
        (
            metadata,
            book_title,
            book_author,
            cover_path,
            _,
            _,
        ) = _book_media_info(book_dir, config, metadata=metadata)
        epub_source = _epub_source_for_book(book_dir, metadata)
        chapters = _list_chapters(book_dir)
        status_snapshot = _status_snapshot(book_id)
        states = []
        for idx, chapter in enumerate(chapters):
            states.append(
                _chapter_state(
                    chapter,
                    config,
                    idx + 1,
                    chapter_meta=metadata.chapters.get(chapter.name)
                    if metadata
                    else None,
                    build_status=status_snapshot.get(chapter.name),
                )
            )
        total = len(states)
        completed = sum(1 for st in states if st["mp3_exists"])
        pending = total - completed
        payload: dict[str, object] = {
            "id": book_id,
            "path": book_id,
            "title": book_title,
            "total_chapters": total,
            "completed_chapters": completed,
            "pending_chapters": pending,
        }
        if book_author:
            payload["author"] = book_author
        cover_url = _cover_url(book_id, cover_path)
        if cover_url:
            payload["cover_url"] = cover_url
        if epub_source:
            try:
                epub_rel = _relative_library_path(root, epub_source, allow_root=True)
            except ValueError:
                epub_rel = epub_source.name
            payload["epub_path"] = epub_rel
        return payload

    def _set_chapter_status(
        book_id: str,
        chapter_id: str,
        *,
        state: str | None = None,
        chunk_index: int | None = None,
        chunk_count: int | None = None,
        message: str | None = None,
        error: str | None = None,
    ) -> None:
        now = time.time()
        with status_lock:
            book_entry = chapter_status.setdefault(book_id, {})
            entry = book_entry.get(chapter_id)
            if entry is None:
                entry = {}
            if state is not None:
                entry["state"] = state
            if chunk_index is not None:
                entry["chunk_index"] = int(chunk_index)
            if chunk_count is not None:
                entry["chunk_count"] = int(chunk_count)
            if message is not None:
                entry["message"] = message
            if error is not None:
                entry["error"] = error
            entry.setdefault("started_at", now)
            entry["updated_at"] = now
            book_entry[chapter_id] = entry

    def _clear_chapter_status(book_id: str, chapter_id: str) -> None:
        with status_lock:
            book_entry = chapter_status.get(book_id)
            if not book_entry:
                return
            book_entry.pop(chapter_id, None)
            if not book_entry:
                chapter_status.pop(book_id, None)

    def _status_snapshot(book_id: str) -> dict[str, dict[str, object]]:
        with status_lock:
            book_entry = chapter_status.get(book_id)
            if not book_entry:
                return {}
            return {
                chapter_id: entry.copy() for chapter_id, entry in book_entry.items()
            }

    def _record_progress_event(target: TTSTarget, event: dict[str, object]) -> None:
        book_id = _book_id_from_target(target)
        chapter_id = target.source.name
        event_type = event.get("event")
        if event_type == "target_start":
            chunk_count = event.get("chunk_count")
            chunk_total = int(chunk_count) if isinstance(chunk_count, int) else None
            _set_chapter_status(
                book_id,
                chapter_id,
                state="building",
                chunk_index=0,
                chunk_count=chunk_total,
                message=None,
                error=None,
            )
        elif event_type == "chunk_start":
            chunk_index = event.get("chunk_index")
            chunk_count = event.get("chunk_count")
            _set_chapter_status(
                book_id,
                chapter_id,
                state="building",
                chunk_index=int(chunk_index) if isinstance(chunk_index, int) else None,
                chunk_count=int(chunk_count) if isinstance(chunk_count, int) else None,
            )
        elif event_type == "target_done":
            _clear_chapter_status(book_id, chapter_id)
        elif event_type == "target_skipped":
            _clear_chapter_status(book_id, chapter_id)
        elif event_type == "target_error":
            message = str(event.get("error") or "unknown error")
            _set_chapter_status(
                book_id,
                chapter_id,
                state="error",
                message=message,
                error=message,
            )

    def _render_player(view: str | None = None) -> HTMLResponse:
        payload = {"reader_url": getattr(app.state, "reader_url", None)}
        if view:
            payload["view"] = view
        config_blob = json.dumps(payload, ensure_ascii=False)
        html = INDEX_HTML.replace("__NK_PLAYER_CONFIG__", config_blob).replace(
            "__NK_FAVICON__", NK_FAVICON_URL
        )
        return HTMLResponse(html)

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return _render_player()

    @app.get("/voice-samples", response_class=HTMLResponse)
    def voice_samples_page() -> HTMLResponse:
        return _render_player("voice-samples")

    @app.get("/apple-touch-icon.png")
    def apple_touch_icon() -> Response:
        return Response(content=NK_APPLE_TOUCH_ICON_PNG, media_type="image/png")

    @app.get("/api/uploads")
    def api_uploads() -> JSONResponse:
        jobs = upload_manager.list_jobs()
        return JSONResponse({"jobs": jobs})

    @app.post("/api/uploads")
    async def api_upload_epub(
        file: UploadFile = File(...),
        prefix: str | None = Form(
            None, description="Relative folder under the library root."
        ),
    ) -> JSONResponse:
        filename = file.filename or "upload.epub"
        if Path(filename).suffix.lower() != ".epub":
            raise HTTPException(
                status_code=400, detail="Only .epub files are supported."
            )
        target_parent = root
        if prefix:
            _, target_parent = _resolve_prefix(prefix)
        sanitized_name = _sanitize_dir_fragment(Path(filename).stem)
        target_dir = target_parent / sanitized_name
        job = UploadJob(root, filename, output_dir=target_dir)
        try:
            with job.temp_path.open("wb") as destination:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    destination.write(chunk)
        except Exception as exc:
            job.cleanup()
            raise HTTPException(
                status_code=500, detail=f"Failed to save upload: {exc}"
            ) from exc
        finally:
            await file.close()
        upload_manager.enqueue(job)
        return JSONResponse({"job": job.to_payload()})

    @app.post("/api/epubs/chapterize")
    def api_chapterize_epubs(payload: dict[str, object] = Body(...)) -> JSONResponse:
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        paths_payload = payload.get("paths")
        if not isinstance(paths_payload, list) or not paths_payload:
            raise HTTPException(
                status_code=400,
                detail="paths must be a non-empty list.",
            )
        queued: list[dict[str, object]] = []
        skipped: list[dict[str, object]] = []
        seen: set[str] = set()
        for raw_entry in paths_payload:
            if not isinstance(raw_entry, str):
                skipped.append({"path": raw_entry, "reason": "Invalid path entry."})
                continue
            normalized = _normalize_library_path(raw_entry)
            if not normalized:
                skipped.append({"path": raw_entry, "reason": "Invalid path."})
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            candidate = root / normalized
            try:
                relative_path = _relative_library_path(root, candidate, allow_root=True)
            except ValueError:
                skipped.append({"path": normalized, "reason": "Path escapes root."})
                continue
            if candidate.suffix.lower() != ".epub":
                skipped.append({"path": relative_path, "reason": "Not an EPUB file."})
                continue
            if not candidate.is_file():
                skipped.append({"path": relative_path, "reason": "File not found."})
                continue
            target_dir = _epub_target_dir(root, candidate)
            if target_dir.exists() and _is_book_dir(target_dir):
                skipped.append(
                    {
                        "path": relative_path,
                        "reason": "Book already chapterized.",
                    }
                )
                continue
            try:
                target_rel = _relative_library_path(root, target_dir, allow_root=True)
            except ValueError:
                target_rel = target_dir.name
            job = UploadJob(
                root,
                candidate.name,
                source_path=candidate,
                output_dir=target_dir,
            )
            upload_manager.enqueue(job)
            queued.append({"path": relative_path, "target": target_rel})
        if not queued:
            return JSONResponse({"queued": [], "skipped": skipped})
        return JSONResponse({"queued": queued, "skipped": skipped})

    @app.get("/api/books")
    def api_books(
        prefix: str | None = Query(
            None, description="Relative folder under the library root."
        ),
        sort: str | None = Query(
            None, description="Sort order: author, recent, or played"
        ),
    ) -> JSONResponse:
        sort_mode = _normalize_sort_mode(sort)
        normalized_prefix = _normalize_library_path(prefix)
        if normalized_prefix == RECENTLY_PLAYED_PREFIX:
            recent_entries = _recently_played_books(root)
            books_payload: list[dict[str, object]] = []
            last_played_payload: dict[str, object] | None = None
            for book_dir, _ in recent_entries:
                payload = _book_payload(book_dir)
                if payload:
                    books_payload.append(payload)
            if recent_entries:
                last_dir, last_ts = recent_entries[0]
                last_played_payload = _book_payload(last_dir)
                if last_played_payload is not None:
                    last_played_payload["last_played_at"] = float(last_ts)
            return JSONResponse(
                {
                    "prefix": RECENTLY_PLAYED_PREFIX,
                    "parent_prefix": "",
                    "collections": [],
                    "books": books_payload,
                    "pending_epubs": [],
                    "last_played_book": last_played_payload,
                }
            )
        prefix_value, prefix_path = _resolve_prefix(normalized_prefix)
        parent_prefix = ""
        if prefix_value:
            parent_prefix = (
                prefix_value.rsplit("/", 1)[0] if "/" in prefix_value else ""
            )
        collections_payload = _list_collections(root, prefix_path)
        pending_epubs = _list_pending_epubs(root, prefix_path)
        last_played_payload: dict[str, object] | None = None
        if not prefix_value:
            recent_entries = _recently_played_books(root)
            if recent_entries:
                first_dir, first_ts = recent_entries[0]
                last_played_payload = _book_payload(first_dir)
                if last_played_payload is not None:
                    last_played_payload["last_played_at"] = float(first_ts)
                cover_samples: list[str] = []
                for book_dir, _ in recent_entries[1:]:
                    if len(cover_samples) >= 9:
                        break
                    cover_url = _cover_url_for_book_dir(root, book_dir)
                    if cover_url:
                        cover_samples.append(cover_url)
                recent_payload = {
                    "id": RECENTLY_PLAYED_PREFIX,
                    "name": RECENTLY_PLAYED_LABEL,
                    "path": RECENTLY_PLAYED_PREFIX,
                    "book_count": len(recent_entries),
                    "cover_samples": cover_samples,
                    "virtual": True,
                }
                collections_payload = [recent_payload, *collections_payload]
        books_payload = []
        for listing in _list_books(prefix_path, sort_mode):
            payload = _book_payload(listing.path, metadata=listing.metadata)
            if payload:
                books_payload.append(payload)
        return JSONResponse(
            {
                "prefix": prefix_value,
                "parent_prefix": parent_prefix,
                "collections": collections_payload,
                "books": books_payload,
                "pending_epubs": pending_epubs,
                "last_played_book": last_played_payload,
            }
        )

    @app.get("/api/voice-samples/status")
    def api_voice_samples_status() -> JSONResponse:
        count = len(_list_voice_samples(root))
        return JSONResponse({"count": count, "has_samples": count > 0})

    @app.get("/api/voice-samples/cache")
    def api_voice_samples_cache() -> JSONResponse:
        samples = _list_cached_voice_samples(root)
        return JSONResponse({"samples": samples, "count": len(samples)})

    @app.post("/api/voice-samples/cache")
    def api_generate_cached_voice_sample(
        payload: dict[str, object] | None = Body(None),
    ) -> JSONResponse:
        data = payload or {}
        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        speaker_value = data.get("speaker", data.get("voice_id"))
        if isinstance(speaker_value, bool) or not isinstance(speaker_value, int):
            raise HTTPException(status_code=400, detail="speaker must be an integer.")
        if speaker_value <= 0:
            raise HTTPException(
                status_code=400, detail="speaker must be a positive integer."
            )
        character = data.get("character")
        if not isinstance(character, str) or not character.strip():
            character = None
        voice_name = data.get("voice_name")
        if not isinstance(voice_name, str) or not voice_name.strip():
            voice_name = None
        defaults = _voice_sample_defaults(config)

        def _parse_scale(key: str) -> float:
            if key not in data or data[key] is None:
                return float(defaults[key])
            value = data[key]
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise HTTPException(
                    status_code=400, detail=f"{key} must be numeric."
                )
            return float(value)

        speed = _parse_scale("speed")
        pitch = _parse_scale("pitch")
        intonation = _parse_scale("intonation")

        sample_lines = None
        if "text" in data:
            text_value = data.get("text")
            if text_value is None:
                sample_lines = None
            elif isinstance(text_value, list):
                sample_lines = [str(item) for item in text_value]
            elif isinstance(text_value, str):
                sample_lines = text_value.splitlines()
            else:
                raise HTTPException(
                    status_code=400, detail="text must be a string or list of strings."
                )
        try:
            sample_text = build_sample_text(sample_lines)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        character_slug = _voice_sample_character_slug(character, int(speaker_value))
        character_dir = root / VOICE_SAMPLES_DIR / character_slug
        index_path = _voice_sample_index_path(character_dir)
        existing_character, samples = _read_voice_sample_index(index_path)
        signature = _voice_sample_signature(
            int(speaker_value),
            sample_text,
            speed,
            pitch,
            intonation,
        )
        filename = _format_cached_sample_filename(
            int(speaker_value),
            speed,
            pitch,
            intonation,
            signature,
        )
        output_path = character_dir / filename
        cached = False
        if output_path.exists():
            cached = True
        else:
            try:
                wav_bytes = _synthesize_voice_sample(
                    config,
                    app.state.voicevox_lock,
                    speaker_id=int(speaker_value),
                    text=sample_text,
                    speed=speed,
                    pitch=pitch,
                    intonation=intonation,
                )
            except (
                VoiceVoxUnavailableError,
                VoiceVoxError,
                VoiceVoxRuntimeError,
                FileNotFoundError,
            ) as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            character_dir.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(wav_bytes)

        existing_entry = None
        for sample in samples:
            if sample.get("id") == signature or sample.get("filename") == filename:
                existing_entry = sample
                break
        created_at = existing_entry.get("created_at") if isinstance(
            existing_entry, dict
        ) else None
        if not isinstance(created_at, (int, float)):
            created_at = time.time()
        entry = {
            "id": signature,
            "voice_id": int(speaker_value),
            "voice_name": voice_name or (existing_entry or {}).get("voice_name"),
            "text": sample_text,
            "speed": speed,
            "pitch": pitch,
            "intonation": intonation,
            "created_at": created_at,
            "character": character or existing_character or (existing_entry or {}).get("character"),
            "filename": filename,
        }
        updated_samples = [entry]
        for sample in samples:
            if sample.get("id") == signature:
                continue
            if sample.get("filename") == filename:
                continue
            updated_samples.append(sample)
        _write_voice_sample_index(
            index_path,
            character=character or existing_character,
            samples=updated_samples,
        )
        entry_payload = {
            **entry,
            "path": f"{character_slug}/{filename}",
            "url": f"/api/voice-samples/cached/{quote(character_slug)}/{quote(filename)}",
        }
        return JSONResponse({"sample": entry_payload, "cached": cached})

    @app.get("/api/voice-samples/voices")
    def api_voice_samples_voices(
        refresh: bool = Query(False, description="Refresh cached voice roster."),
    ) -> JSONResponse:
        cache_path = _voice_roster_cache_path(root)
        roster = None if refresh else _read_voice_roster_cache(cache_path)
        used_cache = roster is not None
        if roster is None:
            try:
                roster = _list_voice_roster(config, app.state.voicevox_lock)
                _write_voice_roster_cache(
                    cache_path,
                    engine_url=config.engine_url,
                    roster=roster,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except (
                VoiceVoxUnavailableError,
                VoiceVoxError,
                VoiceVoxRuntimeError,
                FileNotFoundError,
            ) as exc:
                cached_fallback = _read_voice_roster_cache(cache_path)
                if cached_fallback is None:
                    raise HTTPException(status_code=502, detail=str(exc)) from exc
                roster = cached_fallback
                used_cache = True
        try:
            sample_text = build_sample_text()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(
            {
                "characters": roster,
                "defaults": _voice_sample_defaults(config),
                "sample_text": sample_text,
                "cached": used_cache,
            }
        )

    @app.post("/api/voice-samples/preview")
    def api_voice_samples_preview(
        payload: dict[str, object] | None = Body(None),
    ) -> Response:
        data = payload or {}
        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        speaker_value = data.get("speaker", data.get("voice_id"))
        if isinstance(speaker_value, bool) or not isinstance(speaker_value, int):
            raise HTTPException(status_code=400, detail="speaker must be an integer.")
        if speaker_value <= 0:
            raise HTTPException(
                status_code=400, detail="speaker must be a positive integer."
            )
        defaults = _voice_sample_defaults(config)

        def _parse_scale(key: str) -> float:
            if key not in data or data[key] is None:
                return float(defaults[key])
            value = data[key]
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise HTTPException(
                    status_code=400, detail=f"{key} must be numeric."
                )
            return float(value)

        speed = _parse_scale("speed")
        pitch = _parse_scale("pitch")
        intonation = _parse_scale("intonation")

        sample_lines = None
        if "text" in data:
            text_value = data.get("text")
            if text_value is None:
                sample_lines = None
            elif isinstance(text_value, list):
                sample_lines = [str(item) for item in text_value]
            elif isinstance(text_value, str):
                sample_lines = text_value.splitlines()
            else:
                raise HTTPException(
                    status_code=400, detail="text must be a string or list of strings."
                )
        try:
            sample_text = build_sample_text(sample_lines)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        try:
            wav_bytes = _synthesize_voice_sample(
                config,
                app.state.voicevox_lock,
                speaker_id=int(speaker_value),
                text=sample_text,
                speed=speed,
                pitch=pitch,
                intonation=intonation,
            )
        except (
            VoiceVoxUnavailableError,
            VoiceVoxError,
            VoiceVoxRuntimeError,
            FileNotFoundError,
        ) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return Response(
            content=wav_bytes,
            media_type="audio/wav",
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/api/voice-samples/cached/{sample_path:path}")
    def api_cached_voice_sample(sample_path: str) -> FileResponse:
        sample_file = _resolve_cached_voice_sample(root, sample_path)
        return FileResponse(sample_file, media_type="audio/wav")

    @app.delete("/api/voice-samples/cached/{sample_path:path}")
    def api_delete_cached_voice_sample(sample_path: str) -> JSONResponse:
        sample_file = _resolve_cached_voice_sample(root, sample_path)
        character_dir = sample_file.parent
        index_path = _voice_sample_index_path(character_dir)
        character_name, samples = _read_voice_sample_index(index_path)
        filename = sample_file.name
        filtered = [sample for sample in samples if sample.get("filename") != filename]
        if len(filtered) != len(samples):
            _write_voice_sample_index(
                index_path,
                character=character_name,
                samples=filtered,
            )
        try:
            sample_file.unlink()
        except OSError as exc:
            raise HTTPException(
                status_code=500, detail=f"Failed to delete sample: {exc}"
            ) from exc
        return JSONResponse({"deleted": True, "path": sample_path})

    @app.get("/api/voice-samples")
    def api_voice_samples() -> JSONResponse:
        samples = _list_voice_samples(root)
        return JSONResponse({"samples": samples, "count": len(samples)})

    @app.post("/api/voice-samples/generate")
    def api_generate_voice_samples(
        payload: dict[str, object] | None = Body(None),
    ) -> JSONResponse:
        data = payload or {}
        overwrite = bool(data.get("overwrite"))
        sample_lines = None
        if "text" in data:
            text_value = data.get("text")
            if isinstance(text_value, list):
                sample_lines = [str(item) for item in text_value]
            elif text_value is not None:
                raise HTTPException(
                    status_code=400, detail="text must be a list of strings."
                )
        try:
            result = _generate_voice_samples(
                config,
                root / VOICE_SAMPLES_DIR,
                app.state.voicevox_lock,
                overwrite=overwrite,
                sample_lines=sample_lines,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (
            VoiceVoxUnavailableError,
            VoiceVoxError,
            VoiceVoxRuntimeError,
            FFmpegError,
            FileNotFoundError,
        ) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return JSONResponse(result)

    @app.get("/api/voice-samples/{sample_name}")
    def api_voice_sample(sample_name: str) -> FileResponse:
        sample_path = _resolve_voice_sample(sample_name)
        return FileResponse(sample_path, media_type="audio/mpeg")

    @app.get("/api/books/{book_id:path}/chapters")
    def api_chapters(book_id: str) -> JSONResponse:
        canonical_id, book_path = _resolve_book(book_id)
        (
            metadata,
            book_title,
            book_author,
            cover_path,
            saved_defaults,
            effective_defaults,
        ) = _book_media_info(book_path, config)
        chapters = _list_chapters(book_path)
        status_snapshot = _status_snapshot(canonical_id)
        states = []
        for idx, chapter in enumerate(chapters):
            states.append(
                _chapter_state(
                    chapter,
                    config,
                    idx + 1,
                    chapter_meta=metadata.chapters.get(chapter.name)
                    if metadata
                    else None,
                    build_status=status_snapshot.get(chapter.name),
                )
            )
        completed_count = sum(1 for st in states if st["mp3_exists"])
        summary = {
            "total": len(states),
            "completed": completed_count,
            "pending": len(states) - completed_count,
        }
        with bookmark_lock:
            bookmarks_payload = _bookmarks_payload(book_path)
        media_payload = {
            "album": book_title,
            "artist": book_author or book_title,
            "cover_url": _cover_url(canonical_id, cover_path),
            "tts_defaults": {
                "effective": effective_defaults,
                "saved": saved_defaults,
            },
        }
        return JSONResponse(
            {
                "chapters": states,
                "summary": summary,
                "media": media_payload,
                "bookmarks": bookmarks_payload,
            }
        )

    @app.get("/api/books/{book_id:path}/status")
    def api_book_status(book_id: str) -> JSONResponse:
        canonical_id, _ = _resolve_book(book_id)
        statuses = _status_snapshot(canonical_id)
        return JSONResponse({"status": statuses})

    @app.post("/api/books/{book_id:path}/reprocess")
    def api_reprocess_book(book_id: str) -> JSONResponse:
        canonical_id, book_path = _resolve_book(book_id)
        with build_lock:
            in_progress = any(
                key[0] == canonical_id for key in active_build_jobs.keys()
            )
        if in_progress:
            raise HTTPException(
                status_code=409,
                detail="Cannot reprocess this book while chapters are building.",
            )
        metadata = load_book_metadata(book_path)
        epub_source = _epub_source_for_book(book_path, metadata)
        if epub_source is None:
            raise HTTPException(
                status_code=404,
                detail="No EPUB file found for this book. Place an .epub in the book folder to reprocess.",
            )
        job = UploadJob(
            root, epub_source.name, source_path=epub_source, output_dir=book_path
        )
        upload_manager.enqueue(job)
        return JSONResponse({"job": job.to_payload(), "book": canonical_id})

    @app.get("/api/books/{book_id:path}/bookmarks")
    def api_bookmarks(book_id: str) -> JSONResponse:
        _, book_path = _resolve_book(book_id)
        with bookmark_lock:
            payload = _bookmarks_payload(book_path)
        return JSONResponse(payload)

    @app.post("/api/books/{book_id:path}/bookmarks")
    def api_add_bookmark(
        book_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        _, book_path = _resolve_book(book_id)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        chapter_id = payload.get("chapter_id")
        if not isinstance(chapter_id, str) or not chapter_id.strip():
            raise HTTPException(status_code=400, detail="chapter_id is required.")
        time_value = payload.get("time")
        if not isinstance(time_value, (int, float)) or time_value < 0:
            raise HTTPException(status_code=400, detail="time must be non-negative.")
        label_value = payload.get("label")
        label_text: str | None
        if label_value is None:
            label_text = None
        elif isinstance(label_value, str):
            label_text = label_value.strip() or None
        else:
            raise HTTPException(
                status_code=400, detail="label must be a string or null."
            )
        _ensure_chapter_for_bookmark(book_path, chapter_id)
        with bookmark_lock:
            _append_manual_bookmark(
                book_path, chapter_id, float(time_value), label_text
            )
            bookmarks = _bookmarks_payload(book_path)
        return JSONResponse(bookmarks)

    @app.post("/api/books/{book_id:path}/bookmarks/last-played")
    def api_update_last_played(
        book_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        _, book_path = _resolve_book(book_id)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        chapter_id = payload.get("chapter_id")
        if not isinstance(chapter_id, str) or not chapter_id.strip():
            raise HTTPException(status_code=400, detail="chapter_id is required.")
        time_value = payload.get("time")
        if not isinstance(time_value, (int, float)) or time_value < 0:
            raise HTTPException(status_code=400, detail="time must be non-negative.")
        played_chapter_id = payload.get("played_chapter_id")
        if played_chapter_id is not None:
            if not isinstance(played_chapter_id, str) or not played_chapter_id.strip():
                raise HTTPException(
                    status_code=400,
                    detail="played_chapter_id must be a non-empty string.",
                )
        _ensure_chapter_for_bookmark(book_path, chapter_id)
        if played_chapter_id:
            _ensure_chapter_for_bookmark(book_path, played_chapter_id)
        with bookmark_lock:
            _update_last_played(book_path, chapter_id, float(time_value))
            if played_chapter_id:
                _mark_chapter_played(book_path, played_chapter_id)
            bookmarks = _bookmarks_payload(book_path)
        return JSONResponse(bookmarks)

    @app.delete("/api/books/{book_id:path}/bookmarks/last-played")
    def api_clear_last_played(book_id: str) -> JSONResponse:
        _, book_path = _resolve_book(book_id)
        with bookmark_lock:
            _clear_last_played_entry(book_path)
            bookmarks = _bookmarks_payload(book_path)
        return JSONResponse(bookmarks)

    @app.delete("/api/books/{book_id:path}/bookmarks/{bookmark_id}")
    def api_delete_bookmark(book_id: str, bookmark_id: str) -> JSONResponse:
        _, book_path = _resolve_book(book_id)
        if not bookmark_id:
            raise HTTPException(status_code=400, detail="bookmark_id is required.")
        with bookmark_lock:
            removed = _remove_manual_bookmark(book_path, bookmark_id)
            bookmarks = _bookmarks_payload(book_path)
        if not removed:
            raise HTTPException(status_code=404, detail="Bookmark not found.")
        return JSONResponse(bookmarks)

    @app.patch("/api/books/{book_id:path}/bookmarks/{bookmark_id}")
    def api_update_bookmark(
        book_id: str,
        bookmark_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        _, book_path = _resolve_book(book_id)
        if not bookmark_id:
            raise HTTPException(status_code=400, detail="bookmark_id is required.")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        if "label" not in payload:
            raise HTTPException(status_code=400, detail="label is required.")
        label_value = payload.get("label")
        label_text: str | None
        if label_value is None:
            label_text = None
        elif isinstance(label_value, str):
            label_text = label_value.strip() or None
        else:
            raise HTTPException(
                status_code=400, detail="label must be a string or null."
            )
        with bookmark_lock:
            updated = _update_manual_bookmark_label(book_path, bookmark_id, label_text)
            bookmarks = _bookmarks_payload(book_path)
        if not updated:
            raise HTTPException(status_code=404, detail="Bookmark not found.")
        return JSONResponse(bookmarks)

    @app.delete("/api/books/{book_id:path}")
    def api_delete_book(book_id: str) -> JSONResponse:
        canonical_id, book_path = _resolve_book(book_id)
        with build_lock:
            in_progress = any(
                key[0] == canonical_id for key in active_build_jobs.keys()
            )
        if in_progress:
            raise HTTPException(
                status_code=409,
                detail="Cannot delete this book while chapters are building.",
            )
        with status_lock:
            chapter_status.pop(canonical_id, None)
        try:
            shutil.rmtree(book_path)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Book not found.")
        except OSError as exc:
            raise HTTPException(
                status_code=500, detail=f"Failed to delete book: {exc}"
            ) from exc
        return JSONResponse({"deleted": True, "book": canonical_id})

    @app.post("/api/books/{book_id:path}/tts-defaults")
    def api_update_tts_defaults(
        book_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        _, book_path = _resolve_book(book_id)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        updates: dict[str, float | int | None] = {}
        provided = False
        for key in ("speaker", "speed", "pitch", "intonation"):
            if key not in payload:
                continue
            provided = True
            value = payload[key]
            if value is None:
                updates[key] = None
                continue
            if key == "speaker":
                if isinstance(value, bool):
                    raise HTTPException(
                        status_code=400, detail="speaker must be an integer."
                    )
                if isinstance(value, (int, float)):
                    if isinstance(value, float):
                        if not value.is_integer():
                            raise HTTPException(
                                status_code=400, detail="speaker must be an integer."
                            )
                        value = int(value)
                    speaker_value = int(value)
                    if speaker_value <= 0:
                        raise HTTPException(
                            status_code=400,
                            detail="speaker must be a positive integer.",
                        )
                    updates[key] = speaker_value
                    continue
                raise HTTPException(
                    status_code=400, detail="speaker must be an integer."
                )
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise HTTPException(
                    status_code=400, detail=f"{key} must be numeric or null."
                )
            updates[key] = float(value)
        if not provided:
            raise HTTPException(
                status_code=400,
                detail="Provide at least one voice setting to update.",
            )
        changed = False
        if updates:
            changed = update_book_tts_defaults(book_path, updates)
        (
            _,
            _book_title,
            _book_author,
            _cover_path,
            saved_defaults,
            effective_defaults,
        ) = _book_media_info(book_path, config)
        return JSONResponse(
            {
                "changed": bool(changed),
                "saved": saved_defaults,
                "effective": effective_defaults,
            }
        )

    @app.get("/api/books/{book_id:path}/cover")
    def api_cover(book_id: str) -> FileResponse:
        _, book_path = _resolve_book(book_id)
        _, _, _, cover_path, _, _ = _book_media_info(book_path, config)
        if cover_path is None or not cover_path.exists():
            raise HTTPException(status_code=404, detail="Cover not found")
        return FileResponse(cover_path)

    @app.post("/api/books/{book_id:path}/chapters/{chapter_id}/tokens")
    async def api_create_token(
        book_id: str,
        chapter_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        _, book_path = _resolve_book(book_id)
        chapter_path = book_path / chapter_id
        if not chapter_path.exists():
            raise HTTPException(status_code=404, detail="Chapter not found")
        try:
            start = int(payload.get("start"))  # type: ignore[arg-type]
            end = int(payload.get("end"))  # type: ignore[arg-type]
        except Exception:
            raise HTTPException(
                status_code=400, detail="start and end must be integers"
            )
        if start < 0 or end <= start:
            raise HTTPException(status_code=400, detail="Invalid selection bounds")
        replacement = payload.get("replacement")
        reading = payload.get("reading")
        surface = payload.get("surface")
        pos = payload.get("pos")
        accent = payload.get("accent")
        if replacement is not None and not isinstance(replacement, str):
            replacement = None
        if reading is not None and not isinstance(reading, str):
            reading = None
        if surface is not None and not isinstance(surface, str):
            surface = None
        if pos is not None and not isinstance(pos, str):
            pos = None
        if accent is not None and not isinstance(accent, int):
            accent = None
        try:
            created = create_token_from_selection(
                chapter_path,
                start,
                end,
                replacement=replacement,
                reading=reading,
                surface=surface,
                pos=pos,
                accent=accent,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse({"updated": 1 if created else 0})

    @app.post("/api/books/{book_id:path}/chapters/{chapter_id}/prepare")
    async def api_prepare_chapter(
        book_id: str,
        chapter_id: str,
        restart: bool = Query(False),
    ) -> JSONResponse:
        book_key, book_path = _resolve_book(book_id)
        chapter_path = book_path / chapter_id
        if not chapter_path.exists():
            raise HTTPException(status_code=404, detail="Chapter not found")

        target = TTSTarget(
            source=chapter_path,
            text_path=None,
            output=chapter_path.with_suffix(".mp3"),
        )
        metadata = load_book_metadata(book_path)
        _populate_target_metadata(target, book_path, metadata)
        voice_overrides = _voice_settings_for_book(config, metadata)
        force_indices = frozenset({0}) if restart else frozenset()

        loop = asyncio.get_running_loop()

        job_key = (book_key, chapter_id)
        cancel_event = threading.Event()

        def work() -> int:
            return _synthesize_sequence(
                config,
                [target],
                app.state.voicevox_lock,
                force_indices=force_indices,
                progress_handler=_record_progress_event,
                voice_settings=voice_overrides,
                cancel_event=cancel_event,
            )

        with build_lock:
            if job_key in active_build_jobs:
                raise HTTPException(
                    status_code=409, detail="Chapter is already building."
                )
            _set_chapter_status(
                book_key,
                chapter_id,
                state="queued",
                message="Queued",
            )
            future = loop.run_in_executor(None, work)
            active_build_jobs[job_key] = {"cancel": cancel_event, "future": future}

        try:
            created = await future
        except KeyboardInterrupt:
            _clear_chapter_status(book_key, chapter_id)
            raise HTTPException(
                status_code=409, detail="Build aborted by user."
            ) from None
        except (VoiceVoxUnavailableError, VoiceVoxError, FFmpegError) as exc:
            _set_chapter_status(
                book_key,
                chapter_id,
                state="error",
                message=str(exc),
                error=str(exc),
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except Exception as exc:
            _set_chapter_status(
                book_key,
                chapter_id,
                state="error",
                message=str(exc),
                error=str(exc),
            )
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            with build_lock:
                active_build_jobs.pop(job_key, None)

        cache_dir = _target_cache_dir(config.cache_dir, target)
        total_chunks = _safe_read_int(cache_dir / ".complete")
        try:
            mp3_mtime = target.output.stat().st_mtime
        except OSError:
            mp3_mtime = None
        return JSONResponse(
            {
                "status": "ready",
                "created": bool(created),
                "total_chunks": total_chunks,
                "mp3_mtime": mp3_mtime,
            }
        )

    @app.post("/api/books/{book_id:path}/chapters/{chapter_id}/abort")
    async def api_abort_chapter(book_id: str, chapter_id: str) -> JSONResponse:
        book_key, book_path = _resolve_book(book_id)
        chapter_path = book_path / chapter_id
        if not chapter_path.exists():
            raise HTTPException(status_code=404, detail="Chapter not found")
        key = (book_key, chapter_id)
        with build_lock:
            job = active_build_jobs.get(key)
        if not job:
            raise HTTPException(
                status_code=409, detail="Chapter is not currently building."
            )
        cancel_event = job.get("cancel")
        if isinstance(cancel_event, threading.Event):
            cancel_event.set()
        _set_chapter_status(book_key, chapter_id, state="aborting")
        return JSONResponse({"status": "aborting"})

    @app.get("/api/books/{book_id:path}/chapters/{chapter_id}/stream")
    async def api_stream_chapter(book_id: str, chapter_id: str) -> FileResponse:
        _, book_path = _resolve_book(book_id)
        chapter_path = book_path / chapter_id
        if not chapter_path.exists():
            raise HTTPException(status_code=404, detail="Chapter not found")

        target = TTSTarget(source=chapter_path, output=chapter_path.with_suffix(".mp3"))

        if not target.output.exists():
            raise HTTPException(status_code=409, detail="Chapter audio not built yet.")

        return FileResponse(
            target.output,
            media_type="audio/mpeg",
            filename=target.output.name,
        )

    return app
