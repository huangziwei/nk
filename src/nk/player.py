from __future__ import annotations

import asyncio
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
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from .book_io import (
    ChapterMetadata,
    LoadedBookMetadata,
    load_book_metadata,
    update_book_tts_defaults,
)
from .library import BookListing, list_books_sorted
from .tts import (
    FFmpegError,
    TTSTarget,
    VoiceVoxClient,
    VoiceVoxError,
    VoiceVoxUnavailableError,
    _parse_track_number_from_name,
    _synthesize_target_with_client,
    _target_cache_dir,
    discover_voicevox_runtime,
    managed_voicevox_runtime,
)
from .uploads import UploadJob, UploadManager
from .voice_defaults import (
    DEFAULT_INTONATION_SCALE,
    DEFAULT_PITCH_SCALE,
    DEFAULT_SPEAKER_ID,
    DEFAULT_SPEED_SCALE,
)


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
_SORT_MODES = {"author", "recent"}


def _normalize_sort_mode(value: str | None) -> str:
    if not value:
        return "author"
    normalized = value.strip().lower()
    if normalized in _SORT_MODES:
        return normalized
    raise HTTPException(status_code=400, detail="Invalid sort mode.")


INDEX_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>nk Player</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
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
    header {
      padding: 1.3rem 1.6rem 1rem;
      background: linear-gradient(135deg, rgba(59,130,246,0.18), transparent);
    }
    header h1 {
      margin: 0;
      font-size: 1.55rem;
      font-weight: 700;
      letter-spacing: 0.01em;
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
      padding: 1rem 1.1rem;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      min-height: 160px;
      width: 100%;
      max-width: 420px;
    }
    .card.empty-card {
      justify-content: center;
      text-align: center;
      color: var(--muted);
      border: 1px dashed rgba(255,255,255,0.08);
    }
    .collection-card {
      cursor: pointer;
      transition: transform 0.15s ease, border-color 0.15s ease;
      display: flex;
      flex-direction: column;
      gap: 0.8rem;
      padding: 1rem 1rem 1.1rem;
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
      padding: 1rem 1.1rem;
      background: rgba(15,18,32,0.75);
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      margin-bottom: 1rem;
    }
    .epub-alert-header {
      display: flex;
      justify-content: space-between;
      gap: 0.75rem;
      align-items: center;
      flex-wrap: wrap;
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
      position: relative;
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
    .upload-card .upload-actions {
      display: inline-flex;
      justify-content: center;
      gap: 0.5rem;
      flex-wrap: wrap;
      margin-top: 0.2rem;
    }
    .upload-card .upload-actions button {
      background: rgba(59,130,246,0.15);
      color: var(--accent);
      border-radius: 999px;
      border: 1px solid rgba(59,130,246,0.4);
      padding: 0.25rem 0.9rem;
      font-size: 0.85rem;
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
    }
    .badge.success {
      background: rgba(34, 197, 94, 0.28);
      color: #bbf7d0;
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
    audio {
      width: 100%;
      margin-top: 0.75rem;
    }
    .chapter-player {
      margin-top: 0.4rem;
      padding-top: 0.75rem;
      border-top: 1px solid rgba(148, 163, 184, 0.2);
    }
    .chapter-player.hidden {
      display: none;
    }
    .chapter-player .now-playing {
      font-size: 0.95rem;
      color: var(--muted);
    }
    .chapter-player audio {
      margin-top: 0.5rem;
    }
    .chapter-player .status-line {
      margin-top: 0.4rem;
    }
    .status-line {
      margin-top: 0.5rem;
      color: var(--muted);
      font-size: 0.9rem;
    }
    .player-meta {
      display: flex;
      gap: 0.8rem;
      align-items: center;
    }
    .player-cover {
      width: 64px;
      height: 64px;
      border-radius: 12px;
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
      gap: 0.2rem;
    }
    .player-subtitle {
      font-size: 0.85rem;
      color: var(--muted);
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
      audio {
        margin-top: 0.6rem;
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
    }
  </style>
</head>
<body>
  <header>
    <h1>nk Player</h1>
    <p>Play chapterized TXT files with VoiceVox. Use Build to synthesize audio before playback.</p>
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
            </select>
          </label>
        </div>
      </div>
      <div class="epub-alert hidden" id="pending-epubs">
        <div class="epub-alert-header">
          <div>
            <strong>Unprocessed EPUBs</strong>
            <p class="epub-alert-note">These EPUB files are in this folder but haven't been chapterized yet.</p>
          </div>
          <button type="button" class="secondary" id="epub-chapterize-all">Chapterize all</button>
        </div>
        <div class="epub-list" id="pending-epub-list"></div>
      </div>
      <div class="cards collection-cards hidden" id="collections-grid"></div>
      <div class="cards" id="books-grid"></div>
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
      <audio id="player" controls preload="none"></audio>
      <div class="player-actions hidden">
        <button id="bookmark-add" class="secondary">Add bookmark</button>
      </div>
      <div class="status-line" id="status">Idle</div>
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

  <script>
    const booksGrid = document.getElementById('books-grid');
    const collectionsGrid = document.getElementById('collections-grid');
    const libraryBreadcrumb = document.getElementById('library-breadcrumb');
    const libraryBackButton = document.getElementById('library-back');
    const chaptersPanel = document.getElementById('chapters-panel');
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
    const lastPlayStatus = document.getElementById('last-play-status');
    const bookmarkAddWrapper = document.querySelector('#player-dock .player-actions');
    const bookmarkAddBtn = document.getElementById('bookmark-add');
    const bookmarkList = document.getElementById('bookmark-list');
    const noteModal = document.getElementById('note-modal');
    const noteTextarea = document.getElementById('note-input');
    const noteModalMeta = document.getElementById('note-modal-meta');
    const noteSaveBtn = document.getElementById('note-save');
    const noteCancelBtn = document.getElementById('note-cancel');
    const voiceSpeakerInput = document.getElementById('voice-speaker');
    const voiceSpeedInput = document.getElementById('voice-speed');
    const voicePitchInput = document.getElementById('voice-pitch');
    const voiceIntonationInput = document.getElementById('voice-intonation');
    const voiceSaveBtn = document.getElementById('voice-save');
    const voiceResetBtn = document.getElementById('voice-reset');
    const voiceStatus = document.getElementById('voice-status');
    const booksSortSelect = document.getElementById('books-sort');
    const pendingEpubPanel = document.getElementById('pending-epubs');
    const pendingEpubList = document.getElementById('pending-epub-list');
    const pendingEpubAllBtn = document.getElementById('epub-chapterize-all');

    const DEFAULT_VOICE = {
      speaker: 2,
      speed: 1,
      pitch: -0.08,
      intonation: 1.25,
    };

    const state = {
      books: [],
      collections: [],
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
      },
      localBuilds: new Set(),
      uploadJobs: [],
      librarySortOrder: 'author',
      libraryPrefix: '',
      parentPrefix: '',
      pendingEpubs: [],
      epubBusy: new Set(),
    };
    const LIBRARY_SORT_KEY = 'nkPlayerSortOrder';
    const storedLibrarySort = window.localStorage.getItem(LIBRARY_SORT_KEY);
    if (storedLibrarySort === 'recent' || storedLibrarySort === 'author') {
      state.librarySortOrder = storedLibrarySort;
    }
    let statusPollHandle = null;
    let lastPlaySyncAt = 0;
    let lastPlayPending = null;
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
        const next = booksSortSelect.value === 'recent' ? 'recent' : 'author';
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
        handlePromise(loadBooks(state.parentPrefix || ''));
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

    function chapterDisplayTitle(chapter) {
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
          <input type="file" accept=".epub" data-role="upload-input" tabindex="-1" aria-hidden="true">
          <strong>Upload EPUB</strong>
          <p>Drop an .epub here or click to select a file. nk will chapterize it and add it to your library.</p>
          <div class="upload-actions">
            <button type="button" class="secondary">Select EPUB</button>
          </div>
        </div>
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
      const selectButton = card.querySelector('.upload-actions button');
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
      if (selectButton) {
        selectButton.addEventListener('click', (event) => {
          event.preventDefault();
          triggerFileDialog();
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
      const hasNewSuccess = normalized.some(
        job => job.status === 'success' && prevStatuses.get(job.id) !== 'success'
      );
      if (hasNewSuccess) {
        handlePromise(loadBooks());
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

    function startUploadPolling() {
      if (uploadPollTimer !== null) {
        return;
      }
      uploadPollTimer = window.setInterval(() => {
        loadUploads();
      }, UPLOAD_POLL_INTERVAL);
    }

    function handleUploadFiles(fileList) {
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

    function setBookmarks(payload) {
      const manual = Array.isArray(payload?.manual) ? payload.manual.slice() : [];
      const last = payload?.last_played;
      state.bookmarks = {
        manual,
        lastPlayed:
          last &&
          typeof last.chapter === 'string' &&
          Number.isFinite(Number(last.time))
            ? { chapter: last.chapter, time: Number(last.time) }
            : null,
      };
      updateBookmarkUI();
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
      await playChapter(index, { resumeTime: entry.time || 0 });
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

    async function persistLastPlayed(chapterId, time) {
      if (!state.currentBook || !chapterId || !Number.isFinite(time)) return;
      const data = await fetchJSON(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/bookmarks/last-played`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chapter_id: chapterId, time }),
        }
      );
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
      if (last && last.chapter === chapter.id && Math.abs(last.time - time) < 5) {
        return;
      }
      const now = Date.now();
      if (lastPlayPending || now - lastPlaySyncAt < 5000) {
        return;
      }
      lastPlayPending = persistLastPlayed(chapter.id, time)
        .catch(() => {})
        .finally(() => {
          lastPlayPending = null;
          lastPlaySyncAt = Date.now();
        });
    }

    function recordCompletionProgress() {
      const chapter = currentChapter();
      if (!chapter || !state.currentBook) return;
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
      handlePromise(persistLastPlayed(targetChapterId, resumeTime));
    }

    function updatePlayerDetails(chapter) {
      if (!chapter) {
        nowPlaying.textContent = 'Select a chapter to begin.';
        playerSubtitle.textContent = '';
        if (playerCover) {
          playerCover.classList.add('hidden');
          playerCover.removeAttribute('src');
        }
        return;
      }
      const trackLabel = formatTrackNumber(chapter.track_number);
      const chapterTitle = chapterDisplayTitle(chapter) || chapter.id;
      nowPlaying.textContent = trackLabel ? `${trackLabel} ${chapterTitle}` : chapterTitle;
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

    function handlePromise(promise) {
      promise.catch(err => {
        if (err && err.name === 'AbortError') return;
        const msg = err?.message || String(err);
        statusLine.textContent = `Error: ${msg}`;
      });
    }

    function isNoteModalActive() {
      return noteModal && !noteModal.classList.contains('hidden');
    }

    function setNoteModalDisabled(disabled) {
      if (noteSaveBtn) noteSaveBtn.disabled = disabled;
      if (noteCancelBtn) noteCancelBtn.disabled = disabled;
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
    if (noteTextarea) {
      noteTextarea.addEventListener('keydown', (event) => {
        if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
          event.preventDefault();
          submitNoteEditor();
        }
      });
    }
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && isNoteModalActive()) {
        event.preventDefault();
        closeNoteEditor();
      }
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
      if (!lastOpenedBookId || !booksGrid) return;
      const nodes = booksGrid.querySelectorAll('[data-book-id]');
      for (const node of nodes) {
        if (node.dataset.bookId === lastOpenedBookId) {
          node.scrollIntoView({ behavior: 'smooth', block: 'center' });
          break;
        }
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
          node.textContent = crumb.label || 'Library';
          if (!isLast) {
            node.type = 'button';
            node.addEventListener('click', () => {
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

    function renderCollections() {
      if (!collectionsGrid) return;
      collectionsGrid.innerHTML = '';
      if (!state.collections.length) {
        collectionsGrid.classList.add('hidden');
        return;
      }
      collectionsGrid.classList.remove('hidden');
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
        await loadBooks(state.libraryPrefix || '');
        await loadUploads();
      } catch (err) {
        alert(`Failed to chapterize EPUBs: ${err.message || err}`);
      } finally {
        syncPendingEpubBusy();
        renderPendingEpubs();
      }
    }

    function renderBooks() {
      if (!booksGrid) return;
      booksGrid.innerHTML = '';
      const showUpload = !state.collections.length;
      const hasBooks = Array.isArray(state.books) && state.books.length > 0;
      if (!showUpload && !hasBooks) {
        booksGrid.classList.add('hidden');
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
        empty.textContent = 'No books in this folder yet. Choose a collection or upload a book.';
        booksGrid.appendChild(empty);
        return;
      }
      state.books.forEach(book => {
        const card = document.createElement('article');
        card.className = 'card';
        const bookId = typeof book.path === 'string' && book.path ? book.path : book.id;
        if (bookId) {
          card.dataset.bookId = bookId;
        }

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
        booksGrid.appendChild(card);
      });
      scrollToLastBook();
    }

    function chapterStatusInfo(ch) {
      const status = ch.build_status;
      if (status) {
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
          return { label: 'Aborting…', className: 'warning' };
        }
        if (status.state === 'aborted') {
          return { label: 'Aborted', className: 'muted' };
        }
        if (status.state === 'error') {
          return { label: 'Failed', className: 'danger' };
        }
      }
      if (ch.mp3_exists) {
        return { label: 'Ready', className: 'success' };
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
          const isAborting = chapter.build_status && chapter.build_status.state === 'aborting';
          primaryBtn.disabled = Boolean(isAborting);
          primaryBtn.classList.toggle('danger', Boolean(isBuilding || isAborting));
          primaryBtn.onclick = () => {
            if (isBuilding) {
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
          restartBtn.disabled = Boolean(chapter.build_status && (chapter.build_status.state === 'building' || chapter.build_status.state === 'aborting'));
        }
      });
    }

    function applyStatusUpdates(statusMap) {
      state.chapters.forEach(ch => {
        const nextStatus = statusMap[ch.id] || null;
        const hasLocal = state.localBuilds && state.localBuilds.has(ch.id);
        if (nextStatus) {
          ch.build_status = nextStatus;
        } else if (!hasLocal) {
          ch.build_status = null;
        }
        if (nextStatus && typeof nextStatus.chunk_count === 'number' && !ch.total_chunks) {
          ch.total_chunks = nextStatus.chunk_count;
        }
      });
      updateChapterStatusUI();
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
      if (ch.build_status && ch.build_status.state === 'building') {
        return 'Abort';
      }
      if (ch.build_status && ch.build_status.state === 'aborting') {
        return 'Aborting…';
      }
      return ch.mp3_exists ? 'Play' : 'Build';
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
            if (bookmarkAddWrapper) {
              bookmarkAddWrapper.classList.remove('hidden');
            }
            docked = true;
          }
        } else {
          node.classList.remove('playing');
        }
      });
      if (playerDock && !docked) {
        playerDock.classList.add('hidden');
        chaptersPanel.appendChild(playerDock);
        if (bookmarkAddWrapper) {
          bookmarkAddWrapper.classList.add('hidden');
        }
      }
      if (state.currentChapterIndex < 0) {
        updatePlayerDetails(null);
      }
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
        const displayTitle = chapterDisplayTitle(ch);
        name.textContent = trackLabel ? `${trackLabel} ${displayTitle}` : displayTitle;
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

        footer.appendChild(statusBadges);

        const buttons = document.createElement('div');
        buttons.className = 'badges controls';
        const playBtn = document.createElement('button');
        playBtn.textContent = chapterPrimaryLabel(ch);
        const isBuilding = ch.build_status && ch.build_status.state === 'building';
        const isAborting = ch.build_status && ch.build_status.state === 'aborting';
        playBtn.disabled = Boolean(isAborting);
        playBtn.classList.toggle('danger', Boolean(isBuilding || isAborting));
        playBtn.onclick = () => {
          if (isBuilding) {
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
        restartBtn.disabled = Boolean(ch.build_status && (ch.build_status.state === 'building' || ch.build_status.state === 'aborting'));
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

    async function loadBooks(nextPrefix = undefined) {
      const params = new URLSearchParams();
      params.set('sort', state.librarySortOrder || 'author');
      let targetPrefix = '';
      if (typeof nextPrefix === 'string') {
        targetPrefix = nextPrefix;
      } else if (typeof state.libraryPrefix === 'string') {
        targetPrefix = state.libraryPrefix;
      }
      if (targetPrefix) {
        params.set('prefix', targetPrefix);
      }
      const data = await fetchJSON(`/api/books?${params.toString()}`);
      state.libraryPrefix = typeof data.prefix === 'string' ? data.prefix : '';
      state.parentPrefix = typeof data.parent_prefix === 'string' ? data.parent_prefix : '';
      state.collections = Array.isArray(data.collections) ? data.collections : [];
      state.books = Array.isArray(data.books) ? data.books : [];
      state.pendingEpubs = Array.isArray(data.pending_epubs) ? data.pending_epubs : [];
      syncPendingEpubBusy();
      renderLibraryNav();
      renderCollections();
      renderPendingEpubs();
      renderBooks();
    }

    async function openBook(book) {
      lastOpenedBookId = book.id;
      await loadChapters(book);
      const panel = document.getElementById('chapters-panel');
      if (panel) {
        panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
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
      } catch (err) {
        alert(`Failed to load chapters: ${err.message}`);
      }
    }

    async function buildChapter(index, { restart = false } = {}) {
      if (!state.currentBook) return;
      const chapter = state.chapters[index];
      if (!chapter) return;
      if (chapter.build_status && chapter.build_status.state === 'building') {
        return;
      }
      const params = new URLSearchParams();
      if (restart) params.set('restart', '1');
      if (state.currentChapterIndex === index) {
        statusLine.textContent = restart ? 'Rebuilding audio...' : 'Building audio...';
      }
      setChapterStatusLabel(chapter.id, restart ? 'Rebuilding…' : 'Building…', 'badge warning');
      if (state.localBuilds) {
        state.localBuilds.add(chapter.id);
      }
      const clearLocalBuild = () => {
        if (state.localBuilds) {
          state.localBuilds.delete(chapter.id);
        }
        chapter.build_status = null;
        renderChapters(summaryForChapters());
      };
      chapter.build_status = { state: 'building' };
      renderChapters(summaryForChapters());
      try {
        const res = await fetch(
          `/api/books/${encodeURIComponent(state.currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/prepare?${params.toString()}`,
          { method: 'POST' }
        );
        if (!res.ok) {
          const detail = await readErrorResponse(res);
          if (res.status === 409 && detail.toLowerCase().includes('aborted')) {
            clearLocalBuild();
            if (state.currentChapterIndex === index) {
              statusLine.textContent = 'Build aborted.';
            }
            return;
          }
          clearLocalBuild();
          if (state.currentChapterIndex !== index) {
            setChapterStatusLabel(chapter.id, 'Failed', 'badge danger');
          }
          throw new Error(detail);
        }
        const result = await res.json();
        clearLocalBuild();
        chapter.mp3_exists = true;
        if (typeof result.total_chunks === 'number') {
          chapter.total_chunks = result.total_chunks;
        }
        chapter.has_cache = true;
        renderChapters(summaryForChapters());
        if (state.currentChapterIndex === index) {
          statusLine.textContent = 'Build finished. Tap Play to listen.';
        }
      } catch (err) {
        clearLocalBuild();
        throw err;
      }
    }

    async function abortChapter(index) {
      if (!state.currentBook) return;
      const chapter = state.chapters[index];
      if (!chapter) return;
      const res = await fetch(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/abort`,
        { method: 'POST' }
      );
      if (!res.ok) {
        const detail = await readErrorResponse(res);
        throw new Error(detail);
      }
      setChapterStatusLabel(chapter.id, 'Aborting…', 'badge warning');
      chapter.build_status = { state: 'aborting' };
      renderChapters(summaryForChapters());
      if (state.currentChapterIndex === index) {
        statusLine.textContent = 'Aborting build...';
      }
      refreshStatuses();
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
      updateMediaSession(chapter);
      if (Number.isFinite(resumeTime) && resumeTime >= 0) {
        ensureSeekAfterLoad(resumeTime, `Resumed at ${formatTimecode(resumeTime)}.`);
      }
      try {
        await player.play();
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

    player.addEventListener('playing', () => {
      statusLine.textContent = 'Playing';
    });
    player.addEventListener('waiting', () => {
      statusLine.textContent = 'Buffering...';
    });
    player.addEventListener('timeupdate', () => {
      if (player.paused || player.seeking) return;
      scheduleLastPlaySync(player.currentTime);
    });
    player.addEventListener('pause', () => {
      if (!player.ended) {
        scheduleLastPlaySync(player.currentTime);
        statusLine.textContent = 'Paused';
      }
    });
    player.addEventListener('ended', () => {
      statusLine.textContent = 'Finished';
      renderChapters(summaryForChapters());
      recordCompletionProgress();
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
    });

    backButton.onclick = () => {
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
      if (playerDock) {
        playerDock.classList.add('hidden');
        chaptersPanel.appendChild(playerDock);
      }
      updatePlayerDetails(null);
      applyVoiceDefaults(DEFAULT_VOICE, {});
      renderBooks();
    };

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

    playBookBtn.onclick = () => handlePromise(resumeLastPlay());
    restartBookBtn.onclick = () => handlePromise(playBook(true));

    ensureUploadCard();
    renderUploadJobs();
    loadUploads();
    startUploadPolling();

    setBookmarks({ manual: [], last_played: null });

    renderLibraryNav();
    renderCollections();
    renderPendingEpubs();

    loadBooks().catch(err => {
      booksGrid.innerHTML = `<div style="color:var(--danger)">Failed to load books: ${err.message}</div>`;
    });
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
        if p.is_file() and not p.name.endswith(".original.txt")
    ]


def _bookmark_file(book_dir: Path) -> Path:
    return book_dir / BOOKMARKS_FILENAME


def _empty_bookmark_state() -> dict[str, object]:
    return {"version": BOOKMARK_STATE_VERSION, "manual": [], "last_played": None}


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
    return {
        "version": BOOKMARK_STATE_VERSION,
        "manual": manual_entries,
        "last_played": last_payload,
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
    return {
        "manual": manual_entries,
        "last_played": last_payload,
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
    target = TTSTarget(source=chapter_path, output=chapter_path.with_suffix(".mp3"))
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


def create_app(config: PlayerConfig) -> FastAPI:
    root = config.root.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Books root not found: {root}")

    app = FastAPI(title="nk VoiceVox")
    app.state.config = config
    app.state.root = root
    app.state.voicevox_lock = threading.Lock()
    upload_manager = UploadManager(root)
    app.state.upload_manager = upload_manager
    app.add_event_handler("shutdown", upload_manager.shutdown)

    status_lock = threading.Lock()
    chapter_status: dict[str, dict[str, dict[str, object]]] = {}
    bookmark_lock = threading.Lock()
    build_lock = threading.Lock()
    active_build_jobs: dict[tuple[str, str], dict[str, object]] = {}

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
        if not candidate.exists() or not candidate.is_dir() or not _is_book_dir(candidate):
            raise HTTPException(status_code=404, detail="Book not found")
        return canonical, candidate

    def _book_id_from_target(target: TTSTarget) -> str:
        try:
            return _relative_library_path(root, target.source.parent)
        except ValueError:
            return target.source.parent.name

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

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return INDEX_HTML

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
                skipped.append(
                    {"path": raw_entry, "reason": "Invalid path entry."}
                )
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
                relative_path = _relative_library_path(
                    root, candidate, allow_root=True
                )
            except ValueError:
                skipped.append(
                    {"path": normalized, "reason": "Path escapes root."}
                )
                continue
            if candidate.suffix.lower() != ".epub":
                skipped.append(
                    {"path": relative_path, "reason": "Not an EPUB file."}
                )
                continue
            if not candidate.is_file():
                skipped.append(
                    {"path": relative_path, "reason": "File not found."}
                )
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
                target_rel = _relative_library_path(
                    root, target_dir, allow_root=True
                )
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
        sort: str | None = Query(None, description="Sort order: author or recent"),
    ) -> JSONResponse:
        sort_mode = _normalize_sort_mode(sort)
        prefix_value, prefix_path = _resolve_prefix(prefix)
        parent_prefix = ""
        if prefix_value:
            parent_prefix = (
                prefix_value.rsplit("/", 1)[0] if "/" in prefix_value else ""
            )
        collections_payload = _list_collections(root, prefix_path)
        pending_epubs = _list_pending_epubs(root, prefix_path)
        books_payload = []
        for listing in _list_books(prefix_path, sort_mode):
            book_dir = listing.path
            try:
                book_id = _relative_library_path(root, book_dir)
            except ValueError:
                continue
            (
                metadata,
                book_title,
                book_author,
                cover_path,
                saved_defaults,
                effective_defaults,
            ) = _book_media_info(book_dir, config, metadata=listing.metadata)
            chapters = _list_chapters(book_dir)
            status_snapshot = _status_snapshot(book_id)
            states = [
                _chapter_state(
                    chapter,
                    config,
                    idx + 1,
                    chapter_meta=metadata.chapters.get(chapter.name)
                    if metadata
                    else None,
                    build_status=status_snapshot.get(chapter.name),
                )
                for idx, chapter in enumerate(chapters)
            ]
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
            books_payload.append(payload)
        return JSONResponse(
            {
                "prefix": prefix_value,
                "parent_prefix": parent_prefix,
                "collections": collections_payload,
                "books": books_payload,
                "pending_epubs": pending_epubs,
            }
        )

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
        states = [
            _chapter_state(
                chapter,
                config,
                idx + 1,
                chapter_meta=metadata.chapters.get(chapter.name) if metadata else None,
                build_status=status_snapshot.get(chapter.name),
            )
            for idx, chapter in enumerate(chapters)
        ]
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
        _ensure_chapter_for_bookmark(book_path, chapter_id)
        with bookmark_lock:
            _update_last_played(book_path, chapter_id, float(time_value))
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

        target = TTSTarget(source=chapter_path, output=chapter_path.with_suffix(".mp3"))
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
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            with build_lock:
                active_build_jobs.pop(job_key, None)

        cache_dir = _target_cache_dir(config.cache_dir, target)
        total_chunks = _safe_read_int(cache_dir / ".complete")
        return JSONResponse(
            {
                "status": "ready",
                "created": bool(created),
                "total_chunks": total_chunks,
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
