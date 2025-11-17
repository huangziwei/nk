from __future__ import annotations

import asyncio
import shutil
import threading
import time
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from .book_io import (
    ChapterMetadata,
    LoadedBookMetadata,
    load_book_metadata,
    update_book_tts_defaults,
)
from .voice_defaults import (
    DEFAULT_INTONATION_SCALE,
    DEFAULT_PITCH_SCALE,
    DEFAULT_SPEAKER_ID,
    DEFAULT_SPEED_SCALE,
)
from .tts import (
    FFmpegError,
    TTSTarget,
    VoiceVoxClient,
    VoiceVoxError,
    VoiceVoxUnavailableError,
    _synthesize_target_with_client,
    _target_cache_dir,
    discover_voicevox_runtime,
    managed_voicevox_runtime,
)


@dataclass(slots=True)
class WebConfig:
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


INDEX_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>nk VoiceVox Player</title>
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
      padding: 0.8rem 1rem;
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
    .card .cover {
      width: 100%;
      border-radius: calc(var(--radius) - 6px);
      object-fit: cover;
      display: block;
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
    .player-actions {
      margin-top: 0.6rem;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }
    .bookmark-row {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 0.5rem;
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
    }
    .bookmark-item .label {
      font-weight: 600;
    }
    .bookmark-item .time {
      color: var(--muted);
    }
    .bookmark-item .actions {
      display: flex;
      gap: 0.4rem;
      flex-shrink: 0;
    }
    .bookmark-item button {
      font-size: 0.8rem;
      padding: 0.3rem 0.6rem;
    }
    .bookmark-empty {
      font-size: 0.85rem;
      color: var(--muted);
    }
    @media (max-width: 640px) {
      header {
        padding: 1.2rem 1.1rem 0.9rem;
      }
      main {
        padding: 0 1.1rem 1.6rem;
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
      <div class="voice-controls">
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
          <span class="voice-status" id="voice-status"></span>
        </div>
      </div>
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
      <div class="player-actions">
        <div class="bookmark-row">
          <button id="bookmark-add" class="secondary" disabled>Add bookmark</button>
        </div>
        <span class="bookmark-status" id="last-play-status">No last play saved.</span>
      </div>
      <div class="bookmark-list" id="bookmark-list"></div>
      <div class="status-line" id="status">Idle</div>
    </div>
  </main>

  <script>
    const booksGrid = document.getElementById('books-grid');
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
    const bookmarkAddBtn = document.getElementById('bookmark-add');
    const bookmarkList = document.getElementById('bookmark-list');
    const voiceSpeakerInput = document.getElementById('voice-speaker');
    const voiceSpeedInput = document.getElementById('voice-speed');
    const voicePitchInput = document.getElementById('voice-pitch');
    const voiceIntonationInput = document.getElementById('voice-intonation');
    const voiceSaveBtn = document.getElementById('voice-save');
    const voiceResetBtn = document.getElementById('voice-reset');
    const voiceStatus = document.getElementById('voice-status');

    const DEFAULT_VOICE = {
      speaker: 2,
      speed: 1,
      pitch: -0.08,
      intonation: 1.25,
    };

    const state = {
      books: [],
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
    };
    let statusPollHandle = null;
    let lastPlaySyncAt = 0;
    let lastPlayPending = null;

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

    function currentChapter() {
      if (state.currentChapterIndex < 0) return null;
      return state.chapters[state.currentChapterIndex] || null;
    }

    function chapterById(chapterId) {
      return state.chapters.find(ch => ch.id === chapterId) || null;
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

    function renderManualBookmarks() {
      if (!bookmarkList) return;
      bookmarkList.innerHTML = '';
      const chapter = currentChapter();
      if (!chapter) {
        bookmarkList.innerHTML =
          '<div class="bookmark-empty">Select a chapter to manage bookmarks.</div>';
        if (bookmarkAddBtn) bookmarkAddBtn.disabled = true;
        return;
      }
      if (bookmarkAddBtn) bookmarkAddBtn.disabled = false;
      const entries = (state.bookmarks.manual || [])
        .filter(entry => entry.chapter === chapter.id)
        .sort((a, b) => a.time - b.time);
      if (!entries.length) {
        bookmarkList.innerHTML =
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
          const nextLabel = window.prompt('Bookmark notes (leave empty to clear).', currentLabel);
          if (nextLabel === null) {
            return;
          }
          handlePromise(renameBookmark(entry.id, nextLabel.trim()));
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
        bookmarkList.appendChild(item);
      });
    }

    function updateBookmarkUI() {
      renderManualBookmarks();
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
      const chapterTitle = chapter.title || chapter.id;
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
      const chapterLabel = trackLabel ? `${trackLabel} ${chapter.title}` : chapter.title;
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

    function renderBooks() {
      booksGrid.innerHTML = '';
      if (!state.books.length) {
        const empty = document.createElement('div');
        empty.textContent = 'No books discovered under this library root.';
        empty.style.color = 'var(--muted)';
        booksGrid.appendChild(empty);
        return;
      }
      state.books.forEach(book => {
        const card = document.createElement('article');
        card.className = 'card';

        if (book.cover_url) {
          const cover = document.createElement('img');
          cover.className = 'cover';
          cover.src = book.cover_url;
          cover.alt = `${book.title} cover`;
          card.appendChild(cover);
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

        const open = document.createElement('button');
        open.textContent = 'Open chapters';
        open.onclick = () => handlePromise(loadChapters(book).then(() => {
          const panel = document.getElementById('chapters-panel');
          if (panel) {
            panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
          }
        }));
        card.appendChild(open);

        booksGrid.appendChild(card);
      });
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
        if (status.state === 'error') {
          return { label: 'Failed', className: 'danger' };
        }
      }
      if (ch.mp3_exists) {
        return { label: 'Ready', className: 'success' };
      }
      if (ch.has_cache && ch.total_chunks) {
        return { label: `Cached (${ch.total_chunks} chunks)`, className: 'muted' };
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
        const statusInfo = chapterStatusInfo(chapter);
        const statusEl = node.querySelector('[data-role="status-label"]');
        if (statusEl) {
          statusEl.textContent = statusInfo.label;
          statusEl.className = statusInfo.className ? `badge ${statusInfo.className}` : 'badge';
        }
        const chunkEl = node.querySelector('[data-role="chunk-label"]');
        if (chunkEl) {
          if (chapter.total_chunks) {
            chunkEl.textContent = `${chapter.total_chunks} chunks`;
            chunkEl.classList.remove('hidden');
          } else {
            chunkEl.classList.add('hidden');
          }
        }
      });
    }

    function applyStatusUpdates(statusMap) {
      state.chapters.forEach(ch => {
        const nextStatus = statusMap[ch.id] || null;
        ch.build_status = nextStatus;
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
      return ch.mp3_exists ? 'Play' : 'Build';
    }

    function updateChapterHighlight() {
      const nodes = chaptersList.querySelectorAll('.chapter');
      let docked = false;
      nodes.forEach((node, idx) => {
        if (idx === state.currentChapterIndex) {
          node.classList.add('playing');
          if (playerDock && !docked) {
            node.appendChild(playerDock);
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
        name.textContent = trackLabel ? `${trackLabel} ${ch.title}` : ch.title;
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

        const chunkSpan = document.createElement('span');
        chunkSpan.dataset.role = 'chunk-label';
        chunkSpan.className = 'badge muted';
        if (ch.total_chunks) {
          chunkSpan.textContent = `${ch.total_chunks} chunks`;
        } else {
          chunkSpan.classList.add('hidden');
        }
        statusBadges.appendChild(chunkSpan);
        footer.appendChild(statusBadges);

        const buttons = document.createElement('div');
        buttons.className = 'badges';
        const playBtn = document.createElement('button');
        playBtn.textContent = chapterPrimaryLabel(ch);
        playBtn.onclick = () => {
          if (ch.mp3_exists) {
            handlePromise(playChapter(index));
          } else {
            handlePromise(buildChapter(index, { restart: false }));
          }
        };
        buttons.appendChild(playBtn);

        const restartBtn = document.createElement('button');
        restartBtn.textContent = 'Rebuild';
        restartBtn.className = 'secondary';
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

    async function loadBooks() {
      const data = await fetchJSON('/api/books');
      state.books = data.books;
      renderBooks();
    }

    async function loadChapters(book, options = {}) {
      const { preserveSelection = false } = options;
      state.currentBook = book;
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
      const params = new URLSearchParams();
      if (restart) params.set('restart', '1');
      statusLine.textContent = restart ? 'Rebuilding audio...' : 'Building audio...';
      const res = await fetch(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/prepare?${params.toString()}`,
        { method: 'POST' }
      );
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      const result = await res.json();
      chapter.mp3_exists = true;
      if (typeof result.total_chunks === 'number') {
        chapter.total_chunks = result.total_chunks;
      }
      chapter.has_cache = true;
      renderChapters(summaryForChapters());
      statusLine.textContent = 'Build finished. Tap Play to listen.';
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
      handlePromise(clearLastPlayed());
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

    setBookmarks({ manual: [], last_played: null });

    loadBooks().catch(err => {
      booksGrid.innerHTML = `<div style="color:var(--danger)">Failed to load books: ${err.message}</div>`;
    });
  </script>
</body>
</html>
"""


def _list_books(root: Path) -> list[Path]:
    books: list[Path] = []
    for entry in sorted(root.iterdir()):
        if entry.is_dir() and any(entry.glob("*.txt")):
            books.append(entry)
    return books


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
                    "created_at": created_at if isinstance(created_at, (int, float)) else None,
                    "updated_at": updated_at if isinstance(updated_at, (int, float)) else None,
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
                "updated_at": updated_at if isinstance(updated_at, (int, float)) else None,
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


def _chapter_state(
    chapter_path: Path,
    config: WebConfig,
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
    if chapter_meta:
        if chapter_meta.title:
            title = chapter_meta.title
        elif chapter_meta.original_title:
            title = chapter_meta.original_title
    state: dict[str, object] = {
        "id": chapter_path.name,
        "title": title,
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
    config: WebConfig,
) -> tuple[
    LoadedBookMetadata | None,
    str,
    str | None,
    Path | None,
    dict[str, float | int],
    dict[str, float],
]:
    metadata = load_book_metadata(book_dir)
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
    return f"/api/books/{book_id}/cover?ts={mtime}"


def _voice_settings_for_book(
    config: WebConfig,
    metadata: LoadedBookMetadata | None,
    *,
    for_display: bool = False,
) -> dict[str, float | int | None]:
    defaults = metadata.tts_defaults if metadata else None

    def _pick(
        attr: str,
        cfg_value: float | int | None,
        fallback: float | int,
    ) -> float | int | None:
        if defaults:
            value = getattr(defaults, attr, None)
            if value is not None:
                return value
        if cfg_value is not None:
            return cfg_value
        if for_display:
            return fallback
        return None

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
    config: WebConfig,
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
    effective = _voice_settings_for_book(config, metadata, for_display=True)
    return saved, effective


def _synthesize_sequence(
    config: WebConfig,
    targets: list[TTSTarget],
    lock: threading.Lock,
    *,
    force_indices: frozenset[int] | None = None,
    progress_handler: Callable[[TTSTarget, dict[str, object]], None] | None = None,
    voice_settings: dict[str, float | int | None] | None = None,
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
                    if force:
                        target.output.unlink(missing_ok=True)
                        cache_dir = _target_cache_dir(config.cache_dir, target)
                        shutil.rmtree(cache_dir, ignore_errors=True)
                    progress_callback = None
                    if progress_handler is not None:
                        def _adapter(event: dict[str, object], current_target=target) -> None:
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
                        )
                    except Exception as exc:
                        if progress_handler is not None:
                            progress_handler(target, {"event": "target_error", "error": str(exc)})
                        raise
            finally:
                client.close()
    return len(work_plan)


def create_app(config: WebConfig) -> FastAPI:
    root = config.root.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Books root not found: {root}")

    app = FastAPI(title="nk VoiceVox")
    app.state.config = config
    app.state.root = root
    app.state.voicevox_lock = threading.Lock()

    status_lock = threading.Lock()
    chapter_status: dict[str, dict[str, dict[str, object]]] = {}
    bookmark_lock = threading.Lock()

    def _book_id_from_target(target: TTSTarget) -> str:
        try:
            rel = target.source.parent.resolve().relative_to(root)
            if rel.parts:
                return rel.parts[0]
        except Exception:
            pass
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
            return {chapter_id: entry.copy() for chapter_id, entry in book_entry.items()}

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

    @app.get("/api/books")
    def api_books() -> JSONResponse:
        books_payload = []
        for book_dir in _list_books(root):
            (
                metadata,
                book_title,
                book_author,
                cover_path,
                saved_defaults,
                effective_defaults,
            ) = _book_media_info(book_dir, config)
            chapters = _list_chapters(book_dir)
            status_snapshot = _status_snapshot(book_dir.name)
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
                "id": book_dir.name,
                "title": book_title,
                "total_chapters": total,
                "completed_chapters": completed,
                "pending_chapters": pending,
            }
            if book_author:
                payload["author"] = book_author
            cover_url = _cover_url(book_dir.name, cover_path)
            if cover_url:
                payload["cover_url"] = cover_url
            books_payload.append(payload)
        return JSONResponse({"books": books_payload})

    @app.get("/api/books/{book_id}/chapters")
    def api_chapters(book_id: str) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        (
            metadata,
            book_title,
            book_author,
            cover_path,
            saved_defaults,
            effective_defaults,
        ) = _book_media_info(book_path, config)
        chapters = _list_chapters(book_path)
        status_snapshot = _status_snapshot(book_id)
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
            "cover_url": _cover_url(book_id, cover_path),
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

    @app.get("/api/books/{book_id}/status")
    def api_book_status(book_id: str) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        statuses = _status_snapshot(book_id)
        return JSONResponse({"status": statuses})

    @app.get("/api/books/{book_id}/bookmarks")
    def api_bookmarks(book_id: str) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        with bookmark_lock:
            payload = _bookmarks_payload(book_path)
        return JSONResponse(payload)

    @app.post("/api/books/{book_id}/bookmarks")
    def api_add_bookmark(
        book_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
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
            raise HTTPException(status_code=400, detail="label must be a string or null.")
        if label_text and len(label_text) > 200:
            label_text = label_text[:200]
        _ensure_chapter_for_bookmark(book_path, chapter_id)
        with bookmark_lock:
            _append_manual_bookmark(book_path, chapter_id, float(time_value), label_text)
            bookmarks = _bookmarks_payload(book_path)
        return JSONResponse(bookmarks)

    @app.delete("/api/books/{book_id}/bookmarks/{bookmark_id}")
    def api_delete_bookmark(book_id: str, bookmark_id: str) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        if not bookmark_id:
            raise HTTPException(status_code=400, detail="bookmark_id is required.")
        with bookmark_lock:
            removed = _remove_manual_bookmark(book_path, bookmark_id)
            bookmarks = _bookmarks_payload(book_path)
        if not removed:
            raise HTTPException(status_code=404, detail="Bookmark not found.")
        return JSONResponse(bookmarks)

    @app.patch("/api/books/{book_id}/bookmarks/{bookmark_id}")
    def api_update_bookmark(
        book_id: str,
        bookmark_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
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
            raise HTTPException(status_code=400, detail="label must be a string or null.")
        if label_text and len(label_text) > 200:
            label_text = label_text[:200]
        with bookmark_lock:
            updated = _update_manual_bookmark_label(book_path, bookmark_id, label_text)
            bookmarks = _bookmarks_payload(book_path)
        if not updated:
            raise HTTPException(status_code=404, detail="Bookmark not found.")
        return JSONResponse(bookmarks)

    @app.post("/api/books/{book_id}/bookmarks/last-played")
    def api_update_last_played(
        book_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
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

    @app.delete("/api/books/{book_id}/bookmarks/last-played")
    def api_clear_last_played(book_id: str) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        with bookmark_lock:
            _clear_last_played_entry(book_path)
            bookmarks = _bookmarks_payload(book_path)
        return JSONResponse(bookmarks)

    @app.post("/api/books/{book_id}/tts-defaults")
    def api_update_tts_defaults(
        book_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
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
                raise HTTPException(status_code=400, detail="speaker must be an integer.")
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

    @app.get("/api/books/{book_id}/cover")
    def api_cover(book_id: str) -> FileResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        _, _, _, cover_path, _, _ = _book_media_info(book_path, config)
        if cover_path is None or not cover_path.exists():
            raise HTTPException(status_code=404, detail="Cover not found")
        return FileResponse(cover_path)

    @app.post("/api/books/{book_id}/chapters/{chapter_id}/prepare")
    async def api_prepare_chapter(
        book_id: str,
        chapter_id: str,
        restart: bool = Query(False),
    ) -> JSONResponse:
        book_path = root / book_id
        chapter_path = book_path / chapter_id
        if not chapter_path.exists():
            raise HTTPException(status_code=404, detail="Chapter not found")

        target = TTSTarget(source=chapter_path, output=chapter_path.with_suffix(".mp3"))
        metadata = load_book_metadata(book_path)
        voice_overrides = _voice_settings_for_book(config, metadata, for_display=False)
        force_indices = frozenset({0}) if restart else frozenset()

        loop = asyncio.get_running_loop()

        def work() -> int:
            return _synthesize_sequence(
                config,
                [target],
                app.state.voicevox_lock,
                force_indices=force_indices,
                progress_handler=_record_progress_event,
                voice_settings=voice_overrides,
            )

        try:
            created = await loop.run_in_executor(None, work)
        except (VoiceVoxUnavailableError, VoiceVoxError, FFmpegError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        cache_dir = _target_cache_dir(config.cache_dir, target)
        total_chunks = _safe_read_int(cache_dir / ".complete")
        return JSONResponse(
            {
                "status": "ready",
                "created": bool(created),
                "total_chunks": total_chunks,
            }
        )

    @app.get("/api/books/{book_id}/chapters/{chapter_id}/stream")
    async def api_stream_chapter(book_id: str, chapter_id: str) -> FileResponse:
        book_path = root / book_id
        chapter_path = book_path / chapter_id
        if not chapter_path.exists():
            raise HTTPException(status_code=404, detail="Chapter not found")

        target = TTSTarget(source=chapter_path, output=chapter_path.with_suffix(".mp3"))

        if not target.output.exists():
            raise HTTPException(
                status_code=409, detail="Chapter audio not built yet."
            )

        return FileResponse(
            target.output,
            media_type="audio/mpeg",
            filename=target.output.name,
        )

    return app
