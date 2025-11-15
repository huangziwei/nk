from __future__ import annotations

import asyncio
import shutil
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from .book_io import (
    ChapterMetadata,
    LoadedBookMetadata,
    load_book_metadata,
    write_book_package,
)
from .core import epub_to_chapter_texts, get_epub_cover
from .nlp import NLPBackend, NLPBackendUnavailableError
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
    live_prebuffer: int = 2  # kept for CLI compatibility


COVER_EXTENSIONS = (".jpg", ".jpeg", ".png")


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
    <p>Play chapterized TXT files with VoiceVox. Chapters without MP3s will be rendered before playback.</p>
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
          <button id="play-book">Resume Book</button>
          <button id="restart-book" class="secondary">Restart Book</button>
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

    const state = {
      books: [],
      chapters: [],
      currentBook: null,
      currentChapterIndex: -1,
      autoAdvance: false,
      media: null,
    };
    let statusPollHandle = null;

    function formatTrackNumber(num) {
      if (typeof num !== 'number' || !Number.isFinite(num)) return '';
      return String(num).padStart(3, '0');
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

    async function fetchJSON(url) {
      const res = await fetch(url, { cache: 'no-store' });
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
      return ch.mp3_exists ? 'Play' : 'Build & Play';
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
        playBtn.onclick = () => handlePromise(playChapter(index, { restart: false }));
        buttons.appendChild(playBtn);

        const restartBtn = document.createElement('button');
        restartBtn.textContent = 'Rebuild';
        restartBtn.className = 'secondary';
        restartBtn.onclick = () => handlePromise(playChapter(index, { restart: true }));
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
        renderChapters(data.summary);
        chaptersPanel.classList.remove('hidden');
        startStatusPolling();
      } catch (err) {
        alert(`Failed to load chapters: ${err.message}`);
      }
    }

    function triggerPrefetch(startIndex, { restart = false } = {}) {
      if (!state.currentBook) return;
      if (startIndex == null || startIndex > state.chapters.length) return;
      fetch(`/api/books/${encodeURIComponent(state.currentBook.id)}/prefetch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ start_index: startIndex, restart }),
      }).catch(() => {});
    }

    async function prepareChapter(chapter, { restart = false, prefetchStartIndex = null, prefetchRestart = false } = {}) {
      const params = new URLSearchParams();
      if (restart) params.set('restart', '1');
      const res = await fetch(
        `/api/books/${encodeURIComponent(state.currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/prepare?${params.toString()}`,
        { method: 'POST' }
      );
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      const result = await res.json();
      if (result.created) {
        chapter.mp3_exists = true;
      }
      if (typeof result.total_chunks === 'number') {
        chapter.total_chunks = result.total_chunks;
      }
      chapter.has_cache = true;
      renderChapters(summaryForChapters());

      if (prefetchStartIndex != null) {
        triggerPrefetch(prefetchStartIndex, { restart: prefetchRestart });
      }

      const playUrl = `/api/books/${encodeURIComponent(state.currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/stream?ts=${Date.now()}`;
      player.pause();
      player.src = playUrl;
      player.load();
      updateMediaSession(chapter);
      try {
        await player.play();
        statusLine.textContent = 'Playing';
      } catch {
        statusLine.textContent = 'Tap play to start audio.';
      }
    }

    async function playChapter(index, { restart = false } = {}) {
      if (!state.chapters.length) return;
      state.autoAdvance = true;
      state.currentChapterIndex = index;
      updateChapterHighlight();

      const chapter = state.chapters[index];
      updatePlayerDetails(chapter);
      statusLine.textContent = 'Preparing audio...';

      const nextIndex = chapter.index + 1;
      await prepareChapter(chapter, {
        restart,
        prefetchStartIndex: nextIndex <= state.chapters.length ? nextIndex : null,
        prefetchRestart: false,
      });
    }

    function findChapterIndexForBookStart(restart) {
      if (!state.chapters.length) return 0;
      if (!restart) {
        const firstPending = state.chapters.findIndex(ch => !ch.mp3_exists);
        if (firstPending !== -1) return firstPending;
      }
      return 0;
    }

    async function playBook(restart = false) {
      if (!state.chapters.length) return;
      const index = findChapterIndexForBookStart(restart);
      await playChapter(index, { restart });
      if (restart) {
        triggerPrefetch(1, { restart: true });
      }
    }

    player.addEventListener('playing', () => {
      statusLine.textContent = 'Playing';
    });
    player.addEventListener('waiting', () => {
      statusLine.textContent = 'Buffering...';
    });
    player.addEventListener('pause', () => {
      if (!player.ended) {
        statusLine.textContent = 'Paused';
      }
    });
    player.addEventListener('ended', () => {
      statusLine.textContent = 'Finished';
      renderChapters(summaryForChapters());
      if (state.autoAdvance && state.currentChapterIndex + 1 < state.chapters.length) {
        handlePromise(playChapter(state.currentChapterIndex + 1, { restart: false }));
      } else {
        state.autoAdvance = false;
        if (state.currentBook) {
          handlePromise(loadChapters(state.currentBook, { preserveSelection: false }));
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
      player.pause();
      statusLine.textContent = 'Idle';
      player.removeAttribute('src');
      player.load();
      if (playerDock) {
        playerDock.classList.add('hidden');
        chaptersPanel.appendChild(playerDock);
      }
      updatePlayerDetails(null);
      renderBooks();
    };

    playBookBtn.onclick = () => handlePromise(playBook(false));
    restartBookBtn.onclick = () => handlePromise(playBook(true));

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
    return [p for p in sorted(book_dir.glob("*.txt")) if p.is_file()]


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
) -> tuple[LoadedBookMetadata | None, str, str | None, Path | None]:
    metadata = load_book_metadata(book_dir)
    title = metadata.title if metadata and metadata.title else book_dir.name
    author = metadata.author if metadata else None
    cover_path = None
    if metadata and metadata.cover_path and metadata.cover_path.exists():
        cover_path = metadata.cover_path
    if cover_path is None:
        cover_path = _fallback_cover_path(book_dir)
    return metadata, title, author, cover_path


def _cover_url(book_id: str, cover_path: Path | None) -> str | None:
    if not cover_path or not cover_path.exists():
        return None
    try:
        mtime = int(cover_path.stat().st_mtime)
    except OSError:
        mtime = 0
    return f"/api/books/{book_id}/cover?ts={mtime}"


def _synthesize_sequence(
    config: WebConfig,
    targets: list[TTSTarget],
    lock: threading.Lock,
    *,
    force_indices: frozenset[int] | None = None,
    progress_handler: Callable[[TTSTarget, dict[str, object]], None] | None = None,
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
            client = VoiceVoxClient(
                base_url=config.engine_url,
                speaker_id=config.speaker,
                timeout=60.0,
                post_phoneme_length=config.pause,
                speed_scale=config.speed_scale,
                pitch_scale=config.pitch_scale,
                intonation_scale=config.intonation_scale,
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
                            live_playback=False,
                            playback_callback=None,
                            live_prebuffer=config.live_prebuffer,
                        )
                    except Exception as exc:
                        if progress_handler is not None:
                            progress_handler(target, {"event": "target_error", "error": str(exc)})
                        raise
            finally:
                client.close()
    return len(work_plan)


def _ensure_chapterized(root: Path) -> None:
    epubs = sorted(p for p in root.iterdir() if p.suffix.lower() == ".epub")
    if not epubs:
        return
    mode = "advanced"
    nlp = None
    try:
        nlp = NLPBackend()
    except NLPBackendUnavailableError:
        mode = "fast"
    for epub_path in epubs:
        output_dir = epub_path.with_suffix("")
        if output_dir.exists() and any(output_dir.glob("*.txt")):
            continue
        try:
            print(f"[nk web] Generating chapters for {epub_path.name} (mode={mode})")
            chapters = epub_to_chapter_texts(str(epub_path), mode=mode, nlp=nlp)
            cover = get_epub_cover(str(epub_path))
            write_book_package(
                output_dir,
                chapters,
                source_epub=epub_path,
                cover_image=cover,
            )
        except Exception as exc:  # pragma: no cover - fail fast
            raise RuntimeError(f"Failed to chapterize {epub_path}: {exc}") from exc


def create_app(config: WebConfig) -> FastAPI:
    root = config.root.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Books root not found: {root}")
    _ensure_chapterized(root)

    app = FastAPI(title="nk VoiceVox")
    app.state.config = config
    app.state.root = root
    app.state.voicevox_lock = threading.Lock()
    app.state.prefetch_tasks: dict[str, threading.Thread] = {}

    status_lock = threading.Lock()
    chapter_status: dict[str, dict[str, dict[str, object]]] = {}

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

    def _spawn_prefetch(
        book_id: str,
        targets: list[TTSTarget],
        force_indices: frozenset[int],
    ) -> bool:
        if not targets:
            return False
        pending = any(
            (idx in force_indices) or (not target.output.exists())
            for idx, target in enumerate(targets)
        )
        if not pending:
            return False
        tasks: dict[str, threading.Thread] = app.state.prefetch_tasks
        existing = tasks.get(book_id)
        if existing and existing.is_alive():
            return False

        def worker() -> None:
            try:
                for idx, target in enumerate(targets):
                    force = frozenset({0}) if idx in force_indices else frozenset()
                    try:
                        _synthesize_sequence(
                            config,
                            [target],
                            app.state.voicevox_lock,
                            force_indices=force,
                            progress_handler=_record_progress_event,
                        )
                    except Exception:
                        break
            except Exception:
                pass
            finally:
                tasks.pop(book_id, None)

        thread = threading.Thread(
            target=worker,
            name=f"nk-prefetch-{book_id}",
            daemon=True,
        )
        tasks[book_id] = thread
        thread.start()
        return True

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return INDEX_HTML

    @app.get("/api/books")
    def api_books() -> JSONResponse:
        books_payload = []
        for book_dir in _list_books(root):
            metadata, book_title, book_author, cover_path = _book_media_info(book_dir)
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
        metadata, book_title, book_author, cover_path = _book_media_info(book_path)
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
        summary = {
            "total": len(states),
            "completed": sum(1 for st in states if st["mp3_exists"]),
            "pending": sum(1 for st in states if not st["mp3_exists"]),
        }
        media_payload = {
            "album": book_title,
            "artist": book_author or book_title,
            "cover_url": _cover_url(book_id, cover_path),
        }
        return JSONResponse(
            {"chapters": states, "summary": summary, "media": media_payload}
        )

    @app.get("/api/books/{book_id}/status")
    def api_book_status(book_id: str) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        statuses = _status_snapshot(book_id)
        return JSONResponse({"status": statuses})

    @app.get("/api/books/{book_id}/cover")
    def api_cover(book_id: str) -> FileResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        _, _, _, cover_path = _book_media_info(book_path)
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
        force_indices = frozenset({0}) if restart else frozenset()

        loop = asyncio.get_running_loop()

        def work() -> int:
            return _synthesize_sequence(
                config,
                [target],
                app.state.voicevox_lock,
                force_indices=force_indices,
                progress_handler=_record_progress_event,
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
            loop = asyncio.get_running_loop()

            def work() -> int:
                return _synthesize_sequence(
                    config,
                    [target],
                    app.state.voicevox_lock,
                    force_indices=frozenset(),
                    progress_handler=_record_progress_event,
                )

            try:
                await loop.run_in_executor(None, work)
            except (VoiceVoxUnavailableError, VoiceVoxError, FFmpegError) as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc)) from exc

        if not target.output.exists():
            raise HTTPException(status_code=500, detail="Failed to synthesize chapter.")

        return FileResponse(
            target.output,
            media_type="audio/mpeg",
            filename=target.output.name,
        )

    @app.post("/api/books/{book_id}/prefetch")
    def api_prefetch(
        book_id: str,
        payload: dict[str, object] = Body(...),
    ) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")

        try:
            start_index = int(payload.get("start_index", 1))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail="start_index must be an integer"
            )
        restart_all = bool(payload.get("restart", False))

        chapters = _list_chapters(book_path)
        if not chapters:
            return JSONResponse({"status": "empty", "pending": 0})

        start_index = max(1, start_index)
        if start_index > len(chapters):
            return JSONResponse({"status": "noop", "pending": 0})

        slice_chapters = chapters[start_index - 1 :]
        targets = [
            TTSTarget(source=chapter, output=chapter.with_suffix(".mp3"))
            for chapter in slice_chapters
        ]
        force_indices = frozenset(range(len(targets))) if restart_all else frozenset()

        pending = sum(
            1
            for idx, target in enumerate(targets)
            if (idx in force_indices) or not target.output.exists()
        )
        if pending == 0:
            return JSONResponse({"status": "noop", "pending": 0})

        started = _spawn_prefetch(book_id, targets, force_indices)
        return JSONResponse(
            {"status": "started" if started else "running", "pending": pending}
        )

    return app
