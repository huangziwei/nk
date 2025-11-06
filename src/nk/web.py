from __future__ import annotations

import asyncio
import shutil
import threading
from dataclasses import dataclass
from pathlib import Path
import re
import unicodedata

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from .tts import (
    FFmpegError,
    TTSTarget,
    VoiceVoxClient,
    VoiceVoxError,
    VoiceVoxUnavailableError,
    _target_cache_dir,
    discover_voicevox_runtime,
    managed_voicevox_runtime,
    _synthesize_target_with_client,
)
from .core import ChapterText, epub_to_chapter_texts
from .nlp import NLPBackend, NLPBackendUnavailableError


@dataclass(slots=True)
class WebConfig:
    root: Path
    speaker: int = 2
    engine_url: str = "http://127.0.0.1:50021"
    engine_runtime: Path | None = None
    engine_wait: float = 30.0
    ffmpeg_path: str = "ffmpeg"
    pause: float = 0.4
    cache_dir: Path | None = None
    keep_cache: bool = True
    live_prebuffer: int = 2  # kept for CLI compatibility


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
    }
    .card {
      background: var(--panel-alt);
      border-radius: calc(var(--radius) - 4px);
      padding: 1rem 1.1rem;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      min-height: 160px;
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
    audio {
      width: 100%;
      margin-top: 0.75rem;
    }
    .status-line {
      margin-top: 0.5rem;
      color: var(--muted);
      font-size: 0.9rem;
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
    <h1>nk VoiceVox Player</h1>
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

    <section class="panel hidden" id="player-panel">
      <h2>Now Playing</h2>
      <div id="now-playing" class="metrics">Select a chapter to begin.</div>
      <audio id="player" controls preload="none"></audio>
      <div class="status-line" id="status">Idle</div>
    </section>
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
    const playerPanel = document.getElementById('player-panel');
    const player = document.getElementById('player');
    const nowPlaying = document.getElementById('now-playing');
    const statusLine = document.getElementById('status');

    const state = {
      books: [],
      chapters: [],
      currentBook: null,
      currentChapterIndex: -1,
      autoAdvance: false,
    };

    function updateMediaSession(chapter) {
      if (!('mediaSession' in navigator) || !chapter || !state.currentBook) return;
      const chapterLabel = `${String(chapter.index).padStart(3, '0')} ${state.currentBook.title}`;
      navigator.mediaSession.metadata = new MediaMetadata({
        title: chapterLabel,
        artist: state.currentBook.title,
        album: state.currentBook.title,
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

        const title = document.createElement('div');
        title.className = 'title';
        title.textContent = book.title;
        card.appendChild(title);

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

    function chapterStatusText(ch) {
      if (ch.mp3_exists) {
        return 'Ready';
      }
      if (ch.has_cache && ch.total_chunks) {
        return `Cached (${ch.total_chunks} chunks)`;
      }
      if (ch.has_cache) {
        return 'Cached';
      }
      return 'Pending';
    }

    function chapterPrimaryLabel(ch) {
      return ch.mp3_exists ? 'Play' : 'Build & Play';
    }

    function updateChapterHighlight() {
      const nodes = chaptersList.querySelectorAll('.chapter');
      nodes.forEach((node, idx) => {
        if (idx === state.currentChapterIndex) {
          node.classList.add('playing');
        } else {
          node.classList.remove('playing');
        }
      });
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

        const header = document.createElement('div');
        header.className = 'chapter-header';
        const name = document.createElement('div');
        name.className = 'name';
        name.textContent = ch.title;
        header.appendChild(name);
        wrapper.appendChild(header);

        const footer = document.createElement('div');
        footer.className = 'chapter-footer';
        const statusBadges = document.createElement('div');
        statusBadges.className = 'badges';
        const statusLabel = chapterStatusText(ch);
        statusBadges.appendChild(
          badge(
            statusLabel,
            ch.mp3_exists ? 'success' : statusLabel === 'Pending' ? 'warning' : 'muted'
          )
        );
        if (ch.total_chunks) {
          statusBadges.appendChild(badge(`${ch.total_chunks} chunks`, 'muted'));
        }
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
      }
      try {
        const data = await fetchJSON(`/api/books/${encodeURIComponent(book.id)}/chapters`);
        state.chapters = data.chapters;
        renderChapters(data.summary);
        chaptersPanel.classList.remove('hidden');
      } catch (err) {
        alert(`Failed to load chapters: ${err.message}`);
      }
    }

    function ensurePlayerVisible() {
      if (playerPanel.classList.contains('hidden')) {
        playerPanel.classList.remove('hidden');
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
      ensurePlayerVisible();

      const chapter = state.chapters[index];
      nowPlaying.textContent = `${state.currentBook.title} — ${chapter.title}`;
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
      chaptersPanel.classList.add('hidden');
      state.chapters = [];
      state.currentBook = null;
      state.autoAdvance = false;
      state.currentChapterIndex = -1;
      player.pause();
      nowPlaying.textContent = 'Select a chapter to begin.';
      statusLine.textContent = 'Idle';
      player.removeAttribute('src');
      player.load();
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


def _chapter_state(chapter_path: Path, config: WebConfig, index: int) -> dict[str, object]:
    target = TTSTarget(source=chapter_path, output=chapter_path.with_suffix(".mp3"))
    cache_dir = _target_cache_dir(config.cache_dir, target)
    total_chunks = _safe_read_int(cache_dir / ".complete")
    return {
        "id": chapter_path.name,
        "title": chapter_path.name,
        "index": index,
        "mp3_exists": target.output.exists(),
        "has_cache": cache_dir.exists(),
        "total_chunks": total_chunks,
    }


def _synthesize_sequence(
    config: WebConfig,
    targets: list[TTSTarget],
    lock: threading.Lock,
    *,
    force_indices: frozenset[int] | None = None,
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
        runtime_hint = config.engine_runtime or discover_voicevox_runtime(config.engine_url)
        with managed_voicevox_runtime(
            runtime_hint,
            config.engine_url,
            readiness_timeout=config.engine_wait,
        ):
            client = VoiceVoxClient(
                base_url=config.engine_url,
                speaker_id=config.speaker,
                timeout=60.0,
                post_phoneme_length=config.pause,
            )
            try:
                total = len(work_plan)
                for order, (_, target, force) in enumerate(work_plan, start=1):
                    if force:
                        target.output.unlink(missing_ok=True)
                        cache_dir = _target_cache_dir(config.cache_dir, target)
                        shutil.rmtree(cache_dir, ignore_errors=True)
                    _synthesize_target_with_client(
                        target,
                        client,
                        index=order,
                        total=total,
                        ffmpeg_path=config.ffmpeg_path,
                        overwrite=False,
                        progress=None,
                        cache_base=config.cache_dir,
                        keep_cache=config.keep_cache,
                        live_playback=False,
                        playback_callback=None,
                        live_prebuffer=config.live_prebuffer,
                    )
            finally:
                client.close()
    return len(work_plan)


def _slugify_for_filename(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    cleaned_chars: list[str] = []
    for ch in normalized:
        if ch in {"/", "\\", ":", "*", "?", '"', "<", ">", "|"}:
            cleaned_chars.append("_")
            continue
        if ord(ch) < 32:
            continue
        if ch.isspace():
            cleaned_chars.append("_")
            continue
        cleaned_chars.append(ch)
    slug = "".join(cleaned_chars)
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug[:80]


def _chapter_basename(index: int, chapter: ChapterText, used_names: set[str]) -> str:
    prefix = f"{index + 1:03d}"
    candidates: list[str] = []
    if chapter.title:
        slug = _slugify_for_filename(chapter.title)
        if slug:
            candidates.append(slug)
    source_stem = Path(chapter.source).stem
    stem_slug = _slugify_for_filename(source_stem)
    if stem_slug:
        candidates.append(stem_slug)
    for slug in candidates:
        candidate = f"{prefix}_{slug}"
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
    fallback = prefix
    suffix = 1
    candidate = fallback
    while candidate in used_names:
        suffix += 1
        candidate = f"{fallback}_{suffix}"
    used_names.add(candidate)
    return candidate


def _write_chapter_files(output_dir: Path, chapters: list[ChapterText]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    used_names: set[str] = set()
    for index, chapter in enumerate(chapters):
        basename = _chapter_basename(index, chapter, used_names)
        output_path = output_dir / f"{basename}.txt"
        output_path.write_text(chapter.text, encoding="utf-8")


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
            _write_chapter_files(output_dir, chapters)
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
            chapters = _list_chapters(book_dir)
            states = [
                _chapter_state(chapter, config, idx + 1)
                for idx, chapter in enumerate(chapters)
            ]
            total = len(states)
            completed = sum(1 for st in states if st["mp3_exists"])
            pending = total - completed
            books_payload.append(
                {
                    "id": book_dir.name,
                    "title": book_dir.name,
                    "total_chapters": total,
                    "completed_chapters": completed,
                    "pending_chapters": pending,
                }
            )
        return JSONResponse({"books": books_payload})

    @app.get("/api/books/{book_id}/chapters")
    def api_chapters(book_id: str) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        chapters = _list_chapters(book_path)
        states = [
            _chapter_state(chapter, config, idx + 1)
            for idx, chapter in enumerate(chapters)
        ]
        summary = {
            "total": len(states),
            "completed": sum(1 for st in states if st["mp3_exists"]),
            "pending": sum(1 for st in states if not st["mp3_exists"]),
        }
        return JSONResponse({"chapters": states, "summary": summary})

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
            raise HTTPException(status_code=400, detail="start_index must be an integer")
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
