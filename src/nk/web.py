from __future__ import annotations

import asyncio
import queue
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse

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
    live_prebuffer: int = 2


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
    .badge.accent {
      background: var(--accent);
      color: #fff;
    }
    .badge.success {
      background: rgba(34, 197, 94, 0.28);
      color: #bbf7d0;
    }
    .badge.warning {
      background: rgba(251, 191, 36, 0.3);
      color: #fcd34d;
    }
    .card button,
    .action-bar button,
    .chapter button {
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
    <p>Stream chapterized TXT files with automatic buffering and resume support.</p>
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

        const badges = document.createElement('div');
        badges.className = 'badges';
        badges.appendChild(badge(`${book.completed_chapters}/${book.total_chapters} complete`, book.completed_chapters === book.total_chapters && book.total_chapters > 0 ? 'success' : ''));
        if (book.resume_chapters > 0) {
          badges.appendChild(badge(`${book.resume_chapters} resumable`, 'warning'));
        }
        if (book.total_chapters === 0) {
          badges.appendChild(badge('Empty', 'secondary'));
        }
        card.appendChild(badges);

        const open = document.createElement('button');
        open.textContent = 'Open chapters';
        open.onclick = () => loadChapters(book);
        card.appendChild(open);

        booksGrid.appendChild(card);
      });
    }

    function chapterStatusText(ch) {
      if (ch.mp3_exists) {
        return 'Complete';
      }
      if (ch.resume_available && ch.progress_chunks) {
        if (ch.total_chunks) {
          return `Resume ${ch.progress_chunks}/${ch.total_chunks}`;
        }
        return `Resume chunk ${ch.progress_chunks}`;
      }
      if (ch.has_cache) {
        return 'Cached';
      }
      return 'Not started';
    }

    function chapterPrimaryLabel(ch) {
      if (ch.mp3_exists) {
        return 'Play';
      }
      return ch.resume_available ? 'Resume' : 'Play';
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
      chaptersMetrics.appendChild(badge(`${summary.completed}/${summary.total} complete`, summary.completed === summary.total && summary.total > 0 ? 'success' : ''));
      if (summary.resume > 0) {
        chaptersMetrics.appendChild(badge(`${summary.resume} resumable`, 'warning'));
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
        statusBadges.appendChild(badge(statusLabel, ch.mp3_exists ? 'success' : ch.resume_available ? 'warning' : ''));
        if (ch.total_chunks) {
          statusBadges.appendChild(badge(`${ch.total_chunks} chunks`, 'secondary'));
        }
        footer.appendChild(statusBadges);

        const buttons = document.createElement('div');
        buttons.className = 'badges';
        const playBtn = document.createElement('button');
        playBtn.textContent = chapterPrimaryLabel(ch);
        playBtn.onclick = () => playChapter(index, { restart: false });
        buttons.appendChild(playBtn);

        const restartBtn = document.createElement('button');
        restartBtn.textContent = 'Restart';
        restartBtn.className = 'secondary';
        restartBtn.onclick = () => playChapter(index, { restart: true });
        buttons.appendChild(restartBtn);
        footer.appendChild(buttons);

        wrapper.appendChild(footer);
        chaptersList.appendChild(wrapper);
      });
      updateChapterHighlight();
    }

    async function loadBooks() {
      const data = await fetchJSON('/api/books');
      state.books = data.books;
      renderBooks();
    }

    async function loadChapters(book) {
      state.currentBook = book;
      state.autoAdvance = false;
      state.currentChapterIndex = -1;
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

    function startStream(chapter, restart) {
      ensurePlayerVisible();
      nowPlaying.textContent = `${state.currentBook.title} — ${chapter.title}`;
      statusLine.textContent = `Buffering… (waiting for ${Math.max(1, chapter.live_prebuffer || 0)} chunks)`;
      const params = new URLSearchParams();
      if (restart) params.set('restart', '1');
      params.set('ts', Date.now().toString());
      player.src = `/api/books/${encodeURIComponent(state.currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/stream?${params.toString()}`;
      player.play().catch(() => {
        statusLine.textContent = 'Tap play to start audio.';
      });
    }

    function playChapter(index, { restart }) {
      if (!state.chapters.length) return;
      state.autoAdvance = true;
      state.currentChapterIndex = index;
      const chapter = state.chapters[index];
      startStream(chapter, restart);
      updateChapterHighlight();
    }

    function findChapterIndexForBookStart(restart) {
      if (!state.chapters.length) return 0;
      if (!restart) {
        const resumable = state.chapters.findIndex(ch => ch.resume_available);
        if (resumable !== -1) return resumable;
        const firstIncomplete = state.chapters.findIndex(ch => !ch.mp3_exists);
        if (firstIncomplete !== -1) return firstIncomplete;
      }
      return 0;
    }

    function playBook(restart = false) {
      if (!state.chapters.length) return;
      const index = findChapterIndexForBookStart(restart);
      playChapter(index, { restart });
    }

    player.addEventListener('playing', () => {
      statusLine.textContent = 'Playing';
    });
    player.addEventListener('waiting', () => {
      statusLine.textContent = 'Buffering…';
    });
    player.addEventListener('pause', () => {
      if (!player.ended) {
        statusLine.textContent = 'Paused';
      }
    });
    player.addEventListener('ended', () => {
      statusLine.textContent = 'Finished';
      if (state.autoAdvance && state.currentChapterIndex + 1 < state.chapters.length) {
        playChapter(state.currentChapterIndex + 1, { restart: false });
      } else {
        state.autoAdvance = false;
        if (state.currentBook) {
          loadChapters(state.currentBook);
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

    playBookBtn.onclick = () => playBook(false);
    restartBookBtn.onclick = () => playBook(true);

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
    mp3_exists = target.output.exists()
    cache_dir = _target_cache_dir(config.cache_dir, target)
    has_cache = cache_dir.exists()
    progress_chunks = None
    total_chunks = None
    resume_available = False

    if has_cache:
        progress_chunks = _safe_read_int(cache_dir / ".progress")
        total_chunks = _safe_read_int(cache_dir / ".complete")
        if progress_chunks is not None and progress_chunks <= 0:
            progress_chunks = None
        if (
            total_chunks is not None
            and progress_chunks is not None
            and progress_chunks >= total_chunks
        ):
            progress_chunks = None
        resume_available = progress_chunks is not None and not mp3_exists
    else:
        progress_chunks = None

    return {
        "id": chapter_path.name,
        "title": chapter_path.name,
        "index": index,
        "mp3_exists": mp3_exists,
        "has_cache": has_cache,
        "progress_chunks": progress_chunks,
        "total_chunks": total_chunks,
        "resume_available": resume_available,
        "live_prebuffer": max(1, config.live_prebuffer),
    }


def _encode_wav_to_mp3(wav_path: Path, ffmpeg_path: str) -> Iterable[bytes]:
    absolute = wav_path.resolve()
    cmd = [
        ffmpeg_path,
        "-y",
        "-i",
        str(absolute),
        "-f",
        "mp3",
        "-codec:a",
        "libmp3lame",
        "-qscale:a",
        "2",
        "pipe:1",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert proc.stdout is not None
        while True:
            chunk = proc.stdout.read(65536)
            if not chunk:
                break
            yield chunk
    finally:
        if proc.stdout is not None:
            proc.stdout.close()
        _, stderr = proc.communicate()
        if proc.returncode != 0:
            message = stderr.decode("utf-8", "ignore").strip()
            raise FFmpegError(f"ffmpeg streaming failed: {message}")


def create_app(config: WebConfig) -> FastAPI:
    root = config.root.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Books root not found: {root}")

    app = FastAPI(title="nk VoiceVox")
    app.state.config = config
    app.state.root = root
    app.state.voicevox_lock = asyncio.Lock()

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return INDEX_HTML

    @app.get("/api/books")
    def api_books() -> JSONResponse:
        books_payload = []
        for book_dir in _list_books(root):
            chapters = _list_chapters(book_dir)
            states = [
                _chapter_state(chapter, config, idx + 1) for idx, chapter in enumerate(chapters)
            ]
            total = len(states)
            completed = sum(1 for st in states if st["mp3_exists"])
            resumable = sum(1 for st in states if st["resume_available"])
            books_payload.append(
                {
                    "id": book_dir.name,
                    "title": book_dir.name,
                    "total_chapters": total,
                    "completed_chapters": completed,
                    "resume_chapters": resumable,
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
            _chapter_state(chapter, config, idx + 1) for idx, chapter in enumerate(chapters)
        ]
        summary = {
            "total": len(states),
            "completed": sum(1 for st in states if st["mp3_exists"]),
            "resume": sum(1 for st in states if st["resume_available"]),
        }
        return JSONResponse({"chapters": states, "summary": summary})

    @app.get("/api/books/{book_id}/chapters/{chapter_id}/stream")
    async def api_stream_chapter(
        book_id: str,
        chapter_id: str,
        restart: bool = Query(False),
    ):
        book_path = root / book_id
        chapter_path = book_path / chapter_id
        if not chapter_path.exists():
            raise HTTPException(status_code=404, detail="Chapter not found")

        target = TTSTarget(source=chapter_path, output=chapter_path.with_suffix(".mp3"))
        cache_dir = _target_cache_dir(config.cache_dir, target)
        progress_path = cache_dir / ".progress"

        if progress_path.exists() and not cache_dir.exists():
            progress_path.unlink(missing_ok=True)

        if restart and progress_path.exists():
            progress_path.unlink(missing_ok=True)

        if target.output.exists():
            # Stream the existing MP3; no need to re-synthesize.
            return FileResponse(
                target.output,
                media_type="audio/mpeg",
                filename=target.output.name,
            )

        lock: asyncio.Lock = app.state.voicevox_lock
        await lock.acquire()
        loop = asyncio.get_running_loop()
        runtime_hint = config.engine_runtime or discover_voicevox_runtime(config.engine_url)

        def stream_generator():
            stop_event = threading.Event()
            chunk_queue: queue.Queue[
                tuple[str, Path | VoiceVoxError | VoiceVoxUnavailableError | FFmpegError | None, threading.Event | None]
            ] = queue.Queue()
            prebuffer_target = max(1, config.live_prebuffer)
            buffer: list[tuple[Path, threading.Event]] = []
            streaming_started = False

            class StreamCancelled(RuntimeError):
                pass

            class StreamHandle:
                __slots__ = ("_done",)

                def __init__(self, done: threading.Event) -> None:
                    self._done = done

                def wait_done(self) -> None:
                    # Streaming handles playback scheduling; synthesis should not block.
                    return

            def playback_callback(chunk_path: Path):
                if stop_event.is_set():
                    raise StreamCancelled("stream cancelled")
                done_event = threading.Event()
                chunk_queue.put(("chunk", chunk_path, done_event))
                return StreamHandle(done_event)

            def worker():
                try:
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
                            _synthesize_target_with_client(
                                target,
                                client,
                                index=1,
                                total=1,
                                ffmpeg_path=config.ffmpeg_path,
                                overwrite=False,
                                progress=None,
                                cache_base=config.cache_dir,
                                keep_cache=config.keep_cache,
                                live_playback=True,
                                playback_callback=playback_callback,
                                live_prebuffer=config.live_prebuffer,
                            )
                        finally:
                            client.close()
                except StreamCancelled:
                    chunk_queue.put(("cancelled", None, None))
                except (VoiceVoxUnavailableError, VoiceVoxError, FFmpegError) as exc:
                    chunk_queue.put(("error", exc, None))
                except Exception as exc:  # pragma: no cover - unexpected failure
                    chunk_queue.put(("error", exc, None))
                finally:
                    chunk_queue.put(("done", None, None))

            threading.Thread(target=worker, daemon=True).start()

            try:
                while True:
                    item_type, payload, event = chunk_queue.get()
                    if item_type == "chunk":
                        buffer.append((payload, event))  # type: ignore[arg-type]
                        if not streaming_started:
                            if len(buffer) < prebuffer_target:
                                continue
                            streaming_started = True
                        while buffer:
                            chunk_path, done_event = buffer.pop(0)
                            try:
                                for block in _encode_wav_to_mp3(chunk_path, config.ffmpeg_path):
                                    yield block
                            finally:
                                done_event.set()
                    elif item_type == "error":
                        for _, pending_event in buffer:
                            pending_event.set()
                        buffer.clear()
                        error = payload
                        if isinstance(error, HTTPException):
                            raise error
                        raise RuntimeError(str(error))
                    elif item_type == "cancelled":
                        for _, pending_event in buffer:
                            pending_event.set()
                        buffer.clear()
                        return
                    elif item_type == "done":
                        break

                while buffer:
                    chunk_path, done_event = buffer.pop(0)
                    try:
                        for block in _encode_wav_to_mp3(chunk_path, config.ffmpeg_path):
                            yield block
                    finally:
                        done_event.set()
            finally:
                stop_event.set()
                if lock.locked():
                    loop.call_soon_threadsafe(lock.release)

        return StreamingResponse(
            stream_generator(),
            media_type="audio/mpeg",
            headers={"Cache-Control": "no-store"},
        )

    return app
