from __future__ import annotations

import asyncio
import queue
import shutil
import subprocess
import threading
import urllib.parse
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
    _merge_wavs_to_mp3,
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
    keep_cache: bool = False
    live_prebuffer: int = 2


INDEX_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>nk VoiceVox Stream</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Helvetica Neue", sans-serif; margin: 0; padding: 1.5rem; background: #111; color: #f1f1f1; }
    h1 { margin-top: 0; font-weight: 600; }
    .panel { background: #1f1f1f; border-radius: 12px; padding: 1rem 1.2rem; margin-bottom: 1.5rem; }
    .panel h2 { margin: 0 0 0.8rem 0; font-size: 1.1rem; letter-spacing: 0.02em; text-transform: uppercase; color: #bbb; }
    ul { list-style: none; padding: 0; margin: 0; }
    li { margin-bottom: 0.5rem; }
    button { background: #3b82f6; color: white; border: none; border-radius: 999px; padding: 0.45rem 1rem; font-size: 0.95rem; cursor: pointer; transition: background 0.2s; }
    button:hover { background: #2563eb; }
    button:disabled { background: #555; cursor: default; }
    .item { display: flex; justify-content: space-between; align-items: center; padding: 0.6rem 0.8rem; border-radius: 8px; background: #1b1b1b; }
    .item + .item { margin-top: 0.5rem; }
    .item span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; margin-right: 0.8rem; }
    .toolbar { display: flex; gap: 0.5rem; margin-top: 0.5rem; }
    audio { width: 100%; margin-top: 1rem; }
    .status { margin-top: 0.5rem; color: #9ca3af; font-size: 0.9rem; }
    @media (max-width: 640px) {
      body { padding: 1rem; }
      .item { flex-direction: column; align-items: flex-start; gap: 0.5rem; }
      .toolbar { width: 100%; flex-wrap: wrap; }
    }
  </style>
</head>
<body>
  <h1>nk VoiceVox Stream</h1>
  <div class="panel" id="books-panel">
    <h2>Books</h2>
    <ul id="books"></ul>
  </div>
  <div class="panel" id="chapters-panel" style="display:none;">
    <h2 id="chapters-title"></h2>
    <div class="toolbar" style="flex-wrap: wrap;">
      <button id="back-button">← Back to books</button>
      <button id="play-book">Play book</button>
      <button id="restart-book">Restart book</button>
    </div>
    <ul id="chapters" style="margin-top: 1rem;"></ul>
  </div>
  <div class="panel" id="player-panel" style="display:none;">
    <h2>Now Playing</h2>
    <div id="now-playing"></div>
    <audio id="player" controls autoplay></audio>
    <div class="status" id="status"></div>
  </div>

  <script>
    const booksEl = document.getElementById('books');
    const chaptersPanel = document.getElementById('chapters-panel');
    const chaptersTitle = document.getElementById('chapters-title');
    const chaptersEl = document.getElementById('chapters');
    const backButton = document.getElementById('back-button');
    const playBookBtn = document.getElementById('play-book');
    const restartBookBtn = document.getElementById('restart-book');
    const playerPanel = document.getElementById('player-panel');
    const player = document.getElementById('player');
    const nowPlayingEl = document.getElementById('now-playing');
    const statusEl = document.getElementById('status');

    let currentBook = null;
    let chaptersData = [];
    let currentChapterIdx = -1;
    let autoAdvance = false;

    async function fetchJSON(url) {
      const res = await fetch(url);
      if (!res.ok) throw new Error(await res.text());
      return res.json();
    }

    async function loadBooks() {
      const data = await fetchJSON('/api/books');
      booksEl.innerHTML = '';
      data.books.forEach(book => {
        const li = document.createElement('li');
        const div = document.createElement('div');
        div.className = 'item';
        const span = document.createElement('span');
        span.textContent = book.title;
        const btn = document.createElement('button');
        btn.textContent = 'Chapters';
        btn.onclick = () => loadChapters(book.id, book.title);
        div.appendChild(span);
        div.appendChild(btn);
        li.appendChild(div);
        booksEl.appendChild(li);
      });
    }

    async function loadChapters(bookId, title) {
      currentBook = { id: bookId, title };
      const data = await fetchJSON(`/api/books/${encodeURIComponent(bookId)}/chapters`);
      chaptersData = data.chapters;
      chaptersEl.innerHTML = '';
      chaptersTitle.textContent = title;
      chaptersPanel.style.display = 'block';
      playBookBtn.disabled = chaptersData.length === 0;
      restartBookBtn.disabled = chaptersData.length === 0;
      data.chapters.forEach(ch => {
        const li = document.createElement('li');
        const div = document.createElement('div');
        div.className = 'item';
        const span = document.createElement('span');
        span.textContent = ch.title;
        const toolbar = document.createElement('div');
        toolbar.className = 'toolbar';
        const playResume = document.createElement('button');
        playResume.textContent = ch.has_progress ? 'Resume' : 'Play';
        playResume.onclick = () => playChapter(data.chapters.indexOf(ch), false);
        toolbar.appendChild(playResume);
        const restartBtn = document.createElement('button');
        restartBtn.textContent = 'Restart';
        restartBtn.onclick = () => playChapter(data.chapters.indexOf(ch), true);
        toolbar.appendChild(restartBtn);
        div.appendChild(span);
        div.appendChild(toolbar);
        li.appendChild(div);
        chaptersEl.appendChild(li);
      });
    }

    function startStream(chapter, restart) {
      const params = new URLSearchParams();
      if (restart) params.set('restart', '1');
      nowPlayingEl.textContent = `${currentBook.title} — ${chapter.title}`;
      statusEl.textContent = 'Preparing...';
      playerPanel.style.display = 'block';
      player.src = `/api/books/${encodeURIComponent(currentBook.id)}/chapters/${encodeURIComponent(chapter.id)}/stream?${params.toString()}&ts=${Date.now()}`;
      player.play().catch(() => {
        statusEl.textContent = 'Tap play to begin audio.';
      });
      player.onplaying = () => { statusEl.textContent = 'Playing'; };
      player.onpause = () => { statusEl.textContent = 'Paused'; };
      player.onended = () => {
        if (autoAdvance && currentChapterIdx + 1 < chaptersData.length) {
          playChapter(currentChapterIdx + 1, false, true);
        } else {
          statusEl.textContent = 'Finished';
          loadChapters(currentBook.id, currentBook.title);
        }
      };
    }

    function playChapter(chapterIndex, restart, fromAuto = false) {
      if (!chaptersData.length) return;
      autoAdvance = true;
      currentChapterIdx = chapterIndex;
      const chapter = chaptersData[chapterIndex];
      startStream(chapter, restart);
    }

    function playBook(restart = false) {
      if (!chaptersData.length) return;
      let idx = chaptersData.findIndex(ch => ch.has_progress);
      if (idx === -1) idx = chaptersData.findIndex(ch => !ch.mp3_exists);
      if (idx === -1) idx = 0;
      autoAdvance = true;
      currentChapterIdx = idx;
      startStream(chaptersData[idx], restart);
    }

    playBookBtn.onclick = () => playBook(false);
    restartBookBtn.onclick = () => playBook(true);

    backButton.onclick = () => {
      chaptersPanel.style.display = 'none';
      currentBook = null;
      chaptersData = [];
      currentChapterIdx = -1;
      autoAdvance = false;
      player.pause();
      player.src = '';
    };

    loadBooks().catch(err => {
      booksEl.innerHTML = `<li>Error: ${err.message}</li>`;
    });
  </script>
</body>
</html>
"""


def _list_books(root: Path) -> list[Path]:
    books: list[Path] = []
    for entry in sorted(root.iterdir()):
        if entry.is_dir():
            if any(entry.glob("*.txt")):
                books.append(entry)
    return books


def _list_chapters(book_dir: Path) -> list[Path]:
    return [p for p in sorted(book_dir.glob("*.txt")) if p.is_file()]


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
    assert proc.stdout is not None
    try:
        for chunk in iter(lambda: proc.stdout.read(8192), b""):
            if not chunk:
                break
            yield chunk
    finally:
        proc.stdout.close()
        _, stderr = proc.communicate()
        if proc.returncode != 0:
            raise FFmpegError(f"ffmpeg streaming failed: {stderr.decode('utf-8', 'ignore').strip()}")


def create_app(config: WebConfig) -> FastAPI:
    root = config.root.resolve()
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
        books = []
        for book_dir in _list_books(root):
            books.append(
                {
                    "id": book_dir.name,
                    "title": book_dir.name,
                    "chapter_count": len(_list_chapters(book_dir)),
                }
            )
        return JSONResponse({"books": books})

    @app.get("/api/books/{book_id}/chapters")
    def api_chapters(book_id: str) -> JSONResponse:
        book_path = root / book_id
        if not book_path.is_dir():
            raise HTTPException(status_code=404, detail="Book not found")
        chapters_data = []
        for idx, chapter_path in enumerate(_list_chapters(book_path), start=1):
            target = TTSTarget(source=chapter_path, output=chapter_path.with_suffix(".mp3"))
            cache_dir = _target_cache_dir(config.cache_dir, target)
            has_progress = (cache_dir / ".progress").exists()
            chapters_data.append(
                {
                    "id": chapter_path.name,
                    "title": chapter_path.name,
                    "index": idx,
                    "has_progress": has_progress,
                    "mp3_exists": target.output.exists(),
                }
            )
        return JSONResponse({"chapters": chapters_data})

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
        marker_path = cache_dir / ".complete"
        if restart:
            target.output.unlink(missing_ok=True)
            if cache_dir.exists():
                shutil.rmtree(cache_dir, ignore_errors=True)
        elif progress_path.exists():
            pass  # resume automatically
        else:
            marker_path.unlink(missing_ok=True)

        if target.output.exists() and not restart and not progress_path.exists():
            return FileResponse(  # supports HTTP Range requests for smoother playback
                target.output,
                media_type="audio/mpeg",
                filename=target.output.name,
            )

        async with app.state.voicevox_lock:
            runtime_hint = config.engine_runtime or discover_voicevox_runtime(config.engine_url)
            if runtime_hint is None:
                raise HTTPException(status_code=500, detail="Could not locate VoiceVox runtime.")

            def stream_generator():
                chunk_queue: queue.Queue[tuple[Path, threading.Event] | None] = queue.Queue()
                played_chunks: list[Path] = []
                active_process: subprocess.Popen[bytes] | None = None

                class StreamHandle:
                    def __init__(self, done: threading.Event) -> None:
                        self._done = done

                    def wait_done(self) -> None:
                        self._done.wait()

                def playback_callback(chunk_path: Path):
                    done = threading.Event()
                    chunk_queue.put((chunk_path, done))
                    return StreamHandle(done)

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
                                    keep_cache=True,
                                    live_playback=True,
                                    playback_callback=playback_callback,
                                    live_prebuffer=config.live_prebuffer,
                                )
                            finally:
                                client.close()
                    except Exception as exc:  # pragma: no cover - surfaced downstream
                        chunk_queue.put(exc)
                    finally:
                        chunk_queue.put(None)

                threading.Thread(target=worker, daemon=True).start()

                while True:
                    item = chunk_queue.get()
                    if isinstance(item, Exception):
                        raise item
                    if item is None:
                        break
                    chunk_path, done_event = item
                    played_chunks.append(chunk_path)
                    if active_process is not None:
                        active_process.kill()
                        active_process.wait()
                    try:
                        active_process = subprocess.Popen(
                            [
                                config.ffmpeg_path,
                                "-y",
                                "-i",
                                str(chunk_path.resolve()),
                                "-f",
                                "mp3",
                                "-codec:a",
                                "libmp3lame",
                                "-qscale:a",
                                "2",
                                "pipe:1",
                            ],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                        assert active_process.stdout is not None
                        for data in iter(lambda: active_process.stdout.read(8192), b""):
                            if not data:
                                break
                            yield data
                        active_process.stdout.close()
                        _, stderr = active_process.communicate()
                        if active_process.returncode != 0:
                            raise FFmpegError(stderr.decode("utf-8", "ignore").strip())
                        active_process = None
                    finally:
                        done_event.set()

                if played_chunks:
                    _merge_wavs_to_mp3(
                        played_chunks,
                        target.output,
                        ffmpeg_path=config.ffmpeg_path,
                        overwrite=True,
                    )
                progress_path.unlink(missing_ok=True)
                if cache_dir.exists():
                    if config.keep_cache:
                        marker_path.write_text(str(len(played_chunks)), encoding="utf-8")
                    else:
                        shutil.rmtree(cache_dir, ignore_errors=True)
                progress_path.unlink(missing_ok=True)

            return StreamingResponse(stream_generator(), media_type="audio/mpeg")

    return app
