# nk

Convert Japanese EPUBs into TTS-ready text and narrate them with VoiceVox on macOS.

---

## 1. Prerequisites (macOS)

```bash
brew install p7zip ffmpeg jq
```

## 2. Install nk and Python dependencies

```bash
git clone https://github.com/huangziwei/nk
uv sync
nk tools install-unidic
```

> `nk tools install-unidic` downloads UniDic 3.1.1 (~1.8 GB) directly into `.venv/share/nk/unidic/` and points fugashi at it. Re-run the command if you recreate the virtualenv or supply `--zip /path/to/unidic-cwj-3.1.1-full.zip` for offline installs. Check the detected path any time with `nk tools unidic-status`.

## 3. Install the VoiceVox engine

1. Download `voicevox_engine-macos-x64-*.7z.001` from the [VoiceVox engine releases](https://github.com/VOICEVOX/voicevox_engine/releases).
2. Extract and prepare the runtime:

```bash
mkdir -p "$HOME/opt/voicevox"
cd "$HOME/Downloads"
7z x voicevox_engine-macos-x64-<VERSION>.7z.001 -o"$HOME/opt/voicevox"

cd "$HOME/opt/voicevox/macos-x64"
chmod +x run
```

nk auto-detects installs under `~/opt/voicevox/**`. If you keep the engine elsewhere, either pass `--engine-runtime /path/to/run` or export `NK_VOICEVOX_RUNTIME` in your shell. You can test manually with `./run --host 127.0.0.1 --port 50021`, but nk will launch and stop it for you.

---

## 4. Convert EPUB → TTS-friendly text

```bash
# Advanced mode (default): dictionary-verified ruby propagation
nk my_book.epub

# Fast mode: ruby evidence only
nk my_book.epub --mode fast

# Custom output name or per-chapter output
nk my_book.epub -o custom_name.txt
nk shelf/ --chapterized
```

Expect katakana-only output next to the source EPUB with duplicate titles stripped and line breaks preserved. Advanced mode consumes `fugashi + UniDic 3.1.1 + pykakasi`; fast mode requires no additional NLP setup.

---

## 5. Generate MP3s with VoiceVox

```bash
# Basic run (nk auto-starts VoiceVox at 127.0.0.1:50021)
nk tts output/

# Custom speaker, engine location, and parallelism
nk tts output/chapters --speaker 20 \
                       --engine-runtime "$HOME/opt/voicevox/macos-x64" \
                       --jobs 3
```

**Useful options**

| Option | Purpose |
| --- | --- |
| `--speaker N` | VoiceVox speaker ID (default 2, 四国めたん・セリフ). |
| `--pause SECONDS` | Trailing silence per chunk (default 0.4 s). |
| `--jobs N` | Parallel chapters (default: 1, pass 0 for auto up to 4 workers). |
| `--start-index M` | Skip the first `M-1` chapters and begin synthesis at chapter `M`. |
| `--engine-runtime PATH` | Point at a custom VoiceVox install or the `run` binary. |
| `--cache-dir DIR` | Store chunk caches elsewhere. |
| `--keep-cache` | Leave chunk WAVs on disk after MP3 synthesis. |
| `--overwrite` | Regenerate MP3s even if they already exist. |

**Resume after interruption** – nk caches every chunk under `.nk-tts-cache/<chapter-hash>/`. If you stop midway, rerun the same command (omit `--overwrite`) and synthesis resumes from the last unfinished chunk or merge. Delete MP3s (or use `--overwrite`) to regenerate everything.
- **Skip ahead** – add `--start-index N` to begin at chapter `N` without touching earlier files (helpful when you only need to regenerate later chapters).

---

## 6. Live playback (`--live`)

Stream chapters through your speakers while nk keeps synthesising chunks and writing MP3s in the background.

```bash
# Stream all chapters sequentially
nk tts output/ --live

# Buffer more chunks and jump to chapter 5
nk tts output/ --live --live-prebuffer 3 --live-start 5
```

- `--live-prebuffer N` (default 2) buffers N chunks before playback begins, then keeps synthesising ahead.
- `--live-start M` begins streaming at chapter index `M` (1-based), skipping earlier files.
- Live mode runs chapters sequentially (equivalent to `--jobs 1`) so audio stays ordered while synthesis continues ahead in the background.
- Stopping mid-chapter? nk records the last played chunk in `.progress`; rerun the command and playback resumes from there (it replays the interrupted chunk for continuity).
- MP3s are still written when playback finishes. Combine with `--keep-cache` if you also want to preserve the chunk WAVs.
- Need to reclaim disk space? `nk tts --clear-cache [path]` removes `.nk-tts-cache/` folders under the given path (defaults to the current directory). Use this after long runs if you didn’t enable `--keep-cache`.

---

## 7. VoiceVox tips

- Increase `--engine-runtime-wait` if the engine needs extra time to load models.
- Pass `--pause 0` to keep the engine’s default trailing silence.
- Override `NK_VOICEVOX_RUNTIME` or use `--engine-runtime` if nk can’t find your install.

---

## 8. Web playback service (`nk web`)

Serve your chapterized books over HTTP and stream them from a phone or tablet on the same network.

1. Chapterize your EPUBs if you haven’t already: `nk my_book.epub --chapterized` (creates `output/my_book/*.txt`).
2. Start the server on your Mac:
   ```bash
   nk web output/ --host 0.0.0.0 --port 2046
   ```
   Options mirror `nk tts` (speaker, engine runtime, cache directory, etc.).
3. On your phone, open Safari and visit `http://<your-mac-ip>:2046`.
4. Tap a book → choose a chapter → press play. You can resume or restart individual chapters, and Safari will stream the MP3 while nk continues synthesising and caching in the background.

> The web service uses the same chunk cache/resume logic as the CLI. Stopping playback mid-chapter and tapping “Resume” picks up exactly where you left off. A final MP3 is written when playback completes.

---

## 9. Troubleshooting

| Symptom | Fix |
| --- | --- |
| `simpleaudio` missing | Install it: `pip install simpleaudio`. |
| VoiceVox unavailable | Ensure `~/opt/voicevox/.../run` exists or pass `--engine-runtime`. |
| MP3 skipped | Remove the file or add `--overwrite`. |
| Need a clean slate | Delete `.nk-tts-cache/` (or run with `--overwrite`). |
| Want to inspect chunks | Use `--keep-cache` to leave WAVs in `.nk-tts-cache/<chapter-hash>/`. |

---

## 10. Command reference

```
# EPUB → TXT
nk book.epub [--mode advanced|fast] [--chapterized] [-o output.txt]

# TXT → MP3 (batch)
nk tts book.txt|directory [--speaker N]
                          [--engine-runtime PATH]
                          [--jobs N]
                          [--pause SECONDS]
                          [--cache-dir DIR]
                          [--keep-cache]
                          [--overwrite]

# Live playback (still writes MP3s)
nk tts chapters/ --live [--live-prebuffer N] [--live-start M]

# Web service (browse + stream)
nk web output/ [--host HOST] [--port PORT] [--speaker N] [...]

# WebDAV share (Flacbox, etc.)
nk dav books/ [--host HOST] [--port PORT] [--auth pam-login]

# Environment: NK_VOICEVOX_RUNTIME=/absolute/path/to/run

# UniDic helper commands

nk tools install-unidic [--zip /path/to/unidic-cwj-3.1.1-full.zip]
nk tools unidic-status

- `install-unidic` downloads/extracts the official `unidic-cwj-3.1.1-full.zip` archive **into the current virtualenv** and sets it as the default dictionary for fugashi.
- `unidic-status` prints the managed path and any `NK_UNIDIC_DIR` override so you can confirm which dictionary is active.

- `nk dav` exposes only `.mp3` files via WebDAV using your macOS login (PAM). Point clients such as Flacbox at `http://<your-mac-ip>:PORT/` to stream your nk library without copying files.
```
