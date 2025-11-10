# nk

Convert Japanese EPUBs into TTS-ready text and narrate them with VoiceVox on macOS.

---

## 1. Clone & bootstrap (macOS)

```bash
git clone https://github.com/huangziwei/nk
cd nk
./install.sh
```

`install.sh` assumes you already have `brew`, `curl`, and `uv` on your path. It will:

- install/upgrade `p7zip`, `ffmpeg`, and `jq` via Homebrew.
- run `uv sync` to create/update `.venv` and install all Python deps.
- call `uv run nk tools install-unidic` so fugashi can see UniDic 3.1.1.
- fetch the latest VoiceVox engine release from GitHub, log the tag it grabbed, and unpack it under `${VOICEVOX_ROOT:-$HOME/opt/voicevox}/macos-x64` (with the tag recorded in `.nk-voicevox-version` under that directory; `cat "$HOME/opt/voicevox/macos-x64/.nk-voicevox-version"` to check later).

> Regenerate the UniDic data any time you recreate the virtualenv with `uv run nk tools install-unidic`.

Environment knobs:

- `NK_SKIP_VOICEVOX=1 ./install.sh` &rarr; skip the VoiceVox download.
- `VOICEVOX_VERSION=v0.15.4 ./install.sh` &rarr; pin to a specific release tag.
- `VOICEVOX_URL=https://.../voicevox_engine-macos-x64-*.7z.001 ./install.sh` &rarr; use a pre-downloaded asset URL.
- `VOICEVOX_FORCE=1 ./install.sh` &rarr; overwrite an existing install.
- `VOICEVOX_ROOT=/custom/path ./install.sh` &rarr; choose a different install prefix.

Once the script finishes you can either activate the virtualenv or run nk via uv directly:

```bash
source .venv/bin/activate
nk my_book.epub

# or stay outside the venv
uv run nk my_book.epub
```

## 2. Install the VoiceVox engine manually (optional)

`install.sh` already installs the latest VoiceVox runtime. If you prefer to manage it yourself, grab `voicevox_engine-macos-x64-*.7z.001` from the [VoiceVox engine releases](https://github.com/VOICEVOX/voicevox_engine/releases) and extract it:

```bash
mkdir -p "$HOME/opt/voicevox"
cd "$HOME/Downloads"
7z x voicevox_engine-macos-x64-<VERSION>.7z.001 -o"$HOME/opt/voicevox"

cd "$HOME/opt/voicevox/macos-x64"
chmod +x run
```

nk auto-detects installs under `~/opt/voicevox/**`. If you keep the engine elsewhere, either pass `--engine-runtime /path/to/run` or export `NK_VOICEVOX_RUNTIME` in your shell. You can test manually with `./run --host 127.0.0.1 --port 50021`, but nk will launch and stop it for you.

---

## 3. Convert EPUB → TTS-friendly text

```bash
# Default (per-chapter .txt files)
nk my_book.epub

# Fast mode: ruby evidence only
nk my_book.epub --mode fast

# Batch chapterize an entire shelf of EPUBs
nk shelf/

# Single-file export (legacy behavior)
nk my_book.epub --single-file -o custom_name.txt
```

Expect katakana-only output next to the source EPUB with duplicate titles stripped and line breaks preserved. Advanced mode consumes `fugashi + UniDic 3.1.1 + pykakasi`; fast mode requires no additional NLP setup.

Each chapterized book now carries a `.nk-book.json` manifest plus an extracted (and automatically square-padded) `cover.jpg|png`. The manifest tracks the original/reading titles for every chapter and records the book author so `nk tts` can build accurate ID3 tags; the cover is embedded into every MP3 automatically. Advanced runs also emit `<chapter>.txt.pitch.json` files that capture UniDic pitch-accent metadata so `nk tts` can force the correct VoiceVox tone (雨 vs 飴) without extra setup. Re-run `nk <book>.epub` if you have older chapter folders and want to backfill the metadata/cover bundle.

---

## 4. Generate MP3s with VoiceVox

```bash
# Basic run (nk auto-starts VoiceVox at 127.0.0.1:50021)
nk tts output/

# One-shot: chapterize + synthesize directly from an EPUB
nk tts books/novel.epub --mode fast --speaker 20

# Custom speaker, engine location, and parallelism
nk tts output/chapters --speaker 20 \
                       --engine-runtime "$HOME/opt/voicevox/macos-x64" \
                       --jobs 3
```

**Useful options**

| Option | Purpose |
| --- | --- |
| `--speaker N` | VoiceVox speaker ID (default 2, 四国めたん・セリフ). |
| `--speed SCALE` | Override VoiceVox `speedScale` (e.g., 0.9 slows speech). Defaults to the engine preset. |
| `--pitch SCALE` | Override VoiceVox `pitchScale` (e.g., -0.1 lowers the voice). Defaults to the engine preset. |
| `--intonation SCALE` | Override VoiceVox `intonationScale` (e.g., 1.1 adds more variation). Defaults to the engine preset. |
| `--pause SECONDS` | Trailing silence per chunk (default 0.4 s). |
| `--jobs N` | Parallel chapters (default: 1, pass 0 for auto up to 4 workers). |
| `--start-index M` | Skip the first `M-1` chapters and begin synthesis at chapter `M`. |
| `--engine-runtime PATH` | Point at a custom VoiceVox install or the `run` binary. |
| `--engine-threads N` | When nk auto-starts VoiceVox, set the engine’s CPU thread count (default: engine-decided). |
| `--cache-dir DIR` | Store chunk caches elsewhere. |
| `--keep-cache` | Leave chunk WAVs on disk after MP3 synthesis. |
| `--overwrite` | Regenerate MP3s even if they already exist. |
| `--mode fast/advanced` | When an EPUB is passed to `nk tts`, choose the propagation engine used to chapterize before synthesis (default: advanced). |

**Resume after interruption** – nk caches every chunk under `.nk-tts-cache/<chapter-hash>/`. If you stop midway, rerun the same command (omit `--overwrite`) and synthesis resumes from the last unfinished chunk or merge. Delete MP3s (or use `--overwrite`) to regenerate everything.
- **Skip ahead** – add `--start-index N` to begin at chapter `N` without touching earlier files (helpful when you only need to regenerate later chapters).

---

## 4.1 Build an M4B (optional)

nk drops two helper files next to every chapterized book:

- `.nk-book.json` – structured metadata for nk itself (titles, author, track counts, etc.).
- `<chapter>.txt.pitch.json` – optional per-chapter pitch-accent metadata (advanced mode) that lets `nk tts` override VoiceVox accent phrases for homographs.
- `m4b.json` – directly consumable by [m4b-tool](https://github.com/sandreas/m4b-tool) with every MP3 listed in order, chapter labels, and the padded cover.

### Manual pitch overrides

If MeCab splits a word incorrectly or assigns the wrong accent, add a `custom_pitch.json` next to the book directory:

```json
{
  "overrides": [
    {
      "pattern": "ソガシャハジ",
      "replacement": "ソガノシャチ",
      "reading": "ソガノシャチ",
      "accent": 1,
      "pos": "名詞"
    },
    {
      "pattern": "クラウゼル",
      "reading": "クラウゼル",
      "accent": 2
    }
  ]
}
```

- `pattern` matches against the katakana `.txt` output (set `"regex": true` if you need a regular expression).
- `replacement` rewrites the `.txt` text before re-tokenizing (omit it for pitch-only fixes).
- `reading`/`accent` describe the curated pronunciation to inject into `.pitch.json` (defaults to `replacement` when omitted).

Apply the overrides with:

```bash
nk refine "books/novel/"
```

The command rewrites affected `.txt` files, updates `.pitch.json` with your curated tokens, and recomputes the hashes so subsequent `nk tts` runs use the corrected readings.


If you already have m4b-tool installed, you can jump straight from chapterized MP3s to a single M4B:

```bash
# After running `nk tts books/novel.epub --speaker 20`
m4b-tool merge \
  --audio-book-json "books/novel/m4b.json" \
  --output "books/novel/novel.m4b"
```

m4b-tool automatically reads the cover path and chapter names from `m4b.json`, so you usually don’t need to pass extra tagging flags (add `--jobs` or `--use-fdk-aac` if you want higher-quality AAC encoding). Re-run `nk <book>.epub` if your existing chapter folders predate this manifest; it will regenerate the metadata and cover bundle without touching your EPUB.

---

## 5. Live playback (`--live`)

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

## 6. VoiceVox tips

- Increase `--engine-runtime-wait` if the engine needs extra time to load models.
- Pass `--pause 0` to keep the engine’s default trailing silence.
- Override `NK_VOICEVOX_RUNTIME` or use `--engine-runtime` if nk can’t find your install.

---

## 7. Web playback service (`nk web`)

Serve your chapterized books over HTTP and stream them from a phone or tablet on the same network.

1. Chapterize your EPUBs if you haven’t already: `nk my_book.epub` (creates `output/my_book/*.txt` by default).
2. Start the server on your Mac:
   ```bash
   nk web output/ --host 0.0.0.0 --port 2046
   ```
   Options mirror `nk tts` (speaker, engine runtime, cache directory, etc.).
3. On your phone, open Safari and visit `http://<your-mac-ip>:2046`.
4. Tap a book → choose a chapter → press play. You can resume or restart individual chapters, and Safari will stream the MP3 while nk continues synthesising and caching in the background.

> The web service uses the same chunk cache/resume logic as the CLI. Stopping playback mid-chapter and tapping “Resume” picks up exactly where you left off. A final MP3 is written when playback completes. The server will chapterize missing books on demand, just like `nk tts` does when you point it directly at an EPUB.

---

## 8. Troubleshooting

| Symptom | Fix |
| --- | --- |
| `simpleaudio` missing | Install it: `pip install simpleaudio`. |
| VoiceVox unavailable | Ensure `~/opt/voicevox/.../run` exists or pass `--engine-runtime`. |
| MP3 skipped | Remove the file or add `--overwrite`. |
| Need a clean slate | Delete `.nk-tts-cache/` (or run with `--overwrite`). |
| Want to inspect chunks | Use `--keep-cache` to leave WAVs in `.nk-tts-cache/<chapter-hash>/`. |

---

## 9. Command reference

```
# EPUB → TXT (per-chapter by default)
nk book.epub [--mode advanced|fast] [--single-file] [-o output.txt]

# TXT → MP3 (batch)
nk tts book.txt|directory [--speaker N]
                          [--speed SCALE]
                          [--pitch SCALE]
                          [--intonation SCALE]
                          [--engine-runtime PATH]
                          [--jobs N]
                          [--pause SECONDS]
                          [--cache-dir DIR]
                          [--keep-cache]
                          [--overwrite]

# Live playback (still writes MP3s)
nk tts chapters/ --live [--live-prebuffer N] [--live-start M]

# Web service (browse + stream)
nk web output/ [--host HOST] [--port PORT] [--speaker N]
               [--speed SCALE] [--pitch SCALE] [--intonation SCALE] [...]

# WebDAV share (Flacbox, etc.)
nk dav books/ [--host HOST] [--port PORT] [--auth pam-login]

# Environment: NK_VOICEVOX_RUNTIME=/absolute/path/to/run

# UniDic helper commands

nk tools install-unidic [--zip /path/to/unidic-cwj-3.1.1-full.zip]
nk tools unidic-status

- `install-unidic` downloads/extracts the official `unidic-cwj-3.1.1-full.zip` archive **into the current virtualenv** and sets it as the default dictionary for fugashi.
- `unidic-status` prints the managed path and any `NK_UNIDIC_DIR` override so you can confirm which dictionary is active.

- `nk dav` exposes only `.mp3` files via WebDAV using your macOS login (PAM) and mirrors new MP3s as they are added under `books/`. Point clients such as Flacbox at `http://<your-mac-ip>:PORT/` to stream your nk library without copying files.
> Note: `-o/--output-name` is only honored when `--single-file` is provided.
```
