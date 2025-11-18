# nk – Full Guide

Convert Japanese EPUBs into TTS-ready text and narrate them with VoiceVox on macOS or Ubuntu.

---

## 1. Clone & bootstrap (macOS / Ubuntu)

```bash
git clone https://github.com/huangziwei/nk
cd nk
./install.sh
```

`install.sh` auto-detects your platform (macOS or Ubuntu). On macOS it requires [Homebrew](https://brew.sh/) and installs everything (including `uv`) via `brew`. On Ubuntu it uses `apt-get` for the runtime dependencies (and will happily use Linuxbrew instead if `brew` is already on your `PATH`), then installs `uv` via [the official installer](https://astral.sh/uv).

It will:

- install/upgrade the runtime dependencies (`curl`, `ffmpeg`, `jq`, `p7zip`, `uv`) via the detected package manager.
- run `uv sync` to create/update `.venv` and install all Python deps.
- download UniDic 3.1.1 into `${UNIDIC_ROOT:-$HOME/opt/unidic}/3.1.1` so fugashi can see it immediately.
- fetch the latest VoiceVox engine release from GitHub, log the tag it grabbed, and unpack it under `${VOICEVOX_ROOT:-$HOME/opt/voicevox}/$VOICEVOX_TARGET` (with the tag recorded in `.nk-voicevox-version`; `cat "$HOME/opt/voicevox/$VOICEVOX_TARGET/.nk-voicevox-version"` to check later).

> On macOS the default VoiceVox target is `macos-x64`; on Ubuntu it is `linux-cpu-x64`. Set `VOICEVOX_TARGET=linux-gpu-x64` (and optionally `VOICEVOX_ASSET_PATTERN`) if you prefer the GPU build. Run `install.sh` with `sudo` (or as root) on Ubuntu so `apt-get` can install the required packages.

> Need to refresh UniDic? Re-run `install.sh` (set `UNIDIC_FORCE=1` if you want to overwrite the existing copy).

Environment knobs:

- `NK_SKIP_UNIDIC=1 ./install.sh` &rarr; skip the UniDic download.
- `UNIDIC_ROOT=/custom/path ./install.sh` &rarr; install UniDic somewhere else.
- `UNIDIC_FORCE=1 ./install.sh` &rarr; overwrite an existing UniDic install.
- `UNIDIC_ZIP=/path/to/unidic.zip ./install.sh` &rarr; reuse a pre-downloaded archive instead of fetching it.
- `NK_SKIP_VOICEVOX=1 ./install.sh` &rarr; skip the VoiceVox download.
- `VOICEVOX_VERSION=v0.15.4 ./install.sh` &rarr; pin to a specific release tag.
- `VOICEVOX_URL=https://.../voicevox_engine-macos-x64-*.7z.001 ./install.sh` &rarr; use a pre-downloaded macOS asset (on Ubuntu, set `VOICEVOX_TARGET=linux-cpu-x64` and point `VOICEVOX_URL` at the matching `voicevox_engine-linux-cpu-x64-*.7z.001` archive instead).
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

`install.sh` already installs the latest VoiceVox runtime. If you prefer to manage it yourself, grab the appropriate archive (`voicevox_engine-macos-x64-*.7z.001` on macOS or `voicevox_engine-linux-cpu-x64-*.7z.001` on Ubuntu) from the [VoiceVox engine releases](https://github.com/VOICEVOX/voicevox_engine/releases) and extract it:

```bash
TARGET="macos-x64"  # use linux-cpu-x64 (or linux-gpu-x64) on Ubuntu

mkdir -p "$HOME/opt/voicevox"
cd "$HOME/Downloads"
7z x voicevox_engine-${TARGET}-<VERSION>.7z.001 -o"$HOME/opt/voicevox"

cd "$HOME/opt/voicevox/${TARGET}"
chmod +x run
```

nk auto-detects installs under `~/opt/voicevox/**`. If you keep the engine elsewhere, either pass `--engine-runtime /path/to/run` or export `NK_VOICEVOX_RUNTIME` in your shell. You can test manually with `./run --host 127.0.0.1 --port 50021`, but nk will launch and stop it for you.

---

## 3. Convert EPUB → TTS-friendly text

```bash
# Default (per-chapter .txt files)
nk my_book.epub

# Batch chapterize an entire shelf of EPUBs
nk shelf/
```

Expect katakana-only output next to the source EPUB with duplicate titles stripped and line breaks preserved. nk always runs the advanced propagation engine, which consumes `fugashi + UniDic 3.1.1 + pykakasi` to confirm rubies and fill in missing readings.

Each chapterized book now carries a `.nk-book.json` manifest plus an extracted (and automatically square-padded) `cover.jpg|png`. The manifest tracks the original/reading titles for every chapter and records the book author so `nk tts` can build accurate ID3 tags; the cover is embedded into every MP3 automatically. Advanced runs also emit `<chapter>.txt.token.json` files that list **every** transformed token (surface, reading, offsets, sources and accent metadata when available), so you can audit ruby/UniDic conversions and hand-tune any accent lines before synth. Re-run `nk <book>.epub` if you have older chapter folders and want to backfill the metadata/cover bundle.

---

## 4. Generate MP3s with VoiceVox

```bash
# Basic run (nk auto-starts VoiceVox at 127.0.0.1:50021)
nk tts output/

# One-shot: chapterize + synthesize directly from an EPUB
nk tts books/novel.epub --speaker 20

# Custom speaker, engine location, and parallelism
# (point --engine-runtime at ~/opt/voicevox/macos-x64 on macOS or ~/opt/voicevox/linux-cpu-x64 on Ubuntu)
nk tts output/chapters --speaker 20 \
                       --engine-runtime "$HOME/opt/voicevox/macos-x64" \
                       --jobs 3
```

**Useful options**

| Option | Purpose |
| --- | --- |
| `--speaker N` | VoiceVox speaker ID (defaults to the saved per-book value, falling back to 2 / 四国めたん・セリフ). |
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

**Resume after interruption** – nk caches every chunk under `.nk-tts-cache/<chapter-hash>/`. If you stop midway, rerun the same command (omit `--overwrite`) and synthesis resumes from the last unfinished chunk or merge. Delete MP3s (or use `--overwrite`) to regenerate everything.
- **Skip ahead** – add `--start-index N` to begin at chapter `N` without touching earlier files (helpful when you only need to regenerate later chapters).

### Remember per-book voice settings

- `nk tts` remembers the last `--speaker/--speed/--pitch/--intonation` overrides you use for a book by storing them in that book’s `.nk-book.json`.
- Future runs without those flags automatically reuse the saved values (falling back to speaker `2` and engine defaults otherwise).
- When you rely on VoiceVox defaults, nk captures the actual numeric values and stores them in `tts_defaults` so future runs show the precise baseline you used.
- Re-run `nk tts` with new options or edit `.nk-book.json` to change or clear the remembered values.

---

## 4.1 Build an M4B (optional)

nk drops two helper files next to every chapterized book:

- `.nk-book.json` – structured metadata for nk itself (titles, author, track counts, etc.).
- `tts_defaults` inside `.nk-book.json` records the last speaker/speed/pitch/intonation nk used (whether from CLI or engine defaults).
- `<chapter>.txt.token.json` – per-chapter token log (advanced mode) containing every kanji→kana conversion plus accent metadata so `nk tts` (or you) can override VoiceVox phrasing.
- `m4b.json` – directly consumable by [m4b-tool](https://github.com/sandreas/m4b-tool) with every MP3 listed in order, chapter labels, and the padded cover.

### Manual pitch overrides

If MeCab splits a word incorrectly or assigns the wrong accent, add a `custom_token.json` next to the book directory:

```json
{
  "overrides": [
    {
      "pattern": "テイアラ",
      "replacement": "ティアラ",
      "reading": "ティアラ",
      "accent": 2,
      "surface": "天愛星"
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
- `reading`/`accent` describe the curated pronunciation to inject into `.token.json` (defaults to `replacement` when omitted). You can now target any token because the file carries the full transformed list.
- `surface` lets you keep the kanji label (“天愛星”) in the pitch metadata even if the katakana body uses the ruby form.

Apply the overrides with:

```bash
nk refine "books/novel/"
```

The command rewrites affected `.txt` files, updates `.token.json` with your curated tokens, and recomputes the hashes so subsequent `nk tts` runs use the corrected readings.


If you already have m4b-tool installed, you can jump straight from chapterized MP3s to a single M4B:

```bash
# After running `nk tts books/novel.epub --speaker 20`
m4b-tool merge \
  --audio-book-json "books/novel/m4b.json" \
  --output "books/novel/novel.m4b"
```

m4b-tool automatically reads the cover path and chapter names from `m4b.json`, so you usually don’t need to pass extra tagging flags (add `--jobs` or `--use-fdk-aac` if you want higher-quality AAC encoding). Re-run `nk <book>.epub` if your existing chapter folders predate this manifest; it will regenerate the metadata and cover bundle without touching your EPUB.

---

## 5. VoiceVox tips

- Increase `--engine-runtime-wait` if the engine needs extra time to load models.
- Pass `--pause 0` to keep the engine’s default trailing silence.
- Override `NK_VOICEVOX_RUNTIME` or use `--engine-runtime` if nk can’t find your install.

---

## 6. Web playback service (`nk play`)

Serve your chapterized books over HTTP and stream them from a phone or tablet on the same network.

1. Chapterize your EPUBs if you haven’t already: `nk my_book.epub` (creates `output/my_book/*.txt` by default).
2. Start the server on your Mac:
   ```bash
   nk play output/ --host 0.0.0.0 --port 2046
   ```
   Options mirror `nk tts` (speaker, engine runtime, cache directory, etc.).
3. On your phone, open Safari and visit `http://<your-mac-ip>:2046`.
4. Tap a book → choose a chapter → press play. You can resume or restart individual chapters, and Safari will stream the MP3 while nk continues synthesising and caching in the background.

> The player service uses the same chunk cache/resume logic as the CLI. Stopping playback mid-chapter and tapping “Resume” picks up exactly where you left off. A final MP3 is written when playback completes.

---

## 7. Troubleshooting

| Symptom | Fix |
| --- | --- |
| VoiceVox unavailable | Ensure `~/opt/voicevox/.../run` exists or pass `--engine-runtime`. |
| MP3 skipped | Remove the file or add `--overwrite`. |
| Need a clean slate | Delete `.nk-tts-cache/` (or run with `--overwrite`). |
| Want to inspect chunks | Use `--keep-cache` to leave WAVs in `.nk-tts-cache/<chapter-hash>/`. |

---

## 8. Command reference

```
# EPUB → TXT (per-chapter bundle)
nk book.epub

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

# Player service (browse + stream)
nk play output/ [--host HOST] [--port PORT] [--speaker N]
               [--speed SCALE] [--pitch SCALE] [--intonation SCALE] [...]

# WebDAV share (Flacbox, etc.)
nk dav books/ [--host HOST] [--port PORT] [--auth pam-login]

# Environment: NK_VOICEVOX_RUNTIME=/absolute/path/to/run

# Dependency audit
nk deps

- `nk deps` prints the detected UniDic, VoiceVox, and ffmpeg installations so you can confirm versions/paths quickly.
- `nk dav` exposes only `.mp3` files via WebDAV using your macOS login (PAM) and mirrors new MP3s as they are added under `books/`. Point clients such as Flacbox at `http://<your-mac-ip>:PORT/` to stream your nk library without copying files.
```

---

## 10. Uninstall

Everything nk installs lives either in the repo itself or under your home directory, so removal is just deleting those directories:

1. Remove the project (and its virtualenv, cache, etc.):
   ```bash
   rm -rf /path/to/nk
   ```
2. Remove the nk-managed VoiceVox runtime (if you’re not sharing the install with other tools):
   ```bash
   rm -rf "$HOME/opt/voicevox"
   ```

> nk installs Homebrew/apt packages only if they’re missing. If you added them specifically for nk, remove them manually via `brew uninstall ...` or `sudo apt remove ...`. Otherwise, leave them installed for other projects.
