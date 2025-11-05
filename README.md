# nk

Convert Japanese EPUBs into TTS-friendly plain text.

## Usage

1. Install the CLI (uv keeps it isolated):

   ```bash
   uv tool install git+https://github.com/huangziwei/nk
   ```

2. Install the NLP dependencies (full UniDic for best accuracy):

   ```bash
   uv pip install fugashi pykakasi unidic
   python -m unidic download
   ```

   > Prefer a lighter download? Replace `unidic` with `"fugashi[unidic-lite]"`. Accuracy will drop on rare words, but setup is faster.

3. Convert books:

   ```bash
   nk path/to/book.epub              # advanced mode (default)
   nk book.epub --mode fast          # ruby-only heuristics
   nk book.epub -o custom.txt        # custom output name
   ```

## What to Expect

- Plain text written next to the source EPUB in reading order
- Kanji bases stripped; ruby readings propagated and converted to katakana (strictness depends on `--mode`)
- HTML, styling, and duplicate title noise removed

## Modes & Requirements

- `fast`: Uses only ruby annotations in the EPUB plus conservative heuristics. May miss unannotated kanji.
- `advanced` (default): Requires `fugashi` (MeCab) with UniDic and `pykakasi`. Prefers verified ruby readings when they agree with the dictionary (or appear repeatedly), and replaces every remaining kanji with katakana readings.

## TTS with VoiceVox

- `nk tts path/to/texts` renders `.txt` files into MP3s via a running VoiceVox HTTP engine (default `http://127.0.0.1:50021`).
- Default speaker is 四国めたん・セリフ (ID 2); pass `--speaker` for another preset.
- nk auto-detects bundled VoiceVox releases under `~/opt/voicevox/**`; override with `--engine-runtime` (or `NK_VOICEVOX_RUNTIME`) to point at a custom install. nk launches the engine on demand, waits for readiness, and tears it down when finished.
  ```bash
  nk tts out/book.txt --engine-runtime "$HOME/opt/voicevox/macos-x64"
  ```
- Adjust `--engine-runtime-wait` if the engine takes longer than 30 seconds to load models.
- Use `--pause` (seconds) to stretch trailing silence per chunk when you need clearer separation between sentences.
- Pass `--jobs` to enable parallel chapter synthesis (default auto-selects a small worker pool).
- nk caches synthesized WAV chunks under `.nk-tts-cache/` (or `--cache-dir`) so you can stop and resume mid-chapter; rerun the same command without `--overwrite` and nk reuses any completed chunks. Add `--keep-cache` if you want to retain the WAVs after success.
- `--live` streams each chunk directly through your speakers (requires `pip install simpleaudio`); caches still update so you can pause and resume playback later. Tune `--live-prebuffer` (default 2 chunks) if you want more headroom before playback starts. Live mode runs chapters sequentially (equivalent to `--jobs 1`) so your audio never overlaps.

### Installing VoiceVox (macOS example)

1. Install prerequisites (7-Zip for extracting the release archive, ffmpeg for MP3 encoding):
   ```bash
   brew install p7zip ffmpeg jq
   ```
2. Download the latest macOS VoiceVox engine archive from the [official releases](https://github.com/VOICEVOX/voicevox_engine/releases). The macOS build ships as split `.7z` files.
3. Extract the payload into `~/opt/voicevox` (create the directory if it does not exist):
   ```bash
   mkdir -p "$HOME/opt/voicevox"
   cd "$HOME/Downloads"
   7z x voicevox_engine-macos-x64-<VERSION>.7z.001 -o"$HOME/opt/voicevox"
   ```
4. Mark the launcher executable and test it manually (optional—nk will start it for you):
   ```bash
   cd "$HOME/opt/voicevox/macos-x64"
   chmod +x run
   ./run --host 127.0.0.1 --port 50021
   ```
5. Stop the engine (Ctrl+C) once you see it listening; nk will take it from there. If you keep VoiceVox in a custom location, add `export NK_VOICEVOX_RUNTIME=/path/to/run` to your shell profile so nk can find it.
