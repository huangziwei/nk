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
