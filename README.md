# nk

Convert Japanese EPUBs into TTS-friendly plain text.

## Usage

```bash
uv tool install git+https://github.com/huangziwei/nk
nk path/to/book.epub              # advanced mode (default, requires SudachiPy)
# optional: nk book.epub -o custom.txt
# fast mode (ruby-only heuristics): nk book.epub --mode fast
```

## What to Expect

- Plain text written next to the source EPUB in reading order
- Kanji bases stripped; ruby readings propagated and converted to katakana (strictness depends on `--mode`)
- HTML, styling, and duplicate title noise removed

## Modes & Requirements

- `fast`: Uses only ruby annotations in the EPUB plus conservative heuristics. May miss unannotated kanji.
- `advanced` (default): Requires `sudachipy` and `sudachidict_core`. Prefers verified ruby readings when they agree with Sudachi (or appear repeatedly), and falls back to Sudachi for everything else so every kanji is read.

Install the NLP dependencies with:

```bash
uv pip install sudachipy sudachidict_core
```
