# nk

Convert Japanese EPUBs into TTS-friendly plain text.

## Usage

```bash
uv tool install git+https://github.com/huangziwei/nk
nk path/to/book.epub
# optional: nk book.epub -o custom.txt
# accuracy modes:
# fast (default): nk book.epub
# slow (requires SudachiPy): nk book.epub --mode slow
# advanced (full reading replacement, requires SudachiPy): nk book.epub --mode advanced
```

## What to Expect

- Plain text written next to the source EPUB in reading order
- Kanji bases stripped; ruby readings propagated and converted to katakana (strictness depends on `--mode`)
- HTML, styling, and duplicate title noise removed

## Modes & Requirements

- `fast` (default): Uses only ruby annotations in the EPUB plus conservative heuristics. May miss unannotated kanji.
- `slow`: Requires `sudachipy` and `sudachidict_core`. Verifies ruby readings against Sudachi's dictionary before propagating.
- `advanced`: Requires `sudachipy` and `sudachidict_core`. Replaces every kanji with Sudachi-derived readings, using ruby where available for guaranteed accuracy.

Install the NLP dependencies with:

```bash
uv pip install sudachipy sudachidict_core
```
