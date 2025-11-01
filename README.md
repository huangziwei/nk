# nk

Convert Japanese EPUBs into TTS-friendly plain text.

## Usage

```bash
uvx --force git+https://github.com/huangziwei/nk
nk path/to/book.epub
# optional: nk book.epub -o custom.txt
```

## What to Expect

- Plain text written next to the source EPUB in reading order
- Kanji bases stripped; ruby readings propagated and converted to katakana
- HTML, styling, and duplicate title noise removed
