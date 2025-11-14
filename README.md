# nk

Convert Japanese EPUBs into TTS-ready text with VoiceVox (tested on macOS and Ubuntu).

## Installation

```bash
git clone https://github.com/huangziwei/nk
cd nk
./install.sh  # prepares uv, UniDic, ffmpeg, VoiceVox, etc.
```

## Usage

```bash
source .venv/bin/activate
nk my_book.epub
nk tts my_book/

# or stay outside the venv
uv run nk my_book.epub
uv run nk tts my_book/
```

## More

- [TL;DR guide (installation knobs, VoiceVox tips, pitch overrides, etc.)](TLDR.md)
