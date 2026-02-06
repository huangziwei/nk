# nk

> ## Note
>
> 1. Originally short for `no kanji`, but it evolves so much over time that the name doesn't mean anything anymore. Let's just call it /nik/.
>
> 2. as of 20260206, `nk` has been superseded by [`nik`](https://github.com/huangziwei/nik).



`nk` converts Japanese EPUBs into TTS-friendly, chapterized TXT files. It leans on in-text ruby first and falls back to UniDic lookups (reinforced with NHK easy news corpus). From there you can synthesize MP3s with VoiceVox and expose them over WebDAV or the built-in web-based player.

Tested on macOS and Ubuntu.

## Installation

```bash
uv tool install git+https://github.com/huangziwei/nk
nk deps install
```

or 

```bash
git clone https://github.com/huangziwei/nk
cd nk
./install.sh  # prepares uv, UniDic, ffmpeg, VoiceVox, etc.
```

## Usage

```bash
source .venv/bin/activate

# cli
nk example/\[夏目漱石\]\ 夢十夜.epub # # convert epubs into partially kana-transformed, chapterized txt files
nk example/\[夏目漱石\]\ 夢十夜.epub --transform full # convert epubs into kana-only chapterized txt files
nk tts example/\[夏目漱石\]\ 夢十夜 # convert txt files into mp3s
nk samples books # generate VoiceVox samples in books/samples

# web
nk read example/\[夏目漱石\]\ 夢十夜 --host 0.0.0.0 --port 2045 # inspect tokens/original text in the browser
nk play example --host 0.0.0.0 --port 2046 # start a local server to allow stream via browser
nk dav example --host 0.0.0.0 --port 2047 # start start a local server to allow stream via webdav compatible clients
```

run `nk -h` to see all usages.

## More

- [TL;DR](TLDR.md) (more detailed installation and uninstall guide, VoiceVox tips, pitch overrides, web and WebDav etc.)
