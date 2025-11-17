# nk

`nk` (short for “No Kanji”; pronounce /nɔk/ or /nik/) converts Japanese EPUBs into chapterized TXT files where every kanji is replaced with katakana. It leans on in-text ruby first and falls back to UniDic lookups. From there you can synthesize MP3s with VoiceVoxand expose them over WebDAV or the built-in web-based player.

Tested on macOS and Ubuntu.

## Installation

```bash
git clone https://github.com/huangziwei/nk
cd nk
./install.sh  # prepares uv, UniDic, ffmpeg, VoiceVox, etc.
```

## Usage

```bash
source .venv/bin/activate
nk example/\[夏目漱石\]\ 夢十夜.epub # convert epubs into kana-only chapterized txt files
nk tts example/\[夏目漱石\]\ 夢十夜 # convert txt files into mp3s
nk web example --host 0.0.0.0 --port 2046 # start a local server to allow stream via browser
nk dav example # start start a local server to allow stream via webdav compatible clients
nk read example/\[夏目漱石\]\ 夢十夜 # inspect tokens/original text in the browser
```

run `nk -h` to see all usages.

## More

- [TL;DR](TLDR.md) (more detailed installation and uninstall guide, VoiceVox tips, pitch overrides, web and WebDav etc.)
