# nk

Convert Japanese EPUBs into ruby-expanded chapterized text, synthesize VoiceVox MP3s, and stream them via WebDAV.

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
nk nk example/\[夏目漱石\]\ 夢十夜.epub # convert epubs into kana-based chapterized txt files
nk tts example/\[夏目漱石\]\ 夢十夜 # convert txt files into mp3s
nk web example --host 0.0.0.0 --port 2046 # start a local server to allow stream via browser
nk dav example # start start a local server to allow stream via webdav compatible clients
```

run `-h` to see all usages.

## More

- [TL;DR](TLDR.md) (more detailed installation and uninstall guide, VoiceVox tips, pitch overrides, web and WebDav etc.)
