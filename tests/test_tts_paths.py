from __future__ import annotations

import hashlib
import json
import base64
from pathlib import Path
import zipfile

import pytest

from types import SimpleNamespace

from nk.cli import _ensure_tts_source_ready, _slice_targets_by_index
from nk.pitch import PitchToken
from nk.tts import (
    TTSTarget,
    _MAX_CHARS_PER_CHUNK,
    _target_cache_dir,
    _split_text_on_breaks,
    _split_text_on_breaks_with_spans,
    _slice_pitch_tokens_for_chunk,
    _apply_pitch_overrides,
    _chunk_cache_path,
    _enrich_pitch_tokens_with_voicevox,
    _reset_voicevox_accent_cache_for_tests,
    resolve_text_targets,
)


class _DummyBackend:
    def to_reading_text(self, text: str) -> str:
        return text

    def to_reading_with_pitch(self, text: str):
        return text, []

    def reading_variants(self, text: str) -> set[str]:
        return set()

    def tokenize(self, text: str):
        if not text:
            return []
        return [
            SimpleNamespace(
                surface=text,
                reading=text,
                start=0,
                end=len(text),
                accent_type=None,
                accent_connection=None,
                pos=None,
            )
        ]


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    _reset_voicevox_accent_cache_for_tests()


def test_resolve_single_file(tmp_path: Path) -> None:
    txt = tmp_path / "sample.txt"
    txt.write_text("hello", encoding="utf-8")

    targets = resolve_text_targets(txt)
    assert len(targets) == 1
    target = targets[0]
    assert target.source == txt
    assert target.output == tmp_path / "sample.mp3"
    assert target.track_number == 1
    assert target.track_total == 1
    assert target.book_title == tmp_path.name


def test_resolve_directory_with_output_override(tmp_path: Path) -> None:
    input_dir = tmp_path / "chapters"
    input_dir.mkdir()
    (input_dir / "a.txt").write_text("a", encoding="utf-8")
    (input_dir / "b.txt").write_text("b", encoding="utf-8")
    (input_dir / "b.original.txt").write_text("orig", encoding="utf-8")

    out_dir = tmp_path / "mp3"
    targets = resolve_text_targets(input_dir, out_dir)

    assert [t.source.name for t in targets] == ["a.txt", "b.txt"]
    assert [t.output for t in targets] == [out_dir / "a.mp3", out_dir / "b.mp3"]
    assert [t.track_number for t in targets] == [1, 2]
    assert all(t.track_total == 2 for t in targets)


def test_resolve_uses_book_metadata(tmp_path: Path) -> None:
    book_dir = tmp_path / "novel"
    book_dir.mkdir()
    chapter = book_dir / "001_intro.txt"
    chapter.write_text("こんにちは", encoding="utf-8")
    cover = book_dir / "cover.jpg"
    cover.write_bytes(b"\xff\xd8\xff")
    metadata = {
        "version": 1,
        "title": "Meta Title",
        "author": "Meta Author",
        "cover": "cover.jpg",
        "chapters": [
            {
                "index": 7,
                "file": "001_intro.txt",
                "title": "Intro",
                "original_title": "第一章",
            }
        ],
    }
    (book_dir / ".nk-book.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    targets = resolve_text_targets(book_dir)
    assert len(targets) == 1
    target = targets[0]
    assert target.book_title == "Meta Title"
    assert target.book_author == "Meta Author"
    assert target.chapter_title == "Intro"
    assert target.original_title == "第一章"
    assert target.track_number == 7
    assert target.track_total == 1
    assert target.cover_image == cover


def test_resolve_rejects_original_file(tmp_path: Path) -> None:
    original = tmp_path / "001_intro.original.txt"
    original.write_text("orig", encoding="utf-8")
    with pytest.raises(ValueError):
        resolve_text_targets(original)


_COVER_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAMAAWgmWQ0AAAAASUVORK5CYII="
)


def _build_epub_for_tts(tmp_path: Path) -> Path:
    epub_path = tmp_path / "novel.epub"
    mimetype = "application/epub+zip"
    container = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf = """<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Auto TTS</dc:title>
    <meta name="cover" content="cover-image"/>
  </metadata>
  <manifest>
    <item id="cover-image" href="cover.png" media-type="image/png" properties="cover-image" />
    <item id="chap" href="chap.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="chap"/>
  </spine>
</package>
"""
    chapter = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Chapter</title></head>
  <body>
    <h1>Chapter</h1>
    <p>テスト本文です。</p>
  </body>
</html>
"""
    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", mimetype, compress_type=zipfile.ZIP_STORED)
        zf.writestr("META-INF/container.xml", container)
        zf.writestr("OEBPS/content.opf", opf)
        zf.writestr("OEBPS/chap.xhtml", chapter)
        zf.writestr("OEBPS/cover.png", _COVER_PNG)
    return epub_path


def test_ensure_tts_source_ready_chapterizes_epub(tmp_path: Path) -> None:
    epub = _build_epub_for_tts(tmp_path)
    prepared = _ensure_tts_source_ready(epub, nlp=_DummyBackend(), quiet=True)
    assert prepared.is_dir()
    txt_files = sorted(prepared.glob("*.txt"))
    assert txt_files
    assert (prepared / ".nk-book.json").exists()


def test_ensure_tts_source_ready_refreshes_m4b(tmp_path: Path) -> None:
    book_dir = tmp_path / "book"
    book_dir.mkdir()
    chapter_file = book_dir / "001_intro.txt"
    chapter_file.write_text("content", encoding="utf-8")
    cover = book_dir / "cover.jpg"
    cover.write_bytes(b"\xff\xd8\xff\xdb")
    metadata = {
        "version": 1,
        "title": "Book Title",
        "cover": "cover.jpg",
        "chapters": [
            {
                "index": 1,
                "file": "001_intro.txt",
                "title": "Reading",
                "original_title": "Original Title",
            }
        ],
    }
    (book_dir / ".nk-book.json").write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
    old_manifest = {
        "name": "Book Title",
        "tracks": [{"file": "001_intro.mp3", "chapter": "Reading", "index": 1}],
    }
    manifest_path = book_dir / "m4b.json"
    manifest_path.write_text(json.dumps(old_manifest, ensure_ascii=False), encoding="utf-8")
    epub_path = tmp_path / "book.epub"
    epub_path.write_text("dummy", encoding="utf-8")

    prepared = _ensure_tts_source_ready(epub_path, nlp=_DummyBackend(), quiet=True)
    assert prepared == book_dir
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["tracks"][0]["chapter"] == "Original Title"
    assert payload.get("cover") == "cover.jpg"


def test_resolve_directory_without_txt(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(FileNotFoundError):
        resolve_text_targets(empty)
def test_split_text_on_breaks() -> None:
    text = "Line one\nLine two\n\nLine three\n\n\nLine four"
    chunks = _split_text_on_breaks(text)
    assert chunks == ["Line one\nLine two", "Line three", "Line four"]


def test_split_text_on_breaks_limits_long_blocks() -> None:
    long_block = "ア" * (_MAX_CHARS_PER_CHUNK + 200)
    text = f"{long_block}\n\nEND"
    chunks = _split_text_on_breaks(text)
    assert chunks[-1] == "END"
    long_chunks = chunks[:-1]
    expected_segments = (len(long_block) + _MAX_CHARS_PER_CHUNK - 1) // _MAX_CHARS_PER_CHUNK
    assert len(long_chunks) == expected_segments
    assert all(len(chunk) <= _MAX_CHARS_PER_CHUNK for chunk in long_chunks)
    assert all(set(chunk) == {"ア"} for chunk in long_chunks)


def test_split_text_on_breaks_prefers_sentence_boundaries() -> None:
    text = "ア" * (_MAX_CHARS_PER_CHUNK - 5) + "。" + "イ" * 100
    chunks = _split_text_on_breaks(text)
    assert len(chunks) == 2
    assert chunks[0].endswith("。")
    assert len(chunks[0]) <= _MAX_CHARS_PER_CHUNK


def test_split_text_on_breaks_with_spans_matches_substrings() -> None:
    raw = "  アメ\n\nカサ  \n\n"
    text = raw.strip()
    spans = _split_text_on_breaks_with_spans(text)
    extracted = [chunk.text for chunk in spans]
    assert extracted == _split_text_on_breaks(text)
    for chunk in spans:
        assert text[chunk.start : chunk.end] == chunk.text


def test_slice_pitch_tokens_and_apply_overrides() -> None:
    tokens = [
        PitchToken(surface="雨", reading="アメ", accent_type=1, start=0, end=2, pos="名詞"),
        PitchToken(surface="飴", reading="アメ", accent_type=0, start=3, end=5, pos="名詞"),
    ]
    chunk_tokens = _slice_pitch_tokens_for_chunk(tokens, 0, 5)
    assert len(chunk_tokens) == 2
    payload = {
        "accent_phrases": [
            {"moras": [{"text": "ア"}, {"text": "メ"}], "accent": 2},
            {"moras": [{"text": "ト"}, {"text": "ア"}, {"text": "メ"}], "accent": 3},
        ]
    }
    _apply_pitch_overrides(payload, chunk_tokens)
    accents = [phrase["accent"] for phrase in payload["accent_phrases"]]
    assert accents[0] == 1  # atamadaka
    assert accents[1] == 3  # heiban => len(moras)


def test_chunk_cache_includes_pitch_signature(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    text = "アメ"
    path1 = _chunk_cache_path(cache_dir, 1, text, None)
    path2 = _chunk_cache_path(cache_dir, 1, text, "0:2:アメ:1")
    path3 = _chunk_cache_path(cache_dir, 1, text, "0:2:アメ:0")
    assert path1 != path2
    assert path2 != path3


class _MockVoiceVoxClient:
    def __init__(self, responses: dict[str, dict[str, object]]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def build_audio_query(self, text: str) -> dict[str, object]:
        self.calls.append(text)
        payload = self.responses.get(text)
        if payload is None:
            raise AssertionError(f"Unexpected VoiceVox query for {text}")
        return payload


def test_enrich_pitch_tokens_with_voicevox_overrides_accent() -> None:
    tokens = [
        PitchToken(
            surface="小学館",
            reading="ショウガクカン",
            accent_type=None,
            accent_connection=None,
            pos=None,
            start=0,
            end=4,
            sources=("unidic",),
        ),
        PitchToken(
            surface="ひらがな",
            reading="ヒラガナ",
            accent_type=None,
            accent_connection=None,
            pos=None,
            start=5,
            end=7,
            sources=(),
        ),
    ]
    client = _MockVoiceVoxClient(
        {
            "小学館": {
                "kana": "ショウ'ガクカン",
                "accent_phrases": [
                    {
                        "moras": [{"text": "ショ"}, {"text": "ウ"}, {"text": "ガ"}, {"text": "ク"}, {"text": "カン"}],
                        "accent": 1,
                    }
                ],
            }
        }
    )
    _enrich_pitch_tokens_with_voicevox(tokens, client)
    assert tokens[0].accent_type == 1
    assert tokens[1].accent_type is None


def test_enrich_pitch_tokens_with_voicevox_skips_mismatched_kana() -> None:
    tokens = [
        PitchToken(surface="小学館", reading="ショウガクカン", accent_type=None, accent_connection=None, pos=None, start=0, end=4),
    ]
    client = _MockVoiceVoxClient(
        {
            "小学館": {
                "kana": "ショウ/ガク",
                "accent_phrases": [
                    {
                        "moras": [{"text": "ショ"}, {"text": "ウ"}],
                        "accent": 2,
                    }
                ],
            }
        }
    )
    _enrich_pitch_tokens_with_voicevox(tokens, client)
    assert tokens[0].accent_type is None


def test_enrich_pitch_tokens_with_voicevox_deduplicates_queries() -> None:
    tokens = [
        PitchToken(
            surface="小学館",
            reading="ショウガクカン",
            accent_type=None,
            accent_connection=None,
            pos=None,
            start=0,
            end=4,
            sources=("unidic",),
        ),
        PitchToken(
            surface="小学館",
            reading="ショウガクカン",
            accent_type=None,
            accent_connection=None,
            pos=None,
            start=10,
            end=14,
            sources=("unidic",),
        ),
    ]
    client = _MockVoiceVoxClient(
        {
            "小学館": {
                "kana": "ショウ'ガクカン",
                "accent_phrases": [
                    {
                        "moras": [{"text": "ショ"}, {"text": "ウ"}, {"text": "ガ"}, {"text": "ク"}, {"text": "カン"}],
                        "accent": 2,
                    }
                ],
            }
        }
    )
    _enrich_pitch_tokens_with_voicevox(tokens, client)
    assert all(token.accent_type == 2 for token in tokens)
    assert client.calls.count("小学館") == 1


def test_target_cache_dir_prefers_existing_legacy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "books" / "novel"
    root.mkdir(parents=True)
    source = root / "001.txt"
    source.write_text("hello", encoding="utf-8")
    target = TTSTarget(source=source, output=root / "001.mp3")
    cache_base = tmp_path / "cache"
    cache_base.mkdir()

    slug = "001"
    legacy_key = source.relative_to(tmp_path).as_posix()
    legacy_hash = hashlib.sha1(legacy_key.encode("utf-8")).hexdigest()[:10]
    legacy_dir = cache_base / f"{slug}-{legacy_hash}"
    legacy_dir.mkdir(parents=True)

    monkeypatch.chdir(tmp_path)

    cache_dir = _target_cache_dir(cache_base, target)
    assert cache_dir == legacy_dir


def test_target_cache_dir_uses_canonical_when_missing_legacy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "books" / "novel"
    root.mkdir(parents=True)
    source = root / "001.txt"
    source.write_text("hello", encoding="utf-8")
    target = TTSTarget(source=source, output=root / "001.mp3")
    cache_base = tmp_path / "cache"
    cache_base.mkdir()

    monkeypatch.chdir(tmp_path)
    cache_dir = _target_cache_dir(cache_base, target)

    slug = "001"
    canonical_hash = hashlib.sha1(source.resolve().as_posix().encode("utf-8")).hexdigest()[:10]
    expected = cache_base / f"{slug}-{canonical_hash}"
    assert cache_dir == expected


def test_target_cache_dir_prefers_more_complete_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "books" / "novel"
    root.mkdir(parents=True)
    source = root / "001.txt"
    source.write_text("hello", encoding="utf-8")
    target = TTSTarget(source=source, output=root / "001.mp3")
    cache_base = tmp_path / "cache"
    cache_base.mkdir()

    slug = "001"
    legacy_hash = hashlib.sha1(source.relative_to(tmp_path).as_posix().encode("utf-8")).hexdigest()[:10]
    canonical_hash = hashlib.sha1(source.resolve().as_posix().encode("utf-8")).hexdigest()[:10]

    legacy_dir = cache_base / f"{slug}-{legacy_hash}"
    canonical_dir = cache_base / f"{slug}-{canonical_hash}"
    legacy_dir.mkdir(parents=True)
    canonical_dir.mkdir(parents=True)

    (legacy_dir / "00001_dummy.wav").write_bytes(b"\x00")
    (canonical_dir / ".complete").write_text("2", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    cache_dir = _target_cache_dir(cache_base, target)
    assert cache_dir == canonical_dir


def test_target_cache_dir_scans_slug_prefix_when_needed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "books" / "novel"
    root.mkdir(parents=True)
    source = root / "001.txt"
    source.write_text("hello", encoding="utf-8")
    target = TTSTarget(source=source, output=root / "001.mp3")
    cache_base = tmp_path / "cache"
    cache_base.mkdir()

    slug = "001"
    orphan_dir = cache_base / f"{slug}-deadbeef42"
    orphan_dir.mkdir(parents=True)
    (orphan_dir / ".progress").write_text("5", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    cache_dir = _target_cache_dir(cache_base, target)
    assert cache_dir == orphan_dir


def test_slice_targets_by_index_skips_chapters(tmp_path: Path) -> None:
    targets = [
        TTSTarget(source=tmp_path / "a.txt", output=tmp_path / "a.mp3"),
        TTSTarget(source=tmp_path / "b.txt", output=tmp_path / "b.mp3"),
        TTSTarget(source=tmp_path / "c.txt", output=tmp_path / "c.mp3"),
    ]
    sliced = _slice_targets_by_index(targets, 2)
    assert [t.source.name for t in sliced] == ["b.txt", "c.txt"]


def test_slice_targets_by_index_out_of_range() -> None:
    targets = [
        TTSTarget(source=Path("a.txt"), output=Path("a.mp3")),
    ]
    with pytest.raises(ValueError):
        _slice_targets_by_index(targets, 5)
