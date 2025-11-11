from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("PIL")
from PIL import Image

from nk.book_io import (
    load_book_metadata,
    load_pitch_metadata,
    update_book_tts_defaults,
    write_book_package,
)
from nk.core import ChapterText, CoverImage
from nk.pitch import PitchToken


def test_write_book_package_emits_metadata_and_cover(tmp_path: Path) -> None:
    output_dir = tmp_path / "Book"
    chapters = [
        ChapterText(
            source="ch1.xhtml",
            title="Reading Title",
            text="Reading Title\nContent",
            original_title="Chapter One",
            book_title="Book Title",
            book_author="Author Name",
        )
    ]
    cover = CoverImage(
        path="OEBPS/cover.jpg",
        media_type="image/jpeg",
        data=b"\xff\xd8\xff",
    )

    package = write_book_package(output_dir, chapters, source_epub=tmp_path / "book.epub", cover_image=cover)
    assert package.cover_path is not None
    assert package.cover_path.read_bytes() == cover.data
    assert package.book_title == "Book Title"
    assert package.book_author == "Author Name"
    assert package.metadata_path.exists()
    payload = json.loads(package.metadata_path.read_text(encoding="utf-8"))
    assert payload["title"] == "Book Title"
    assert payload["author"] == "Author Name"
    assert payload["cover"] == package.cover_path.name
    assert payload["chapters"][0]["file"].startswith("001_")
    assert payload["chapters"][0]["original_title"] == "Chapter One"
    assert payload["chapters"][0]["source"] == "ch1.xhtml"
    assert package.m4b_manifest_path.exists()
    assert (output_dir / "custom_pitch.json").exists()
    m4b_payload = json.loads(package.m4b_manifest_path.read_text(encoding="utf-8"))
    assert m4b_payload["name"] == "Book Title"
    assert m4b_payload["artist"] == "Author Name"
    assert m4b_payload["cover"] == package.cover_path.name
    assert m4b_payload["tracks"][0]["file"].endswith(".mp3")
    assert m4b_payload["tracks"][0]["chapter"] == "Chapter One"

    loaded = load_book_metadata(output_dir)
    assert loaded is not None
    assert loaded.title == "Book Title"
    assert loaded.author == "Author Name"
    assert loaded.cover_path == package.cover_path
    file_name = package.chapter_records[0].path.name
    assert file_name in loaded.chapters
    assert loaded.chapters[file_name].original_title == "Chapter One"


def test_cover_is_padded_to_square(tmp_path: Path) -> None:
    rectangular = tmp_path / "cover_raw.png"
    original = Image.new("RGB", (640, 960), (20, 100, 160))
    original.save(rectangular, format="PNG")
    cover = CoverImage(
        path="OPS/cover.png",
        media_type="image/png",
        data=rectangular.read_bytes(),
    )
    chapters = [
        ChapterText(source="c.xhtml", title="Title", text="Title\nBody"),
    ]

    package = write_book_package(tmp_path / "Square", chapters, cover_image=cover)
    assert package.cover_path is not None
    with Image.open(package.cover_path) as processed, Image.open(rectangular) as source:
        processed = processed.convert("RGB")
        source = source.convert("RGB")
        assert processed.width == processed.height == max(source.width, source.height)
        offset_x = (processed.width - source.width) // 2
        offset_y = (processed.height - source.height) // 2
        assert offset_x > 0 or offset_y > 0
        extracted = processed.crop(
            (offset_x, offset_y, offset_x + source.width, offset_y + source.height)
        )
        assert list(extracted.getdata()) == list(source.getdata())


def test_write_book_package_persists_pitch_metadata(tmp_path: Path) -> None:
    output_dir = tmp_path / "PitchBook"
    tokens = [
        PitchToken(surface="雨", reading="アメ", accent_type=1, start=0, end=2, pos="名詞"),
        PitchToken(surface="飴", reading="アメ", accent_type=0, start=3, end=5, pos="名詞"),
    ]
    chapters = [
        ChapterText(
            source="ch1.xhtml",
            title="Reading",
            text="アメトアメ",
            pitch_data=tokens,
        )
    ]

    package = write_book_package(output_dir, chapters)
    record = package.chapter_records[0]
    pitch_path = record.path.with_name(record.path.name + ".pitch.json")
    assert pitch_path.exists()
    payload = json.loads(pitch_path.read_text(encoding="utf-8"))
    expected_sha1 = hashlib.sha1("アメトアメ".encode("utf-8")).hexdigest()
    assert payload["text_sha1"] == expected_sha1
    assert payload["tokens"][0]["accent"] == 1
    assert payload["tokens"][1]["accent"] == 0
    loaded = load_pitch_metadata(record.path)
    assert loaded is not None
    assert len(loaded.tokens) == 2


def test_write_book_package_preserves_tts_defaults(tmp_path: Path) -> None:
    output_dir = tmp_path / "Book"
    output_dir.mkdir()
    existing = {
        "version": 1,
        "title": "Old Title",
        "chapters": [
            {"index": 1, "file": "001_old.txt", "title": "Old", "original_title": None},
        ],
        "tts_defaults": {"speaker": 11, "speed": 0.88, "pitch": 0.0, "intonation": 1.1},
    }
    (output_dir / ".nk-book.json").write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    chapters = [
        ChapterText(
            source="c.xhtml",
            title="New",
            text="Body",
            book_title="New Title",
            book_author="Author",
        )
    ]

    package = write_book_package(output_dir, chapters)
    payload = json.loads(package.metadata_path.read_text(encoding="utf-8"))
    assert payload["tts_defaults"]["speaker"] == 11
    assert payload["tts_defaults"]["speed"] == 0.88


def test_load_book_metadata_reads_tts_defaults(tmp_path: Path) -> None:
    book_dir = tmp_path / "novel"
    book_dir.mkdir()
    (book_dir / "001_intro.txt").write_text("text", encoding="utf-8")
    payload = {
        "version": 1,
        "title": "Novel",
        "chapters": [
            {
                "index": 1,
                "file": "001_intro.txt",
                "title": "Intro",
                "original_title": "第一章",
            }
        ],
        "tts_defaults": {
            "speaker": 7,
            "speed": 0.92,
            "pitch": -0.07,
            "intonation": 1.15,
        },
    }
    (book_dir / ".nk-book.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    metadata = load_book_metadata(book_dir)
    assert metadata is not None
    defaults = metadata.tts_defaults
    assert defaults is not None
    assert defaults.speaker == 7
    assert defaults.speed == 0.92
    assert defaults.pitch == -0.07
    assert defaults.intonation == 1.15


def test_update_book_tts_defaults_merges_fields(tmp_path: Path) -> None:
    book_dir = tmp_path / "novel"
    book_dir.mkdir()
    meta_path = book_dir / ".nk-book.json"
    payload = {
        "version": 1,
        "title": "Novel",
        "chapters": [
            {"index": 1, "file": "001.txt", "title": "One", "original_title": None},
        ],
        "tts_defaults": {"speaker": 5, "speed": 0.95},
    }
    meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    assert update_book_tts_defaults(book_dir, {"speaker": 9, "pitch": -0.12}) is True
    updated = json.loads(meta_path.read_text(encoding="utf-8"))
    assert updated["tts_defaults"]["speaker"] == 9
    assert updated["tts_defaults"]["speed"] == 0.95
    assert updated["tts_defaults"]["pitch"] == -0.12

    assert update_book_tts_defaults(book_dir, {"pitch": None}) is True
    updated = json.loads(meta_path.read_text(encoding="utf-8"))
    assert "pitch" not in updated["tts_defaults"]

    assert update_book_tts_defaults(book_dir, {"pitch": None}) is False

