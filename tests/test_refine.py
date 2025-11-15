from __future__ import annotations

import hashlib
import json
from pathlib import Path

from nk.refine import load_override_config, refine_book


def _write_pitch_file(path: Path, tokens: list[dict[str, object]], text: str) -> None:
    payload = {
        "version": 2,
        "text_sha1": hashlib.sha1(text.encode("utf-8")).hexdigest(),
        "tokens": tokens,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_refine_applies_replacement_and_pitch(tmp_path: Path) -> None:
    book_dir = tmp_path / "book"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("テイアラが来た。", encoding="utf-8")
    pitch_path = book_dir / "001.txt.pitch.json"
    _write_pitch_file(
        pitch_path,
        [
            {"surface": "テイ", "reading": "テイ", "accent": 0, "start": 0, "end": 2},
            {"surface": "アラ", "reading": "アラ", "accent": 0, "start": 2, "end": 4},
        ],
        "テイアラが来た。",
    )
    overrides = {
        "overrides": [
            {
                "pattern": "テイアラ",
                "replacement": "ティアラ",
                "reading": "ティアラ",
                "accent": 2,
                "surface": "天愛星",
            }
        ]
    }
    (book_dir / "custom_pitch.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")

    rules = load_override_config(book_dir)
    updated = refine_book(book_dir, rules)
    assert updated == 1

    assert chapter.read_text(encoding="utf-8") == "ティアラが来た。"
    payload = json.loads(pitch_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert len(tokens) == 1
    assert tokens[0]["reading"] == "ティアラ"
    assert tokens[0]["accent"] == 2
    assert tokens[0]["surface"] == "天愛星"


def test_refine_allows_pitch_only_override(tmp_path: Path) -> None:
    book_dir = tmp_path / "book2"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("クラウゼル", encoding="utf-8")
    pitch_path = book_dir / "001.txt.pitch.json"
    _write_pitch_file(
        pitch_path,
        [
            {"surface": "クラウゼル", "reading": "クラウゼル", "accent": 0, "start": 0, "end": 5},
        ],
        "クラウゼル",
    )
    overrides = {
        "overrides": [
            {
                "pattern": "クラウゼル",
                "reading": "クラウゼル",
                "accent": 2,
            }
        ]
    }
    (book_dir / "custom_pitch.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    updated = refine_book(book_dir, rules)
    assert updated == 1
    assert chapter.read_text(encoding="utf-8") == "クラウゼル"
    payload = json.loads(pitch_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert tokens[0]["accent"] == 2


def test_refine_updates_sha_with_stripped_text(tmp_path: Path) -> None:
    book_dir = tmp_path / "book3"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("アメヲタベル。\n", encoding="utf-8")
    pitch_path = book_dir / "001.txt.pitch.json"
    _write_pitch_file(
        pitch_path,
        [
            {"surface": "飴", "reading": "アメ", "accent": 0, "start": 0, "end": 2},
        ],
        "アメヲタベル。\n",
    )
    overrides = {
        "overrides": [
            {"pattern": "アメ", "reading": "アメ", "accent": 1},
        ]
    }
    (book_dir / "custom_pitch.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    refine_book(book_dir, rules)
    payload = json.loads(pitch_path.read_text(encoding="utf-8"))
    assert payload["text_sha1"] == hashlib.sha1("アメヲタベル。".encode("utf-8")).hexdigest()
