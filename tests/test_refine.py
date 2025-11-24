from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from nk.refine import append_override_entry, edit_single_token, load_override_config, refine_book, remove_token


def _write_token_file(path: Path, tokens: list[dict[str, object]], text: str) -> None:
    payload = {
        "version": 2,
        "text_sha1": hashlib.sha1(text.encode("utf-8")).hexdigest(),
        "tokens": tokens,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_edit_single_token_updates_token_file(tmp_path: Path) -> None:
    book_dir = tmp_path / "book_manual"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("アメが降る。", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {
                "surface": "雨",
                "reading": "アメ",
                "accent": 0,
                "start": 0,
                "end": 1,
                "transformed_start": 0,
                "transformed_end": 2,
            }
        ],
        "アメが降る。",
    )

    updated = edit_single_token(chapter, 0, reading="アマ", accent=2, pos="名詞")
    assert updated
    assert chapter.read_text(encoding="utf-8") == "アマが降る。"
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert len(tokens) == 1
    token = tokens[0]
    assert token["reading"] == "アマ"
    assert token["fallback_reading"] == "アマ"
    assert token["accent"] == 2
    assert token["pos"] == "名詞"
    assert token["reading_source"] == "manual"


def test_edit_single_token_uses_replacement_when_reading_missing(tmp_path: Path) -> None:
    book_dir = tmp_path / "book_manual_replacement"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("アメ", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {
                "surface": "雨",
                "reading": "アメ",
                "accent": 0,
                "start": 0,
                "end": 1,
                "transformed_start": 0,
                "transformed_end": 2,
            }
        ],
        "アメ",
    )

    updated = edit_single_token(chapter, 0, replacement="アマ")
    assert updated
    assert chapter.read_text(encoding="utf-8") == "アマ"
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    token = payload["tokens"][0]
    assert token["reading"] == "アマ"
    assert token["fallback_reading"] == "アマ"


def test_edit_single_token_validates_index(tmp_path: Path) -> None:
    book_dir = tmp_path / "book_manual_invalid"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("アメ", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [{"surface": "雨", "reading": "アメ", "accent": 0, "start": 0, "end": 1, "transformed_start": 0, "transformed_end": 2}],
        "アメ",
    )
    with pytest.raises(ValueError):
        edit_single_token(chapter, 5, reading="アマ")


def test_remove_token_updates_metadata(tmp_path: Path) -> None:
    book_dir = tmp_path / "book_remove"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    text = "アメとユキ"
    chapter.write_text(text, encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {"surface": "雨", "reading": "アメ", "accent": 0, "start": 0, "end": 1, "transformed_start": 0, "transformed_end": 2},
            {"surface": "雪", "reading": "ユキ", "accent": 1, "start": 2, "end": 3, "transformed_start": 3, "transformed_end": 5},
        ],
        text,
    )

    removed = remove_token(chapter, 0)
    assert removed
    assert chapter.read_text(encoding="utf-8") == "雨とユキ"
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert len(tokens) == 1
    assert tokens[0]["surface"] == "雪"
    assert tokens[0]["transformed_start"] == 2
    assert tokens[0]["transformed_end"] == 4
    assert payload["text_sha1"] == hashlib.sha1("雨とユキ".encode("utf-8")).hexdigest()


def test_remove_token_validates_index(tmp_path: Path) -> None:
    book_dir = tmp_path / "book_remove_invalid"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("アメ", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [{"surface": "アメ", "reading": "アメ", "accent": 0, "start": 0, "end": 2, "transformed_start": 0, "transformed_end": 2}],
        "アメ",
    )
    with pytest.raises(ValueError):
        remove_token(chapter, 1)


def test_refine_applies_replacement_to_tokens(tmp_path: Path) -> None:
    book_dir = tmp_path / "book"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("テイアラが来た。", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {
                "surface": "天愛星",
                "reading": "テイアラ",
                "accent": 0,
                "start": 0,
                "end": 4,
                "transformed_start": 0,
                "transformed_end": 4,
            },
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
    (book_dir / "custom_token.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")

    rules = load_override_config(book_dir)
    updated = refine_book(book_dir, rules)
    assert updated == 1

    assert chapter.read_text(encoding="utf-8") == "ティアラが来た。"
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert len(tokens) == 1
    assert tokens[0]["reading"] == "ティアラ"
    assert tokens[0]["accent"] == 2
    assert tokens[0]["surface"] == "天愛星"


def test_refine_allows_token_only_override(tmp_path: Path) -> None:
    book_dir = tmp_path / "book2"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("クラウゼル", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {"surface": "クラウゼル", "reading": "クラウゼル", "accent": 0, "start": 0, "end": 5, "transformed_start": 0, "transformed_end": 5},
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
    (book_dir / "custom_token.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    updated = refine_book(book_dir, rules)
    assert updated == 1
    assert chapter.read_text(encoding="utf-8") == "クラウゼル"
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert tokens[0]["accent"] == 2


def test_refine_updates_sha_with_stripped_text(tmp_path: Path) -> None:
    book_dir = tmp_path / "book3"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("アメヲタベル。\n", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {"surface": "飴", "reading": "アメ", "accent": 0, "start": 0, "end": 2, "transformed_start": 0, "transformed_end": 2},
        ],
        "アメヲタベル。\n",
    )
    overrides = {
        "overrides": [
            {"pattern": "アメ", "reading": "アメ", "accent": 1},
        ]
    }
    (book_dir / "custom_token.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    refine_book(book_dir, rules)
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    assert payload["text_sha1"] == hashlib.sha1("アメヲタベル。".encode("utf-8")).hexdigest()


def test_refine_limits_matches_with_surface(tmp_path: Path) -> None:
    book_dir = tmp_path / "surface"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("カミカミ", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {"surface": "神", "reading": "カミ", "accent": 0, "start": 0, "end": 1, "transformed_start": 0, "transformed_end": 2},
            {"surface": "紙", "reading": "カミ", "accent": 0, "start": 1, "end": 2, "transformed_start": 2, "transformed_end": 4},
        ],
        "カミカミ",
    )
    overrides = {
        "overrides": [
            {
                "pattern": "カミ",
                "reading": "カミ",
                "surface": "神",
                "match_surface": "神",
                "accent": 2,
            }
        ]
    }
    (book_dir / "custom_token.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    updated = refine_book(book_dir, rules)
    assert updated == 1
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert len(tokens) == 2
    first, second = tokens
    assert first["surface"] == "神"
    assert first["accent"] == 2
    assert second["surface"] == "紙"
    assert second["accent"] == 0


def test_refine_skips_replacement_when_surface_mismatch(tmp_path: Path) -> None:
    book_dir = tmp_path / "surface_mismatch"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    text = "トリヤマセキエン"
    chapter.write_text(text, encoding="utf-8")
    original = book_dir / "001.original.txt"
    original.write_text("鳥山石燕", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {
                "surface": "鳥山石燕",
                "reading": "トリヤマセキエン",
                "accent": 0,
                "start": 0,
                "end": 4,
                "transformed_start": 0,
                "transformed_end": len(text),
            }
        ],
        text,
    )
    overrides = {
        "overrides": [
            {
                "pattern": "セキ",
                "replacement": "アカ",
                "reading": "アカ",
                "surface": "赤",
                "pos": "接頭辞",
            }
        ]
    }
    (book_dir / "custom_token.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    refined = refine_book(book_dir, rules)
    assert refined == 0
    assert chapter.read_text(encoding="utf-8") == text
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert len(tokens) == 1
    token = tokens[0]
    assert token["surface"] == "鳥山石燕"
    assert token["reading"] == "トリヤマセキエン"


def test_refine_shifts_transformed_offsets(tmp_path: Path) -> None:
    book_dir = tmp_path / "offsets"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("カナカナ", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {"surface": "仮名一", "reading": "カナ", "accent": 0, "start": 0, "end": 1, "transformed_start": 0, "transformed_end": 2},
            {"surface": "仮名二", "reading": "カナ", "accent": 0, "start": 1, "end": 2, "transformed_start": 2, "transformed_end": 4},
        ],
        "カナカナ",
    )
    overrides = {
        "overrides": [
            {
                "pattern": "カナ",
                "replacement": "カーナ",
                "reading": "カーナ",
                "surface": "仮名一",
                "match_surface": "仮名一",
            }
        ]
    }
    (book_dir / "custom_token.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    updated = refine_book(book_dir, rules)
    assert updated == 1
    assert chapter.read_text(encoding="utf-8") == "カーナカナ"
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert len(tokens) == 2
    first, second = tokens
    assert first["surface"] == "仮名一"
    assert first["transformed_start"] == 0
    assert first["transformed_end"] == 3
    assert second["surface"] == "仮名二"
    assert second["transformed_start"] == 3
    assert second["transformed_end"] == 5


def test_refine_overrides_covered_spans(tmp_path: Path) -> None:
    book_dir = tmp_path / "covered"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    text = "正崎が来た"
    chapter.write_text(text, encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(
        token_path,
        [
            {"surface": "正", "reading": "セイ", "accent": 0, "start": 0, "end": 1, "transformed_start": 0, "transformed_end": 1},
            {"surface": "崎", "reading": "サキ", "accent": 0, "start": 1, "end": 2, "transformed_start": 1, "transformed_end": 2},
            {"surface": "が", "reading": "ガ", "accent": 0, "start": 2, "end": 3, "transformed_start": 2, "transformed_end": 3},
            {"surface": "来た", "reading": "キタ", "accent": 0, "start": 3, "end": 5, "transformed_start": 3, "transformed_end": 5},
        ],
        text,
    )
    overrides = {
        "overrides": [
            {
                "pattern": "正崎",
                "replacement": "セイザキ",
                "reading": "セイザキ",
                "surface": "正崎",
            }
        ]
    }
    (book_dir / "custom_token.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)

    updated = refine_book(book_dir, rules)
    assert updated == 1
    assert chapter.read_text(encoding="utf-8") == "セイザキが来た"

    payload = json.loads(token_path.read_text(encoding="utf-8"))
    refined_tokens = payload["tokens"]
    assert len(refined_tokens) == 3
    assert not any(tok.get("surface") == "正" for tok in refined_tokens)
    assert not any(tok.get("surface") == "崎" for tok in refined_tokens)
    override_token = next(tok for tok in refined_tokens if tok.get("surface") == "正崎")
    assert override_token["reading"] == "セイザキ"
    assert override_token["transformed_start"] == 0
    assert override_token["transformed_end"] == 4
    assert any(tok.get("surface") == "が" and tok.get("transformed_start") == 4 for tok in refined_tokens)


def test_refine_supports_legacy_custom_pitch_file(tmp_path: Path) -> None:
    book_dir = tmp_path / "legacy"
    book_dir.mkdir()
    overrides = {"overrides": [{"pattern": "legacy", "reading": "legacy", "accent": 1}]}
    legacy_path = book_dir / "custom_pitch.json"
    new_path = book_dir / "custom_token.json"
    legacy_path.write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    assert len(rules) == 1


def test_override_rules_create_tokens_for_plain_text(tmp_path: Path) -> None:
    book_dir = tmp_path / "plain"
    book_dir.mkdir()
    chapter = book_dir / "001.txt"
    chapter.write_text("CONTENTS\n本編", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(token_path, [], "CONTENTS\n本編")
    overrides = {
        "overrides": [
            {
                "pattern": "CONTENTS",
                "reading": "コンテンツ",
                "surface": "CONTENTS",
            }
        ]
    }
    (book_dir / "custom_token.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    updated = refine_book(book_dir, rules)
    assert updated == 1
    assert "コンテンツ" in chapter.read_text(encoding="utf-8")
    payload = json.loads(token_path.read_text(encoding="utf-8"))
    tokens = payload["tokens"]
    assert any(token.get("surface") == "CONTENTS" and token.get("reading") == "コンテンツ" for token in tokens)


def test_refine_skips_original_text_files(tmp_path: Path) -> None:
    book_dir = tmp_path / "skip_original"
    book_dir.mkdir()
    transformed = book_dir / "001.txt"
    transformed.write_text("CONTENTS", encoding="utf-8")
    original = book_dir / "001.original.txt"
    original.write_text("CONTENTS_ORIGINAL", encoding="utf-8")
    token_path = book_dir / "001.txt.token.json"
    _write_token_file(token_path, [], "CONTENTS")
    overrides = {"overrides": [{"pattern": "CONTENTS", "reading": "コンテンツ", "surface": "CONTENTS"}]}
    (book_dir / "custom_token.json").write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    rules = load_override_config(book_dir)
    refined = refine_book(book_dir, rules)
    assert refined == 1
    assert transformed.read_text(encoding="utf-8") == "コンテンツ"
    # Original text must remain untouched
    assert original.read_text(encoding="utf-8") == "CONTENTS_ORIGINAL"


def test_append_override_entry_creates_file(tmp_path: Path) -> None:
    book_dir = tmp_path / "book_append"
    book_dir.mkdir()
    entry = {
        "pattern": "test",
        "reading": "テスト",
        "accent": 1,
    }
    path = append_override_entry(book_dir, entry)
    assert path.exists()
    rules = load_override_config(book_dir)
    assert len(rules) == 1
    assert rules[0].pattern == "test"
