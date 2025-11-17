from __future__ import annotations

import json
from pathlib import Path

from nk.core import _ReadingAccumulator, _serialize_ruby_evidence


class _StubNLP:
    def __init__(self, readings: dict[str, str]) -> None:
        self._readings = readings

    def to_reading_text(self, text: str) -> str:
        return self._readings.get(text, text)


def test_ruby_evidence_uses_unidic_small_kana_variant() -> None:
    accumulator = _ReadingAccumulator()
    accumulator.counts["シンチヨウ"] = 2
    accumulator.total = 2
    accumulator.suffix_counts["に"] = 2
    nlp = _StubNLP({"慎重": "シンチョウ"})

    entries = _serialize_ruby_evidence({"慎重": accumulator}, nlp=nlp)

    assert entries[0]["reading"] == "シンチョウ"
    assert entries[0]["suffix"] == "に"


def test_ruby_evidence_keeps_different_unidic_reading() -> None:
    accumulator = _ReadingAccumulator()
    accumulator.counts["カンゼン"] = 3
    accumulator.total = 3
    nlp = _StubNLP({"完全": "マッタク"})  # drastically different reading

    entries = _serialize_ruby_evidence({"完全": accumulator}, nlp=nlp)

    assert entries[0]["reading"] == "カンゼン"


def test_fixture_book_ruby_evidence_has_small_kana() -> None:
    path = Path(
        "data/test/負けヒロインが多すぎる！ ６ (ガガガ文庫) - 雨森たきび/ruby_evidence.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    target = next(entry for entry in payload if entry.get("base") == "慎重")
    assert target["reading"] == "シンチョウ"
