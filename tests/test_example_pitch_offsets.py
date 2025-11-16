from __future__ import annotations

import json
from pathlib import Path

import pytest


EXAMPLE_PITCH_FILES = sorted(Path("example").rglob("*.pitch.json"))

if not EXAMPLE_PITCH_FILES:
    pytest.skip("example pitch fixtures not found", allow_module_level=True)


def _assert_index_order(tokens: list[dict], key: str, *, pitch_path: Path) -> None:
    """
    Ensure indexes monotonically increase without overlap for the given key.
    """
    prev_end: int | None = None
    for idx, token in enumerate(tokens):
        start = token["start"].get(key)
        end = token["end"].get(key)
        if start is None or end is None:
            continue
        assert start <= end, f"{pitch_path} token #{idx} has {key} start > end"
        if prev_end is not None:
            assert (
                start >= prev_end
            ), f"{pitch_path} token #{idx} has {key} overlap (start={start}, prev_end={prev_end})"
        prev_end = end


@pytest.mark.parametrize("pitch_path", EXAMPLE_PITCH_FILES)
def test_example_pitch_original_indexes_are_ordered(pitch_path: Path) -> None:
    data = json.loads(pitch_path.read_text())
    _assert_index_order(data["tokens"], "original", pitch_path=pitch_path)


@pytest.mark.parametrize("pitch_path", EXAMPLE_PITCH_FILES)
def test_example_pitch_transformed_indexes_are_ordered(pitch_path: Path) -> None:
    data = json.loads(pitch_path.read_text())
    _assert_index_order(data["tokens"], "transformed", pitch_path=pitch_path)
