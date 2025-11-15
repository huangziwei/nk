#!/usr/bin/env python3
"""
Evaluate the nk reading pipeline against ruby evidence from an EPUB.

Example:
    uv run python dev/test_eval.py --epub dev/nhkeasier-2024.epub --output dev/nhk_eval_results.json
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

RUN_EVAL = os.getenv("NK_RUN_EVAL") == "1"

from nk.cli import _apply_dictionary_mapping  # noqa: E402
from nk.core import (  # noqa: E402
    _load_corpus_reading_accumulators,
    _hiragana_to_katakana,
    _normalize_katakana,
    _select_reading_mapping,
)
from nk.data.build_corpus_readings import iter_ruby_records  # noqa: E402
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from nk.nlp import NLPBackend, NLPBackendUnavailableError  # noqa: E402


def evaluate_ruby_records(
    records,
    convert_fn,
    *,
    max_contexts: int = 3,
    progress: Progress | None = None,
    progress_task: int | None = None,
):
    total = 0
    matches = 0
    mismatch_map: dict[tuple[str, str, str], dict[str, object]] = {}
    context_limit = max(0, max_contexts)
    for record in records:
        base = getattr(record, "base", "") or ""
        reading = getattr(record, "reading", "") or ""
        if not base or not reading:
            continue
        prefix = getattr(record, "prefix", "") or ""
        suffix = getattr(record, "suffix", "") or ""
        expected = _normalize_katakana(_hiragana_to_katakana(reading.strip()))
        prefix_reading_raw = convert_fn(prefix) if prefix else ""
        suffix_reading_raw = convert_fn(suffix) if suffix else ""
        combined = f"{prefix}{base}{suffix}"
        combined_reading_raw = convert_fn(combined)
        combined_norm = _normalize_katakana(_hiragana_to_katakana((combined_reading_raw or "").strip()))
        prefix_norm = _normalize_katakana(_hiragana_to_katakana((prefix_reading_raw or "").strip()))
        suffix_norm = _normalize_katakana(_hiragana_to_katakana((suffix_reading_raw or "").strip()))
        remainder_norm = combined_norm
        prefix_ok = False
        suffix_ok = False
        if prefix_norm:
            if remainder_norm.startswith(prefix_norm):
                remainder_norm = remainder_norm[len(prefix_norm) :]
                prefix_ok = True
        elif not prefix:
            prefix_ok = True
        if suffix_norm:
            if remainder_norm.endswith(suffix_norm):
                remainder_norm = remainder_norm[: len(remainder_norm) - len(suffix_norm)]
                suffix_ok = True
        elif not suffix:
            suffix_ok = True
        if prefix_ok and suffix_ok:
            actual_norm = remainder_norm
        else:
            base_reading_raw = convert_fn(base)
            actual_norm = _normalize_katakana(_hiragana_to_katakana((base_reading_raw or "").strip()))
        actual = actual_norm
        total += 1
        if actual == expected:
            matches += 1
        else:
            key = (base, expected, actual)
            entry = mismatch_map.get(key)
            if entry is None:
                entry = {
                    "base": base,
                    "expected": expected,
                    "actual": actual,
                    "count": 0,
                    "contexts": [],
                }
                mismatch_map[key] = entry
            entry["count"] += 1  # type: ignore[index]
            if len(entry["contexts"]) < context_limit:  # type: ignore[index]
                entry["contexts"].append(  # type: ignore[index]
                    {
                        "prefix": getattr(record, "prefix", "") or "",
                        "suffix": getattr(record, "suffix", "") or "",
                    }
                )
        if progress and progress_task is not None:
            progress.advance(progress_task)
    if progress and progress_task is not None:
        progress.stop_task(progress_task)
    mismatches = sorted(
        mismatch_map.values(),
        key=lambda item: (-item["count"], item["base"], item["expected"], item["actual"]),
    )
    summary = {
        "total": total,
        "matches": matches,
        "accuracy": (matches / total) if total else 0.0,
        "mismatches": mismatches,
    }
    return summary


def _build_converter() -> tuple[NLPBackend, callable]:
    backend = NLPBackend()
    accumulators = _load_corpus_reading_accumulators()
    tier3, tier2, context_rules = _select_reading_mapping(accumulators, "advanced", backend)

    def _convert(text: str) -> str:
        processed = _apply_dictionary_mapping(text, tier3, context_rules)
        processed = _apply_dictionary_mapping(processed, tier2, context_rules)
        return backend.to_reading_text(processed)

    return backend, _convert


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate nk readings against ruby evidence.")
    parser.add_argument(
        "--epub",
        type=Path,
        default=ROOT / "dev" / "nhkeasier-2024.epub",
        help="Path to the EPUB to use for evaluation (default: dev/nhkeasier-2024.epub).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "dev" / "nhk_eval_results.json",
        help="Where to write the JSON summary (default: dev/nhk_eval_results.json).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of ruby records to sample.",
    )
    parser.add_argument(
        "--contexts",
        type=int,
        default=3,
        help="How many prefix/suffix contexts to keep per mismatch (default: 3).",
    )
    return parser.parse_args()


def _collect_records(epub_path: Path, limit: int | None) -> list:
    records = []
    for record in iter_ruby_records(epub_path):
        records.append(record)
        if limit is not None and limit > 0 and len(records) >= limit:
            break
    return records


def main() -> int:
    args = parse_args()
    epub_path = args.epub.expanduser()
    if not epub_path.exists():
        print(f"EPUB not found: {epub_path}", file=sys.stderr)
        return 1
    print(f"Collecting ruby evidence from {epub_path}...")
    records = _collect_records(epub_path, args.limit)
    if not records:
        print("No ruby annotations found in the EPUB.", file=sys.stderr)
        return 1
    try:
        backend, converter = _build_converter()
    except NLPBackendUnavailableError as exc:
        print(f"Failed to initialize NLP backend: {exc}", file=sys.stderr)
        return 2
    progress_columns = [
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ]
    try:
        with Progress(*progress_columns) as progress:
            task_total = len(records)
            task_id = progress.add_task("Evaluating rubies", total=task_total)
            summary = evaluate_ruby_records(
                records,
                converter,
                max_contexts=args.contexts,
                progress=progress,
                progress_task=task_id,
            )
    finally:
        del backend
    total = summary["total"]
    matches = summary["matches"]
    accuracy = summary["accuracy"]
    mismatches = summary["mismatches"]
    print(f"Total samples: {total}")
    print(f"Matches: {matches}")
    print(f"Accuracy: {accuracy:.4%}")
    print(f"Mismatches: {len(mismatches)}")
    output_path = args.output.expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Wrote summary to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


@pytest.mark.skipif(not RUN_EVAL, reason="manual regression; set NK_RUN_EVAL=1 to enable")
def test_eval_script() -> None:
    epub = ROOT / "dev" / "nhkeasier-2024.epub"
    if not epub.exists():
        pytest.skip(f"EPUB not found: {epub}")
    output = ROOT / "tests" / "artifacts" / "nhk_eval_results.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        __file__,
        "--epub",
        str(epub),
        "--output",
        str(output),
    ]
    env = os.environ.copy()
    env["NK_RUN_EVAL"] = "1"
    subprocess.run(cmd, check=True, env=env)
    assert output.exists()
