from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class CastResult:
    manifest_path: Path
    updated_chunks: int = 0
    total_chunks: int = 0


def annotate_manifest(manifest_path: Path, *, force: bool = False, **_: object) -> CastResult:
    """Placeholder for future LLM-powered casting workflow."""
    raise NotImplementedError("LLM-based cast workflow not implemented yet.")


def cast_manifests(target: Path, *, force: bool = False) -> list[CastResult]:
    """Placeholder for future LLM-powered casting workflow."""
    raise NotImplementedError("LLM-based cast workflow not implemented yet.")


__all__ = ["cast_manifests", "annotate_manifest", "CastResult"]
