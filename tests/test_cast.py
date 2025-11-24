from pathlib import Path

from nk.cast import annotate_manifest
from nk.chunk_manifest import write_chunk_manifest


def test_annotate_manifest_assigns_voice(tmp_path: Path) -> None:
    text = "ここまでの本文。\n\n「あたしは行くわ」と小声で言った。"
    chapter = tmp_path / "001.txt"
    chapter.write_text(text, encoding="utf-8")
    manifest_path = write_chunk_manifest(chapter, text)
    assert manifest_path is not None

    voice_map = {"narrator": 13, "female": 2, "male": 13}
    result = annotate_manifest(manifest_path, voice_map=voice_map, force=True)
    assert result.updated_chunks == result.total_chunks

    payload = manifest_path.read_text(encoding="utf-8")
    assert "\"speaker\": \"female\"" in payload
    assert "\"voice\": 2" in payload
