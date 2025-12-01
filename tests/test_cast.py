from pathlib import Path

import pytest

from nk.cast import annotate_manifest


def test_annotate_manifest_is_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        annotate_manifest(Path("dummy"))
