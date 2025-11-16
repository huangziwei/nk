from nk.core import _align_tokens_to_original_text, _normalize_token_order
from nk.pitch import PitchToken


def test_token_offsets_follow_original_order() -> None:
    piece_text = "アカエンピツデ"
    original_text = "赤鉛筆で"
    tokens = [
        PitchToken(surface="鉛筆", reading="エンピツ", accent_type=0, start=200, end=204, sources=("ruby",)),
        PitchToken(surface="赤", reading="アカ", accent_type=1, start=500, end=502, sources=("unidic",)),
    ]

    _align_tokens_to_original_text(original_text, tokens)
    _normalize_token_order(piece_text, tokens)

    assert [token.surface for token in tokens] == ["赤", "鉛筆"]
    assert tokens[0].start == 0
    assert tokens[0].original_start == 0
    assert tokens[1].start == 2
    assert tokens[1].original_start == 1
