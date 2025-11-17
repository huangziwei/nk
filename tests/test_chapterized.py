from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

pytest.importorskip("fugashi")

from nk.core import epub_to_chapter_texts
from nk.nlp import NLPBackend


@pytest.fixture(scope="module")
def backend() -> NLPBackend:
    return NLPBackend()


def _build_simple_epub(target: Path) -> Path:
    epub_path = target / "sample.epub"
    mimetype = "application/epub+zip"
    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf_xml = """<?xml version="1.0" encoding="UTF-8"?>
<package version="3.0" unique-identifier="BookId" xmlns="http://www.idpf.org/2007/opf">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Sample Book</dc:title>
    <dc:creator>Sample Author</dc:creator>
  </metadata>
  <manifest>
    <item id="toc" href="toc.xhtml" media-type="application/xhtml+xml"/>
    <item id="ch1" href="ch1.xhtml" media-type="application/xhtml+xml"/>
    <item id="ch2" href="ch2.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="toc"/>
    <itemref idref="ch1"/>
    <itemref idref="ch2"/>
  </spine>
</package>
"""
    toc_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Table of Contents</title></head>
  <body>
    <h1>Table of Contents</h1>
    <p>Chapter One</p>
    <p>Chapter Two</p>
  </body>
</html>
"""
    ch1_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Chapter One</title></head>
  <body>
    <h1>Chapter One</h1>
    <p>This is the first chapter.</p>
  </body>
</html>
"""
    ch2_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Chapter Two</title></head>
  <body>
    <h1>Chapter Two</h1>
    <p>This is the second chapter.</p>
  </body>
</html>
"""
    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", mimetype)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("OEBPS/content.opf", opf_xml)
        zf.writestr("OEBPS/toc.xhtml", toc_html)
        zf.writestr("OEBPS/ch1.xhtml", ch1_html)
        zf.writestr("OEBPS/ch2.xhtml", ch2_html)
    return epub_path


def test_chapterized_output_matches_join(tmp_path: Path, backend: NLPBackend) -> None:
    epub_path = _build_simple_epub(tmp_path)
    chapters, _ = epub_to_chapter_texts(str(epub_path), nlp=backend)
    assert len(chapters) == 3
    assert chapters[1].source.endswith("ch1.xhtml")
    assert chapters[2].source.endswith("ch2.xhtml")
    assert chapters[1].title == "Chapter One"
    assert chapters[2].title == "Chapter Two"
    assert all(ch.book_author == "Sample Author" for ch in chapters)
    for idx in (1, 2):
        first_line = chapters[idx].text.splitlines()[0]
        assert first_line == chapters[idx].title


def test_repeated_dialogue_lines_are_preserved(tmp_path: Path, backend: NLPBackend) -> None:
    epub_path = tmp_path / "repeat.epub"
    repeat_line = "「……。そうか」"
    mimetype = "application/epub+zip"
    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf_xml = """<?xml version="1.0" encoding="UTF-8"?>
<package version="3.0" unique-identifier="BookId" xmlns="http://www.idpf.org/2007/opf">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Repeat Test</dc:title>
  </metadata>
  <manifest>
    <item id="ch1" href="ch1.xhtml" media-type="application/xhtml+xml"/>
    <item id="ch2" href="ch2.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="ch1"/>
    <itemref idref="ch2"/>
  </spine>
</package>
"""
    ch1_html = f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Scene A</title></head>
  <body>
    <h1>Scene A</h1>
    <p>First chapter intro.</p>
    <p>{repeat_line}</p>
    <p>Afterwards A.</p>
  </body>
</html>
"""
    ch2_html = f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Scene B</title></head>
  <body>
    <h1>Scene B</h1>
    <p>Second chapter intro.</p>
    <p>{repeat_line}</p>
    <p>Afterwards B.</p>
  </body>
</html>
"""
    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", mimetype)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("content.opf", opf_xml)
        zf.writestr("ch1.xhtml", ch1_html)
        zf.writestr("ch2.xhtml", ch2_html)

    chapters, _ = epub_to_chapter_texts(str(epub_path), nlp=backend)
    assert len(chapters) == 2
    target_line = next(
        (line.strip() for line in chapters[0].text.splitlines() if "そうか" in line),
        None,
    )
    assert target_line is not None
    total_repeat = sum(
        1 for chapter in chapters for line in chapter.text.splitlines() if line.strip() == target_line
    )
    assert total_repeat == 2
    scene_b = next(ch for ch in chapters if ch.title == "Scene B")
    assert target_line in {line.strip() for line in scene_b.text.splitlines()}


def test_ascii_ruby_is_propagated(tmp_path: Path, backend: NLPBackend) -> None:
    epub_path = tmp_path / "ascii_ruby.epub"
    mimetype = "application/epub+zip"
    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf_xml = """<?xml version="1.0" encoding="UTF-8"?>
<package version="3.0" unique-identifier="BookId" xmlns="http://www.idpf.org/2007/opf">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Ascii Ruby</dc:title>
  </metadata>
  <manifest>
    <item id="ch1" href="ch1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="ch1"/>
  </spine>
</package>
"""
    ch1_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Ascii Ruby</title></head>
  <body>
    <h1>Ascii Ruby</h1>
    <p><ruby>JUN<rt>ジュン</rt></ruby>は歌う。</p>
    <p><ruby>JUN<rt>ジュン</rt></ruby>と<ruby>YOHILA<rt>ヨヒラ</rt></ruby>がステージに立つ。</p>
    <p>サポートには<ruby>YOHILA<rt>ヨヒラ</rt></ruby>のバンドも参加している。</p>
    <p>今日は JUN のソロライブだ。</p>
    <p>YOHILA の演奏も続く。</p>
  </body>
</html>
"""
    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", mimetype)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("content.opf", opf_xml)
        zf.writestr("ch1.xhtml", ch1_html)

    chapters, _ = epub_to_chapter_texts(str(epub_path), nlp=backend)
    assert len(chapters) == 1
    text = chapters[0].text
    assert "JUN" not in text
    assert "YOHILA" not in text
    assert text.count("ジュン") >= 3
    assert text.count("ヨヒラ") >= 2
    assert "Raininグラム" not in text


def test_pitch_tokens_capture_transformation_sources(tmp_path: Path, backend: NLPBackend) -> None:
    epub_path = tmp_path / "sources.epub"
    mimetype = "application/epub+zip"
    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf_xml = """<?xml version="1.0" encoding="UTF-8"?>
<package version="3.0" unique-identifier="BookId" xmlns="http://www.idpf.org/2007/opf">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Sources</dc:title>
  </metadata>
  <manifest>
    <item id="ch1" href="ch1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="ch1"/>
  </spine>
</package>
"""
    ch1_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Sources</title></head>
  <body>
    <p><ruby>明日<rt>アシタ</rt></ruby>が来る。<ruby>明日<rt>アシタ</rt></ruby>は近い。</p>
    <p>明日が来る。</p>
  </body>
</html>
"""
    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", mimetype)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("content.opf", opf_xml)
        zf.writestr("ch1.xhtml", ch1_html)

    chapters, _ = epub_to_chapter_texts(str(epub_path), nlp=backend)
    assert len(chapters) == 1
    tokens = chapters[0].pitch_data
    assert tokens is not None
    assert len(tokens) >= 3
    sources = [tuple(token.sources or ()) for token in tokens]
    assert ("ruby",) in sources
    assert any("unidic" in source for source in sources)


def test_chapter_title_preserves_original_text(tmp_path: Path, backend: NLPBackend) -> None:
    epub_path = tmp_path / "title.epub"
    mimetype = "application/epub+zip"
    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf_xml = """<?xml version="1.0" encoding="UTF-8"?>
<package version="3.0" unique-identifier="BookId" xmlns="http://www.idpf.org/2007/opf">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Title Preservation</dc:title>
  </metadata>
  <manifest>
    <item id="ch1" href="ch1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="ch1"/>
  </spine>
</package>
"""
    ch1_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Title Preservation</title></head>
  <body>
    <h1>Interlude Melancholic Hydrangea</h1>
    <p>Interlude Melancholic <ruby>Hydrangea<rt>ハイドランジア</rt></ruby></p>
  </body>
</html>
"""
    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", mimetype)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("content.opf", opf_xml)
        zf.writestr("ch1.xhtml", ch1_html)

    chapters, _ = epub_to_chapter_texts(str(epub_path), nlp=backend)
    assert len(chapters) == 1
    assert chapters[0].title == "Interlude Melancholic ハイドランジア"


def test_ellipsis_normalization_survives_backend(tmp_path: Path, backend: NLPBackend) -> None:
    epub_path = tmp_path / "ellipsis.epub"
    mimetype = "application/epub+zip"
    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf_xml = """<?xml version="1.0" encoding="UTF-8"?>
<package version="3.0" unique-identifier="BookId" xmlns="http://www.idpf.org/2007/opf">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Ellipsis</dc:title>
  </metadata>
  <manifest>
    <item id="ch1" href="ch1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="ch1"/>
  </spine>
</package>
"""
    ellipsis_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Ellipsis</title></head>
  <body>
    <h1>Ellipsis</h1>
    <p>マッテ...まだ...</p>
  </body>
</html>
"""
    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", mimetype)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("content.opf", opf_xml)
        zf.writestr("ch1.xhtml", ellipsis_html)

    chapters, _ = epub_to_chapter_texts(str(epub_path), nlp=backend)
    assert len(chapters) == 1
    text = chapters[0].text
    assert "…" in text
    assert "..." not in text


def test_nlp_backend_provides_pitch_tokens() -> None:
    pytest.importorskip("fugashi")
    from nk.nlp import NLPBackend

    backend = NLPBackend()
    reading, tokens = backend.to_reading_with_pitch("雨と飴")
    assert reading.startswith("アメとアメ")
    accents = {token.surface: token.accent_type for token in tokens}
    assert accents.get("雨") == 1
    assert accents.get("飴") == 0
    structured, _ = backend.to_reading_with_pitch("雨\n\n飴")
    assert "\n\n" in structured


def test_toc_splits_shared_spine_item(tmp_path: Path, backend: NLPBackend) -> None:
    epub_path = tmp_path / "toc_split.epub"
    mimetype = "application/epub+zip"
    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf_xml = """<?xml version="1.0" encoding="UTF-8"?>
<package version="3.0" unique-identifier="BookId" xmlns="http://www.idpf.org/2007/opf">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Ten Nights</dc:title>
  </metadata>
  <manifest>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
    <item id="text" href="text.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="text"/>
  </spine>
</package>
"""
    nav_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
  <head><title>TOC</title></head>
  <body>
    <nav epub:type="toc">
      <ol>
        <li><a href="text.xhtml#night1">第一夜</a></li>
        <li><a href="text.xhtml#night2">第二夜</a></li>
      </ol>
    </nav>
  </body>
</html>
"""
    text_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Ten Nights</title></head>
  <body>
    <p>序</p>
    <h2 id="night1">第一夜</h2>
    <p>これは第一夜の物語。</p>
    <h2 id="night2">第二夜</h2>
    <p>これは第二夜の物語。</p>
  </body>
</html>
"""
    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", mimetype)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("OEBPS/content.opf", opf_xml)
        zf.writestr("OEBPS/nav.xhtml", nav_html)
        zf.writestr("OEBPS/text.xhtml", text_html)

    chapters, _ = epub_to_chapter_texts(str(epub_path), nlp=backend)
    assert len(chapters) == 3
    assert chapters[0].title == "ジョ"
    expected_first = backend.to_reading_text("第一夜").strip()
    expected_second = backend.to_reading_text("第二夜").strip()
    assert chapters[1].title == expected_first
    assert chapters[2].title == expected_second
    assert f"{expected_first}のモノガタリ" in chapters[1].text
    assert f"{expected_second}のモノガタリ" in chapters[2].text


def test_first_chapter_inserts_break_between_title_and_author(tmp_path: Path, backend: NLPBackend) -> None:
    epub_path = tmp_path / "title_author.epub"
    mimetype = "application/epub+zip"
    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf_xml = """<?xml version="1.0" encoding="UTF-8"?>
<package version="3.0" unique-identifier="BookId" xmlns="http://www.idpf.org/2007/opf">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Title Author Test</dc:title>
    <dc:creator>Example Author</dc:creator>
  </metadata>
  <manifest>
    <item id="ch1" href="ch1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="ch1"/>
  </spine>
</package>
"""
    ch1_html = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Title page</title></head>
  <body>
    <h1>夢十夜</h1>
    <h2>夏目漱石</h2>
  </body>
</html>
"""
    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", mimetype)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("content.opf", opf_xml)
        zf.writestr("ch1.xhtml", ch1_html)

    chapters, _ = epub_to_chapter_texts(str(epub_path), nlp=backend)
    assert len(chapters) == 1
    lines = chapters[0].text.splitlines()
    assert lines[0] == "ユメジュウヤ"
    assert lines[1] == ""
    assert lines[2] == "ナツメソウセキ"


def _assert_token_offsets_match(text: str | None, tokens: list, *, use_original: bool) -> None:
    if not text:
        return
    for token in tokens:
        expected = token.surface if use_original else token.reading
        if not expected:
            continue
        start = token.original_start if use_original else token.start
        end = token.original_end if use_original else token.end
        assert start is not None and end is not None
        assert 0 <= start <= end <= len(text)
        assert text[start:end] == expected


def test_pitch_tokens_align_with_example_text(backend: NLPBackend) -> None:
    epub_path = Path("example/[夏目漱石] 夢十夜.epub")
    chapters, _ = epub_to_chapter_texts(str(epub_path), nlp=backend)
    assert chapters
    for chapter in chapters[:3]:
        tokens = chapter.pitch_data or []
        _assert_token_offsets_match(chapter.text, tokens, use_original=False)
        _assert_token_offsets_match(chapter.original_text, tokens, use_original=True)
