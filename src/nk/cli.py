from __future__ import annotations

import argparse
from pathlib import Path

from .core import epub_to_txt


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="EPUB → TXT with ruby propagation and base removal.",
    )
    ap.add_argument("input_epub", help="Path to input .epub")
    ap.add_argument(
        "-o",
        "--output-name",
        help="Optional name for the output .txt (same folder as input)",
    )
    return ap


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    inp_path = Path(args.input_epub)
    if not inp_path.exists():
        raise FileNotFoundError(f"Input EPUB not found: {inp_path}")

    if args.output_name:
        out_name_path = Path(args.output_name)
        if out_name_path.parent not in (Path("."), Path("")):
            raise ValueError(
                "Output name must not contain directory components; "
                "it is saved next to the EPUB."
            )
        output_path = inp_path.with_name(out_name_path.name)
    else:
        output_path = inp_path.with_suffix(".txt")

    txt = epub_to_txt(str(inp_path))
    output_path.write_text(txt, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
