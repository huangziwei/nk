from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .core import epub_to_txt
from .nlp import NLPBackend, NLPBackendUnavailableError


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="EPUB → TXT with ruby propagation and base removal.",
    )
    ap.add_argument(
        "input_path",
        help="Path to input .epub or a directory containing .epub files",
    )
    ap.add_argument(
        "-o",
        "--output-name",
        help="Optional name for the output .txt (same folder as input)",
    )
    ap.add_argument(
        "-m",
        "--mode",
        choices=["fast", "slow", "advanced"],
        default="fast",
        help=(
            "Propagation strategy: 'fast' balances coverage with accuracy, 'slow' is stricter, "
            "and 'advanced' disables propagation outside ruby for maximum safety."
        ),
    )
    return ap


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)

    inp_path = Path(args.input_path)
    if not inp_path.exists():
        raise FileNotFoundError(f"Input path not found: {inp_path}")

    backend = None
    if args.mode in ("slow", "advanced"):
        try:
            backend = NLPBackend()
        except NLPBackendUnavailableError as exc:
            raise SystemExit(str(exc)) from exc

    if inp_path.is_dir():
        if args.output_name:
            raise ValueError("Output name cannot be used when processing a directory.")
        epubs = sorted(p for p in inp_path.iterdir() if p.suffix.lower() == ".epub")
        if not epubs:
            raise FileNotFoundError(f"No .epub files found in directory: {inp_path}")
        for epub_path in epubs:
            txt = epub_to_txt(str(epub_path), mode=args.mode, nlp=backend)
            output_path = epub_path.with_suffix(".txt")
            output_path.write_text(txt, encoding="utf-8")
    else:
        if inp_path.suffix.lower() != ".epub":
            raise ValueError(f"Input must be an .epub file or directory: {inp_path}")

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

        txt = epub_to_txt(str(inp_path), mode=args.mode, nlp=backend)
        output_path.write_text(txt, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
