# ruff: noqa: E402,F403

import argparse
import importlib
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

lib = importlib.import_module("data_import.lib")

from taxonomy.refmatch import matcher
from taxonomy.refmatch.matcher import *

DEFAULT_INPUT = lib.DATA_DIR / "chiroptera-hmw-refs-parsed.csv"
DEFAULT_OUTPUT = lib.DATA_DIR / "chiroptera-hmw-refs-taxonomy-matches.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match parsed HMW Chiroptera references to taxonomy database Articles."
    )
    parser.set_defaults(doi_mode="cached", bhl_mode="cached")
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Stage 2 parsed CSV to read (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Stage 3 CSV to write (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--doi-mode",
        choices=matcher.LOOKUP_MODES,
        help=(
            "DOI inference mode: off, cached-only, or cached plus fresh network calls "
            "(default: cached)."
        ),
    )
    parser.add_argument(
        "--bhl-mode",
        choices=matcher.LOOKUP_MODES,
        help=(
            "BHL inference mode: off, cached-only, or cached plus fresh network calls "
            "(default: cached)."
        ),
    )
    parser.add_argument(
        "--doi-learning",
        action="store_true",
        help=(
            "Use Crossref DOI lookups during the first learning pass. Slower; normally "
            "secure taxonomy matches are enough to learn citation-group mappings."
        ),
    )
    parser.add_argument(
        "--clear-crossref-cache-every",
        type=int,
        default=matcher.CLEAR_CROSSREF_CACHE_EVERY,
        help=(
            "Clear in-memory CrossRef caches after this many processed rows "
            f"(default: {matcher.CLEAR_CROSSREF_CACHE_EVERY}; use 0 to disable)."
        ),
    )
    parser.add_argument(
        "--infer-doi",
        action="store_const",
        const="network",
        dest="doi_mode",
        help="Deprecated alias for --doi-mode=network.",
    )
    parser.add_argument(
        "--infer-bhl",
        action="store_const",
        const="network",
        dest="bhl_mode",
        help="Deprecated alias for --bhl-mode=network.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    matcher.run_match_csv(
        args.input,
        args.output,
        doi_mode=args.doi_mode,
        bhl_mode=args.bhl_mode,
        doi_learning=args.doi_learning,
        clear_crossref_cache_every=args.clear_crossref_cache_every,
    )


if __name__ == "__main__":
    main()
