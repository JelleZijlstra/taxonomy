from __future__ import annotations

import argparse
from pathlib import Path

from data_import import ce_file, lib


def import_and_lint(entries: list[lib.CEDict], *, verbose: bool) -> None:
    article = entries[0]["article"]
    list(
        lib.add_classification_entries(
            entries, dry_run=False, strict=True, verbose=verbose
        )
    )
    lib.format_ces_in_article(article)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manually import a validated JSONL CE file into the database."
    )
    parser.add_argument("ce_file", type=Path)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write to the database; without this flag, only show a dry run",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--allow-imperfect-matches",
        action="store_true",
        help="permit ambiguous or unrecognized Name mappings during --apply",
    )
    args = parser.parse_args()

    entries = ce_file.read_ce_file(args.ce_file)
    entries = ce_file.validate_structure(entries)
    clean = ce_file.print_validation_report(entries)
    if args.apply and not clean and not args.allow_imperfect_matches:
        raise SystemExit(
            "Refusing import: fix ambiguous or unrecognized names, or explicitly review them and pass --allow-imperfect-matches"
        )
    if not args.apply:
        print("Dry run only. Re-run with --apply after reviewing this output.")
        list(
            lib.add_classification_entries(
                entries, dry_run=True, strict=True, verbose=args.verbose
            )
        )
    else:
        import_and_lint(entries, verbose=args.verbose)


if __name__ == "__main__":
    main()
