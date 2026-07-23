from __future__ import annotations

import argparse
from pathlib import Path

from data_import import ce_file, lib, location_file, occurrence_import


def import_and_lint(
    entries: list[lib.CEDict],
    *,
    verbose: bool,
    location_plan: location_file.LocationPlan | None = None,
) -> None:
    article = entries[0]["article"]
    if location_plan is None:
        location_plan = location_file.build_plan(entries, [])
    locations = location_file.apply_plan(location_plan)
    list(
        lib.add_classification_entries(
            entries, dry_run=False, strict=True, verbose=verbose
        )
    )
    lib.format_ces_in_article(article)
    occurrence_import.add_occurrence_records(entries, locations)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manually import a validated JSONL CE file into the database."
    )
    parser.add_argument("ce_file", type=Path)
    parser.add_argument(
        "--location-file",
        type=Path,
        help=(
            "reviewed Location proposals; defaults to the sibling "
            "*.locations.jsonl file when it exists"
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write to the database; without this flag, only show a dry run",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    entries = ce_file.read_ce_file(args.ce_file)
    entries = ce_file.validate_structure(entries)
    ce_file.print_validation_report(entries)
    location_path = args.location_file or location_file.companion_path(args.ce_file)
    if args.location_file is not None or location_path.exists():
        proposal_report = location_file.read_location_file_report(location_path)
        proposals = proposal_report.locations
        proposal_errors = proposal_report.errors
        print(f"Location proposals: {location_path}")
    else:
        proposals = []
        proposal_errors = []
    location_plan = location_file.build_plan(
        entries, proposals, proposal_errors=proposal_errors
    )
    location_file.print_plan(location_plan)
    occurrence_import.print_occurrence_report(entries, location_plan.locations)
    if args.apply and not location_plan.is_clean:
        raise SystemExit("Refusing import: resolve Location proposal conflicts")
    if not args.apply:
        print("Dry run only. Re-run with --apply after reviewing this output.")
        list(
            lib.add_classification_entries(
                entries, dry_run=True, strict=True, verbose=args.verbose
            )
        )
    else:
        import_and_lint(entries, verbose=args.verbose, location_plan=location_plan)


if __name__ == "__main__":
    main()
