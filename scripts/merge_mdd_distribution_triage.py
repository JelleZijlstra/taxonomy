"""Carry review notes into a regenerated MDD distribution-problem CSV.

Only ``review_status`` and ``review_comment`` are copied from the previous
triage file. All generated evidence and distribution fields come from the new
file, so rerunning the MDD report cannot leave stale evidence in the result.
"""

import argparse
import csv
import json
from collections import Counter
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

KEY_FIELDS = ("mdd_id", "species", "taxon_id", "missing_country")
REVIEW_FIELDS = ("review_status", "review_comment")

type Row = dict[str, str]
type RowKey = tuple[str, ...]


def read_rows(path: Path) -> tuple[list[str], list[Row]]:
    with path.open(newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError(f"{path} has no header row")
        return list(reader.fieldnames), list(reader)


def validate_headers(path: Path, headers: Iterable[str]) -> None:
    header_set = set(headers)
    missing = (set(KEY_FIELDS) | set(REVIEW_FIELDS)) - header_set
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"{path} is missing required columns: {missing_text}")


def row_key(row: Row) -> RowKey:
    return tuple(row[field] for field in KEY_FIELDS)


def rows_by_key(path: Path, rows: Sequence[Row]) -> dict[RowKey, Row]:
    keyed: dict[RowKey, Row] = {}
    duplicates: list[RowKey] = []
    for row in rows:
        key = row_key(row)
        if key in keyed:
            duplicates.append(key)
        keyed[key] = row
    if duplicates:
        examples = "; ".join("|".join(key) for key in duplicates[:5])
        raise ValueError(f"{path} has duplicate triage keys: {examples}")
    return keyed


def is_reviewed(row: Row) -> bool:
    return bool(row["review_status"].strip())


def merge_rows(
    previous_rows: Sequence[Row], new_rows: Sequence[Row]
) -> tuple[list[Row], dict[str, Any]]:
    previous_by_key = rows_by_key(Path("previous input"), previous_rows)
    rows_by_key(Path("new input"), new_rows)

    merged: list[Row] = []
    carried = 0
    already_reviewed = 0
    matched = 0
    for new_row in new_rows:
        row = new_row.copy()
        previous = previous_by_key.get(row_key(row))
        if previous is not None:
            matched += 1
        if is_reviewed(row):
            already_reviewed += 1
        elif previous is not None and is_reviewed(previous):
            for field in REVIEW_FIELDS:
                row[field] = previous[field]
            carried += 1
        merged.append(row)

    merged_keys = {row_key(row) for row in merged}
    retired_reviewed = [
        row
        for key, row in previous_by_key.items()
        if is_reviewed(row) and key not in merged_keys
    ]
    reviewed = sum(is_reviewed(row) for row in merged)
    summary: dict[str, Any] = {
        "new_rows": len(merged),
        "matched_rows": matched,
        "unmatched_new_rows": len(merged) - matched,
        "carried_reviews": carried,
        "already_reviewed_new_rows": already_reviewed,
        "reviewed_rows": reviewed,
        "untriaged_rows": len(merged) - reviewed,
        "retired_reviewed_rows": len(retired_reviewed),
        "review_status_counts": dict(
            sorted(
                Counter(
                    row["review_status"] for row in merged if is_reviewed(row)
                ).items()
            )
        ),
        "retired_review_status_counts": dict(
            sorted(Counter(row["review_status"] for row in retired_reviewed).items())
        ),
    }
    return merged, summary


def write_rows(path: Path, headers: Sequence[str], rows: Sequence[Row]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("previous", type=Path, help="Previously triaged CSV")
    parser.add_argument("new", type=Path, help="Newly regenerated discrepancy CSV")
    parser.add_argument("output", type=Path, help="Merged triage CSV to create")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    previous_headers, previous_rows = read_rows(args.previous)
    new_headers, new_rows = read_rows(args.new)
    validate_headers(args.previous, previous_headers)
    validate_headers(args.new, new_headers)
    previous_by_key = rows_by_key(args.previous, previous_rows)
    rows_by_key(args.new, new_rows)

    merged, summary = merge_rows(list(previous_by_key.values()), new_rows)
    write_rows(args.output, new_headers, merged)
    summary["output"] = str(args.output)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
