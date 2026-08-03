"""Compare two IUCN Red List taxonomy exports and write a CSV report.

The comparison treats ``internalTaxonId`` as the stable identity when possible.
It separately recognizes cases where an unchanged scientific name received a
new ID, so IUCN identifier replacement is not reported as taxonomic turnover.

Example::

    python scripts/compare_iucn_red_list_taxonomy.py \
        'Mammalia-Red List 2024-1 (taxonomy).csv' \
        /tmp/iucn2026 \
        --order CHIROPTERA \
        --output /tmp/iucn-taxonomy-differences.csv

When an input is a directory, ``taxonomy.csv`` is preferred over
``taxonomy_with_html.csv``.
"""

import argparse
import csv
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypedDict, cast

REQUIRED_COLUMNS = {
    "internalTaxonId",
    "scientificName",
    "kingdomName",
    "phylumName",
    "className",
    "orderName",
    "familyName",
    "genusName",
    "speciesName",
    "infraType",
    "infraName",
    "infraAuthority",
    "subpopulationName",
    "authority",
    "taxonomicNotes",
}

IDENTITY_FIELDS = (
    "scientificName",
    "kingdomName",
    "phylumName",
    "className",
    "orderName",
    "familyName",
    "genusName",
    "speciesName",
    "infraType",
    "infraName",
    "subpopulationName",
)
AUTHORITY_FIELDS = ("authority", "infraAuthority")
COMPARED_FIELDS = (*IDENTITY_FIELDS, *AUTHORITY_FIELDS)

CHANGE_TYPE_ORDER = {
    "renamed": 0,
    "taxonomy_changed": 1,
    "id_changed_same_name": 2,
    "removed": 3,
    "added": 4,
    "authority_changed": 5,
}


class TaxonomyRow(TypedDict):
    internalTaxonId: str
    scientificName: str
    kingdomName: str
    phylumName: str
    className: str
    orderName: str
    familyName: str
    genusName: str
    speciesName: str
    infraType: str
    infraName: str
    infraAuthority: str
    subpopulationName: str
    authority: str
    taxonomicNotes: str


REPORT_COLUMNS = (
    "change_type",
    "changed_fields",
    "old_internal_taxon_id",
    "new_internal_taxon_id",
    "old_scientific_name",
    "new_scientific_name",
    "old_order",
    "new_order",
    "old_family",
    "new_family",
    "old_genus",
    "new_genus",
    "old_species_epithet",
    "new_species_epithet",
    "old_authority",
    "new_authority",
    "old_taxonomic_notes",
    "new_taxonomic_notes",
    "new_taxa_whose_notes_mention_old_name",
)


class ReportRow(TypedDict):
    change_type: str
    changed_fields: str
    old_internal_taxon_id: str
    new_internal_taxon_id: str
    old_scientific_name: str
    new_scientific_name: str
    old_order: str
    new_order: str
    old_family: str
    new_family: str
    old_genus: str
    new_genus: str
    old_species_epithet: str
    new_species_epithet: str
    old_authority: str
    new_authority: str
    old_taxonomic_notes: str
    new_taxonomic_notes: str
    new_taxa_whose_notes_mention_old_name: str


@dataclass(frozen=True)
class Comparison:
    old_count: int
    new_count: int
    shared_id_count: int
    rows: tuple[ReportRow, ...]
    counts: Counter[str]


def resolve_taxonomy_path(path: Path) -> Path:
    if path.is_file():
        return path
    if not path.exists():
        raise FileNotFoundError(path)
    if not path.is_dir():
        raise ValueError(f"Not a file or directory: {path}")
    for filename in ("taxonomy.csv", "taxonomy_with_html.csv"):
        candidate = path / filename
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"Could not find taxonomy.csv or taxonomy_with_html.csv in {path}"
    )


def read_taxonomy(path: Path) -> list[TaxonomyRow]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise ValueError(f"Missing columns in {path}: {missing_list}")
        rows = [
            cast("TaxonomyRow", {column: row[column] for column in REQUIRED_COLUMNS})
            for row in reader
        ]
    _validate_unique(rows, "internalTaxonId", path)
    _validate_unique(rows, "scientificName", path)
    return rows


def filter_by_order(rows: Iterable[TaxonomyRow], order_name: str) -> list[TaxonomyRow]:
    normalized_order = order_name.strip().casefold()
    if not normalized_order:
        raise ValueError("Order filter must not be empty")
    return [
        row for row in rows if row["orderName"].strip().casefold() == normalized_order
    ]


def _validate_unique(
    rows: Iterable[TaxonomyRow],
    field: Literal["internalTaxonId", "scientificName"],
    path: Path | None = None,
) -> None:
    values: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        value = (
            row["internalTaxonId"]
            if field == "internalTaxonId"
            else row["scientificName"]
        )
        if not value:
            raise ValueError(f"Empty {field} in {path or 'taxonomy rows'}")
        if value in values:
            duplicates.add(value)
        values.add(value)
    if duplicates:
        duplicate_list = ", ".join(sorted(duplicates))
        raise ValueError(
            f"Duplicate {field} values in {path or 'taxonomy rows'}: {duplicate_list}"
        )


def _changed_fields(old: TaxonomyRow, new: TaxonomyRow) -> tuple[str, ...]:
    old_values = cast("dict[str, str]", old)
    new_values = cast("dict[str, str]", new)
    return tuple(
        field for field in COMPARED_FIELDS if old_values[field] != new_values[field]
    )


def _make_report_row(
    change_type: str,
    old: TaxonomyRow | None,
    new: TaxonomyRow | None,
    changed_fields: Sequence[str],
    *,
    notes_mentions: Sequence[TaxonomyRow] = (),
) -> ReportRow:
    return {
        "change_type": change_type,
        "changed_fields": "|".join(changed_fields),
        "old_internal_taxon_id": old["internalTaxonId"] if old else "",
        "new_internal_taxon_id": new["internalTaxonId"] if new else "",
        "old_scientific_name": old["scientificName"] if old else "",
        "new_scientific_name": new["scientificName"] if new else "",
        "old_order": old["orderName"] if old else "",
        "new_order": new["orderName"] if new else "",
        "old_family": old["familyName"] if old else "",
        "new_family": new["familyName"] if new else "",
        "old_genus": old["genusName"] if old else "",
        "new_genus": new["genusName"] if new else "",
        "old_species_epithet": old["speciesName"] if old else "",
        "new_species_epithet": new["speciesName"] if new else "",
        "old_authority": old["authority"] if old else "",
        "new_authority": new["authority"] if new else "",
        "old_taxonomic_notes": old["taxonomicNotes"] if old else "",
        "new_taxonomic_notes": new["taxonomicNotes"] if new else "",
        "new_taxa_whose_notes_mention_old_name": "|".join(
            f"{row['internalTaxonId']}:{row['scientificName']}"
            for row in notes_mentions
        ),
    }


def compare_taxonomies(
    old_rows: Sequence[TaxonomyRow],
    new_rows: Sequence[TaxonomyRow],
    *,
    include_authority_only: bool = False,
) -> Comparison:
    _validate_unique(old_rows, "internalTaxonId")
    _validate_unique(old_rows, "scientificName")
    _validate_unique(new_rows, "internalTaxonId")
    _validate_unique(new_rows, "scientificName")

    old_by_id = {row["internalTaxonId"]: row for row in old_rows}
    new_by_id = {row["internalTaxonId"]: row for row in new_rows}
    shared_ids = old_by_id.keys() & new_by_id.keys()
    old_only_ids = set(old_by_id) - set(new_by_id)
    new_only_ids = set(new_by_id) - set(old_by_id)

    report_rows: list[ReportRow] = []
    counts: Counter[str] = Counter()

    for taxon_id in shared_ids:
        old = old_by_id[taxon_id]
        new = new_by_id[taxon_id]
        changed_fields = _changed_fields(old, new)
        if not changed_fields:
            continue
        identity_changed = any(field in IDENTITY_FIELDS for field in changed_fields)
        if old["scientificName"] != new["scientificName"]:
            change_type = "renamed"
        elif identity_changed:
            change_type = "taxonomy_changed"
        else:
            change_type = "authority_changed"
        counts[change_type] += 1
        if change_type != "authority_changed" or include_authority_only:
            report_rows.append(_make_report_row(change_type, old, new, changed_fields))

    old_only_by_name = {
        old_by_id[taxon_id]["scientificName"]: old_by_id[taxon_id]
        for taxon_id in old_only_ids
    }
    new_only_by_name = {
        new_by_id[taxon_id]["scientificName"]: new_by_id[taxon_id]
        for taxon_id in new_only_ids
    }
    replacement_names = old_only_by_name.keys() & new_only_by_name.keys()
    for name in replacement_names:
        old = old_only_by_name[name]
        new = new_only_by_name[name]
        changed_fields = ("internalTaxonId", *_changed_fields(old, new))
        report_rows.append(
            _make_report_row("id_changed_same_name", old, new, changed_fields)
        )
        counts["id_changed_same_name"] += 1
        old_only_ids.remove(old["internalTaxonId"])
        new_only_ids.remove(new["internalTaxonId"])

    new_note_index: defaultdict[str, list[TaxonomyRow]] = defaultdict(list)
    for old_id in old_only_ids:
        old_name = old_by_id[old_id]["scientificName"]
        old_name_lower = old_name.casefold()
        for new in new_rows:
            if old_name_lower in new["taxonomicNotes"].casefold():
                new_note_index[old_id].append(new)

    for old_id in old_only_ids:
        old = old_by_id[old_id]
        mentions = sorted(
            new_note_index[old_id], key=lambda row: row["scientificName"].casefold()
        )
        report_rows.append(
            _make_report_row("removed", old, None, (), notes_mentions=mentions)
        )
        counts["removed"] += 1

    for new_id in new_only_ids:
        report_rows.append(_make_report_row("added", None, new_by_id[new_id], ()))
        counts["added"] += 1

    report_rows.sort(key=_report_sort_key)
    return Comparison(
        old_count=len(old_rows),
        new_count=len(new_rows),
        shared_id_count=len(shared_ids),
        rows=tuple(report_rows),
        counts=counts,
    )


def _id_sort_key(value: str) -> tuple[int, int | str]:
    try:
        return (0, int(value))
    except ValueError:
        return (1, value)


def _report_sort_key(row: ReportRow) -> tuple[int, str, tuple[int, int | str]]:
    name = row["old_scientific_name"] or row["new_scientific_name"]
    taxon_id = row["old_internal_taxon_id"] or row["new_internal_taxon_id"]
    return (
        CHANGE_TYPE_ORDER[row["change_type"]],
        name.casefold(),
        _id_sort_key(taxon_id),
    )


def write_report(path: Path, rows: Iterable[ReportRow]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    row_list = list(rows)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=REPORT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(row_list)
    return len(row_list)


def print_summary(comparison: Comparison, output_path: Path) -> None:
    print(f"Old taxonomy records: {comparison.old_count:,}")
    print(f"New taxonomy records: {comparison.new_count:,}")
    print(f"Net record change: {comparison.new_count - comparison.old_count:+,}")
    print(f"Shared internalTaxonIds: {comparison.shared_id_count:,}")
    print(f"Renamed with stable ID: {comparison.counts['renamed']:,}")
    print(
        f"Taxonomy changed without name change: {comparison.counts['taxonomy_changed']:,}"
    )
    print(
        f"Unchanged name with replacement ID: {comparison.counts['id_changed_same_name']:,}"
    )
    print(f"Added unmatched records: {comparison.counts['added']:,}")
    print(f"Removed unmatched records: {comparison.counts['removed']:,}")
    print(f"Authority-only changes: {comparison.counts['authority_changed']:,}")
    print(f"Wrote {len(comparison.rows):,} rows to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "old",
        type=Path,
        help="Old taxonomy CSV, or a directory containing taxonomy.csv",
    )
    parser.add_argument(
        "new",
        type=Path,
        help="New taxonomy CSV, or a directory containing taxonomy.csv",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        type=Path,
        help="Path for the CSV difference report",
    )
    parser.add_argument(
        "--include-authority-only",
        action="store_true",
        help="Include rows whose only changed fields are authority fields",
    )
    parser.add_argument(
        "--order",
        help=(
            "Restrict both taxonomy exports to one order; matching is case-insensitive (for example: CHIROPTERA)"
        ),
    )
    args = parser.parse_args()

    old_path = resolve_taxonomy_path(args.old)
    new_path = resolve_taxonomy_path(args.new)
    old_rows = read_taxonomy(old_path)
    new_rows = read_taxonomy(new_path)
    if args.order is not None:
        old_rows = filter_by_order(old_rows, args.order)
        new_rows = filter_by_order(new_rows, args.order)
        print(f"Order filter: {args.order.strip().upper()}")
    comparison = compare_taxonomies(
        old_rows, new_rows, include_authority_only=args.include_authority_only
    )
    write_report(args.output, comparison.rows)
    print_summary(comparison, args.output)


if __name__ == "__main__":
    main()
