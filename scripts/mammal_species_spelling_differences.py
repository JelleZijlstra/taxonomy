"""Summarize spelling differences for valid mammal species.

For ASM abstract.

"""

import argparse
import csv
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from taxonomy.db.constants import Rank
from taxonomy.db.models.classification_entry.ce import ClassificationEntry
from taxonomy.db.models.taxon.taxon import Taxon
from taxonomy.db.nomenclature_book import PAST_TREATMENTS, get_all_ces, get_taxa

DEFAULT_OUTPUT = Path("mammal_species_spelling_differences.csv")
SOURCE_LABELS = [label for label, _ in PAST_TREATMENTS]


@dataclass(frozen=True)
class SourceUsage:
    name: str
    epithet: str

    @classmethod
    def current(cls, taxon: Taxon) -> SourceUsage:
        _, epithet = split_species_name(taxon.valid_name)
        return cls(name=taxon.valid_name, epithet=epithet)

    @classmethod
    def from_ce(cls, ce: ClassificationEntry) -> SourceUsage | None:
        name = ce.get_corrected_name() or ce.name
        try:
            _, epithet = split_species_name(name)
        except ValueError:
            return None
        return cls(name=ce.name, epithet=epithet)


@dataclass(frozen=True)
class SpeciesReportRow:
    taxon: Taxon
    source_to_usages: dict[str, tuple[SourceUsage, ...]]

    @property
    def current(self) -> SourceUsage:
        return SourceUsage.current(self.taxon)

    def differing_sources(self) -> list[str]:
        return [
            label
            for label in SOURCE_LABELS
            if any(
                usage.epithet != self.current.epithet
                for usage in self.source_to_usages.get(label, ())
            )
        ]

    def to_csv(self) -> dict[str, str]:
        order = self.taxon.get_derived_field("order")
        family = self.taxon.get_derived_field("family")
        row = {
            "order": order.valid_name if order and order.rank is Rank.order else "",
            "family": (
                family.valid_name if family and family.rank is Rank.family else ""
            ),
            "current_name": self.taxon.valid_name,
        }
        for label in SOURCE_LABELS:
            usages = self.source_to_usages.get(label, ())
            names = sorted({usage.name for usage in usages})
            row[label] = " | ".join(names)
        return row


def split_species_name(name: str) -> tuple[str, str]:
    parts = name.split()
    if len(parts) < 2:
        raise ValueError(f"Could not parse species name: {name!r}")
    return parts[0], parts[-1]


def get_current_mammal_species() -> list[Taxon]:
    root = Taxon.getter("valid_name")("Mammalia")
    assert root is not None
    return [taxon for taxon in get_taxa(root) if taxon.rank is Rank.species]


def get_source_usages_by_taxon() -> dict[Taxon, dict[str, tuple[SourceUsage, ...]]]:
    taxon_to_ces = get_all_ces()
    result: dict[Taxon, dict[str, tuple[SourceUsage, ...]]] = {}
    for taxon, labeled_ces in taxon_to_ces.items():
        label_to_usages: dict[str, set[SourceUsage]] = {}
        for label, ce in labeled_ces:
            if ce.rank is not Rank.species:
                continue
            usage = SourceUsage.from_ce(ce)
            if usage is None:
                continue
            label_to_usages.setdefault(label, set()).add(usage)
        if label_to_usages:
            result[taxon] = {
                label: tuple(sorted(usages, key=lambda usage: usage.name))
                for label, usages in label_to_usages.items()
            }
    return result


def find_rows(
    taxa: Iterable[Taxon],
    source_usages: dict[Taxon, dict[str, tuple[SourceUsage, ...]]],
) -> list[SpeciesReportRow]:
    rows: list[SpeciesReportRow] = []
    for taxon in taxa:
        row = SpeciesReportRow(taxon, source_usages.get(taxon, {}))
        if row.differing_sources():
            rows.append(row)
    return sorted(rows, key=lambda row: row.taxon.valid_name)


def write_csv(path: Path, rows: Iterable[SpeciesReportRow]) -> None:
    csv_rows = [row.to_csv() for row in rows]
    columns = ["order", "family", "current_name"]
    columns.extend(SOURCE_LABELS)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(csv_rows)


def print_summary(
    taxa: list[Taxon],
    rows: list[SpeciesReportRow],
    source_usages: dict[Taxon, dict[str, tuple[SourceUsage, ...]]],
) -> None:
    total_species = len(taxa)
    species_with_difference = len(rows)
    print(
        f"Current recognized mammal species: {total_species:,}; "
        f"with a different spelling in at least one major source: "
        f"{species_with_difference:,} ({format_percentage(species_with_difference, total_species)})."
    )
    print()
    print("By source:")

    taxon_to_row = {row.taxon: row for row in rows}
    totals: Counter[str] = Counter()
    different: Counter[str] = Counter()
    current_taxa = set(taxa)
    for taxon, label_to_usages in source_usages.items():
        if taxon not in current_taxa:
            continue
        current_epithet = SourceUsage.current(taxon).epithet
        for label, usages in label_to_usages.items():
            if not usages:
                continue
            totals[label] += 1
            if any(usage.epithet != current_epithet for usage in usages):
                different[label] += 1

    for label in SOURCE_LABELS:
        total = totals[label]
        diff = different[label]
        print(
            f"{label}: {diff:,} of {total:,} species recognized both by {label} "
            f"and currently have a different epithet ({format_percentage(diff, total)})."
        )

    source_count_counter = Counter(
        len(taxon_to_row[taxon].differing_sources()) for taxon in taxon_to_row
    )
    if source_count_counter:
        print()
        print("Differing-source count per affected species:")
        for num_sources, count in sorted(source_count_counter.items()):
            print(f"{num_sources} source(s): {count:,} species")


def format_percentage(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "n/a"
    return f"{numerator / denominator:.1%}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Report mammal species whose specific epithet differs between the "
            "current taxonomy and at least one major source used by nomenclature_book."
        )
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    taxa = get_current_mammal_species()
    source_usages = get_source_usages_by_taxon()
    rows = find_rows(taxa, source_usages)
    write_csv(args.csv, rows)
    print(f"Wrote {len(rows):,} rows to {args.csv}")
    print_summary(taxa, rows, source_usages)


if __name__ == "__main__":
    main()
