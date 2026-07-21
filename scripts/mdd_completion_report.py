"""Script to print completion rates for MDD."""

import argparse
import csv
import re
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import gspread

from taxonomy.config import get_options
from taxonomy.db import models
from taxonomy.db.constants import NomenclatureStatus

Data = list[dict[str, str]]
Row = dict[str, str]


@dataclass(frozen=True)
class Coverage:
    label: str
    count: int
    denominator: int

    def format(self) -> str:
        percentage = (
            f"{self.count / self.denominator:.1%}" if self.denominator else "n/a"
        )
        return f"{self.label}: {self.count}/{self.denominator} ({percentage})"


def rows_from_values(rows: Sequence[Sequence[Any]]) -> Data:
    if not rows:
        return []
    headings = [str(heading) for heading in rows[0]]
    return [
        {
            heading: str(row[i]) if i < len(row) else ""
            for i, heading in enumerate(headings)
        }
        for row in rows[1:]
    ]


def read_csv(filename: Path) -> Data:
    with filename.open() as file:
        return list(csv.DictReader(file))


def get_worksheet_data(sheet: Any, worksheet_gid: int, label: str) -> Data:
    print(f"downloading MDD {label}... ", end="", flush=True)
    worksheet = sheet.get_worksheet_by_id(worksheet_gid)
    data = rows_from_values(worksheet.get())
    print(f"done, {len(data)} found")
    return data


def get_data(filename: Path | None) -> Data:
    if filename is None:
        options = get_options()
        gc = gspread.oauth()
        sheet = gc.open(options.mdd_sheet)
        return get_worksheet_data(sheet, options.mdd_worksheet_gid, "names")
    return read_csv(filename)


def print_percentages(data: Data) -> None:
    if not data:
        return
    column_to_count = {
        column: sum(is_present(row.get(column, "")) for row in data)
        for column in data[0]
    }
    for column, count in sorted(column_to_count.items(), key=lambda pair: pair[1]):
        print(f"{column}: {count}/{len(data)} ({count/len(data):.1%})")


# Names with any of the following statuses are not available
UNAVAILABLE_STATUSES = {
    "nomen_nudum",
    "name_combination",
    "subsequent_usage",
    "incorrect_subsequent_spelling",
    "before_1758",
    "conditional",
    "hybrid_as_such",
    "hypothetical_concept",
    "inconsistently_binominal",
    "no_type_specified",
    "infrasubspecific",
    "mandatory_change",
    "not_published_with_a_generic_name",
    "not_used_as_valid",
    "unpublished_supplement",
    "unpublished_thesis",
    "variety_or_form",
    "not_intended_as_a_scientific_name",
    "unpublished",
    "unpublished_electronic",
    "not_explicitly_new",
    "variant",
    "incorrect_original_spelling",
    "rejected_by_fiat",
}


def is_available(row: dict[str, str]) -> bool:
    statuses = get_nomenclature_statuses(row)
    return not (statuses & UNAVAILABLE_STATUSES)


def get_nomenclature_statuses(row: Row) -> set[str]:
    return {
        status.strip()
        for status in row.get("MDD_nomenclature_status", "").split("|")
        if status.strip()
    }


def needs_type_data(row: Row) -> bool:
    """Whether a species-group name establishes its own type data."""
    statuses = get_nomenclature_statuses(row)
    if not statuses:
        return False
    try:
        return all(NomenclatureStatus[status].requires_type() for status in statuses)
    except KeyError:
        return False


def is_present(value: str) -> bool:
    return bool(value.strip())


def has_any(row: Row, *columns: str) -> bool:
    return any(is_present(row.get(column, "")) for column in columns)


def has_all(row: Row, *columns: str) -> bool:
    return all(is_present(row.get(column, "")) for column in columns)


def coverage(
    label: str, rows: Sequence[Row], predicate: Callable[[Row], bool]
) -> Coverage:
    return Coverage(label, sum(predicate(row) for row in rows), len(rows))


def print_coverage(stats: Sequence[Coverage]) -> None:
    for stat in stats:
        print(f"  {stat.format()}")


def coordinate_coverage(
    rows: Sequence[Row], latitude_column: str, longitude_column: str
) -> list[Coverage]:
    return [
        coverage(
            "Type locality latitude", rows, lambda row: has_any(row, latitude_column)
        ),
        coverage(
            "Type locality longitude", rows, lambda row: has_any(row, longitude_column)
        ),
        coverage(
            "Both type locality coordinates",
            rows,
            lambda row: has_all(row, latitude_column, longitude_column),
        ),
        coverage(
            "Only one type locality coordinate",
            rows,
            lambda row: has_any(row, latitude_column) != has_any(row, longitude_column),
        ),
    ]


def print_species_stats(species: Data) -> None:
    print("\nMDD species sheet")
    print(f"{len(species)} total species")
    print("Type data (denominator: all species)")
    print_coverage(
        [
            coverage(
                "Type locality", species, lambda row: has_any(row, "typeLocality")
            ),
            *coordinate_coverage(
                species, "typeLocalityLatitude", "typeLocalityLongitude"
            ),
            coverage("Type voucher", species, lambda row: has_any(row, "typeVoucher")),
            coverage("Type kind", species, lambda row: has_any(row, "typeKind")),
            coverage(
                "Type voucher URI", species, lambda row: has_any(row, "typeVoucherURIs")
            ),
        ]
    )
    print("Other nomenclatural data (denominator: all species)")
    print_coverage(
        [
            coverage(
                "Original name combination",
                species,
                lambda row: has_any(row, "originalNameCombination"),
            ),
            coverage(
                "Authority citation",
                species,
                lambda row: has_any(row, "authoritySpeciesCitation"),
            ),
            coverage(
                "Authority link",
                species,
                lambda row: has_any(row, "authoritySpeciesLink"),
            ),
        ]
    )


def print_names_stats(names: Data) -> None:
    need_types = [row for row in names if needs_type_data(row)]
    unknown_statuses = sorted(
        {
            status
            for row in names
            for status in get_nomenclature_statuses(row)
            if status not in NomenclatureStatus.__members__
        }
    )

    print("\nMDD names sheet")
    print(f"{len(names)} total names")
    eligible = Coverage("Names that establish type data", len(need_types), len(names))
    print(f"  {eligible.format()}")
    if unknown_statuses:
        print(
            "  Unknown nomenclature statuses (excluded from the type-data "
            f"denominator): {', '.join(unknown_statuses)}"
        )

    print("Type data (denominator: names that establish type data)")
    print_coverage(
        [
            coverage(
                "Any type locality text",
                need_types,
                lambda row: has_any(
                    row,
                    "MDD_original_type_locality",
                    "MDD_unchecked_type_locality",
                    "MDD_emended_type_locality",
                ),
            ),
            coverage(
                "Original type locality",
                need_types,
                lambda row: has_any(row, "MDD_original_type_locality"),
            ),
            *coordinate_coverage(need_types, "MDD_type_latitude", "MDD_type_longitude"),
            coverage(
                "Type country", need_types, lambda row: has_any(row, "MDD_type_country")
            ),
            coverage(
                "Type specimen", need_types, lambda row: has_any(row, "MDD_holotype")
            ),
            coverage(
                "Type kind", need_types, lambda row: has_any(row, "MDD_type_kind")
            ),
            coverage(
                "Type specimen link",
                need_types,
                lambda row: has_any(row, "MDD_type_specimen_link"),
            ),
        ]
    )
    print("Other nomenclatural data (denominator: all names)")
    print_coverage(
        [
            coverage(
                "Nomenclature status",
                names,
                lambda row: has_any(row, "MDD_nomenclature_status"),
            ),
            coverage(
                "Original name combination",
                names,
                lambda row: has_any(row, "MDD_original_combination"),
            ),
            coverage(
                "Authority citation (checked or unchecked)",
                names,
                lambda row: has_any(
                    row, "MDD_authority_citation", "MDD_unchecked_authority_citation"
                ),
            ),
            coverage(
                "Authority page", names, lambda row: has_any(row, "MDD_authority_page")
            ),
            coverage(
                "Authority link",
                names,
                lambda row: has_any(
                    row, "MDD_authority_link", "MDD_authority_page_link"
                ),
            ),
        ]
    )


def print_percentage(column: str | list[str], names: Data) -> None:
    if isinstance(column, str):
        column = [column]
    count = sum(any(bool(row[column]) for column in column) for row in names)
    print(f"{', '.join(column)}: {count}/{len(names)} ({count/len(names):.1%})")


def print_bhl_percentage(names: Data) -> None:
    count = sum(
        1
        for row in names
        if "biodiversitylibrary.org" in row["MDD_authority_page_link"]
    )
    print(f"BHL link: {count}/{len(names)} ({count/len(names):.1%})")


def citation_stats(names: Data) -> None:
    cites = Counter(row["MDD_authority_citation"] for row in names)
    print("most common citations:")
    for cite, count in cites.most_common(10):
        print(f"{count} {cite} ({count/len(names):.1%})")

    cgs = Counter(row["MDD_citation_group"] for row in names)
    print("most common citation groups:")
    for cg, count in cgs.most_common(10):
        print(f"{count} {cg} ({count/len(names):.1%})")


def tl_stats(names: Data) -> None:
    tls = Counter(row["MDD_type_country"] for row in names)
    print("most common type localities:")
    for tl, count in tls.most_common(10):
        print(f"{count} {tl} ({count/len(names):.1%})")

    tls = Counter(row["MDD_type_subregion"] for row in names)
    print("most common type localities:")
    for tl, count in tls.most_common(30):
        print(f"{count} {tl} ({count/len(names):.1%})")


def extract_multiple_collections(text: str) -> list[str]:
    if not text:
        return []
    if m := re.match(r"^([A-Za-z ]+) \(number not known\)$", text):
        return [m.group(1)]
    try:
        specs = models.name.type_specimen.parse_type_specimen(text)
    except Exception as e:
        print(f"error parsing {text!r}: {e}")
        return []
    collections = [models.name.type_specimen.get_instution_code(spec) for spec in specs]
    return [coll for coll in collections if coll is not None]


def extract_collection(text: str) -> str:
    colls = set(extract_multiple_collections(text))
    if not colls:
        return "none"
    if len(colls) > 1:
        return "multiple"
    return colls.pop()


def type_specimen_stats(names: Data) -> None:
    colls_to_show = 20
    types = [row["MDD_holotype"] for row in names]
    tls = Counter(extract_collection(type) for type in types)
    print(
        f"most common collections out of {len(names)} names (counting shared as 'multiple'):"
    )
    for i, (tl, count) in enumerate(tls.most_common(colls_to_show)):
        print(f"{i}: {count} {tl} ({count/len(names):.1%})")

    # Double-count shared types
    counts2 = Counter[str]()
    for type in types:
        colls = extract_multiple_collections(type)
        for coll in set(colls):
            counts2[coll] += 1
    print(
        f"most common collections out of {len(names)} names (counting shared specimens once for each collection):"
    )
    for i, (tl, count) in enumerate(counts2.most_common(colls_to_show)):
        print(f"{i}: {count} {tl} ({count/len(names):.1%})")

    # Count shared types as fractional
    counts3 = Counter[str]()
    for type in types:
        colls = extract_multiple_collections(type)
        for coll in colls:
            counts3[coll] += 1 / len(colls)  # type: ignore[assignment]
    print(
        f"most common collections out of {len(names)} names (counting shared specimens fractionally):"
    )
    for i, (tl, count) in enumerate(counts3.most_common(colls_to_show)):
        print(f"{i}: {count:.2f} {tl} ({count/len(names):.1%})")

    names_with_type_kind = [row for row in names if row["MDD_type_kind"]]
    kinds = Counter(row["MDD_type_kind"] for row in names_with_type_kind)
    print("most common type kinds:")
    for kind, count in kinds.most_common(10):
        print(f"{count} {kind} ({count/len(names_with_type_kind):.1%})")


def print_detailed_names_stats(data: Data) -> None:
    available = [row for row in data if is_available(row)]
    need_types = [row for row in available if needs_type_data(row)]
    statuses = {row["MDD_nomenclature_status"] for row in available}
    print("Treating names as available with these statuses:", sorted(statuses))
    print(f"{len(data)} total names")
    print(f"{len(available)} available names")
    print(f"{len(need_types)} available names that could have type data")
    print_percentage("MDD_author", available)
    print_percentage("MDD_year", available)
    print_percentage("MDD_original_combination", available)
    print_percentage("MDD_authority_citation", available)
    print_percentage("MDD_unchecked_authority_citation", available)
    print_percentage("MDD_authority_page", available)
    print_percentage(["MDD_authority_link", "MDD_authority_page_link"], available)
    print_percentage("MDD_original_type_locality", need_types)
    print_percentage("MDD_type_latitude", need_types)
    print_percentage("MDD_type_longitude", need_types)
    print_percentage("MDD_type_country", need_types)
    print_percentage("MDD_holotype", need_types)
    print_percentage("MDD_type_specimen_link", need_types)
    print_bhl_percentage(available)
    citation_stats(available)
    tl_stats(need_types)
    type_specimen_stats(need_types)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "names_file",
        type=Path,
        nargs="?",
        help="read the names sheet from this CSV instead of Google Sheets",
    )
    parser.add_argument(
        "--species-file",
        type=Path,
        help="read the species sheet from this CSV instead of Google Sheets",
    )
    parser.add_argument(
        "--all-columns",
        action="store_true",
        help="also print nonempty-value coverage for every column",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="also print citation, locality, and type-collection summaries",
    )
    args = parser.parse_args()

    sheet = None
    if args.names_file is None:
        options = get_options()
        gc = gspread.oauth()
        sheet = gc.open(options.mdd_sheet)
        names = get_worksheet_data(sheet, options.mdd_worksheet_gid, "names")
    else:
        names = read_csv(args.names_file)

    if args.species_file is not None:
        species = read_csv(args.species_file)
    elif sheet is not None:
        species = get_worksheet_data(
            sheet, options.mdd_species_worksheet_gid, "species"
        )
    else:
        # Preserve the old offline invocation, which accepted only a names CSV.
        species = None

    if species is not None:
        print_species_stats(species)
    print_names_stats(names)

    if args.all_columns:
        if species is not None:
            print("\nAll species-sheet columns")
            print_percentages(species)
        print("\nAll names-sheet columns")
        print_percentages(names)
    if args.details:
        print("\nDetailed names-sheet summaries")
        print_detailed_names_stats(names)


if __name__ == "__main__":
    main()
