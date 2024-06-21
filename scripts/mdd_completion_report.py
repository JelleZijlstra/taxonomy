"""Script to print completion rates for MDD."""

import argparse
import csv
from pathlib import Path

import gspread

from taxonomy.config import get_options

Data = list[dict[str, str]]


def get_data(filename: Path | None) -> Data:
    if filename is None:
        options = get_options()

        print("downloading MDD names... ")
        gc = gspread.oauth()
        sheet = gc.open(options.mdd_sheet)
        worksheet = sheet.get_worksheet_by_id(options.mdd_worksheet_gid)
        rows = worksheet.get()
        print(f"done, {len(rows)} found")
        headings = rows[0]
        return [
            {
                heading: row[i] if i < len(row) else ""
                for i, heading in enumerate(headings)
            }
            for row in rows[1:]
        ]
    else:
        with filename.open() as file:
            reader = csv.DictReader(file)
            return list(reader)


def print_percentages(data: Data) -> None:
    column_to_count = {
        column: sum(bool(row[column]) for row in data) for column in data[0]
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
# Names with any of the following statuses do not need a type locality or type specimen
NO_TYPE_DATA_STATUSES = {
    "fully_suppressed",
    "nomen_novum",
    "justified_emendation",
    "unjustified_emendation",
    "partially_suppressed",
}


def is_available(row: dict[str, str]) -> bool:
    statuses = set(row["MDD_nomenclature_status"].split(" | "))
    return not (statuses & UNAVAILABLE_STATUSES)


def needs_type_data(row: dict[str, str]) -> bool:
    statuses = set(row["MDD_nomenclature_status"].split(" | "))
    return not (statuses & NO_TYPE_DATA_STATUSES)


def print_percentage(column: str | list[str], names: Data) -> None:
    if isinstance(column, str):
        column = [column]
    count = sum(any(bool(row[column]) for column in column) for row in names)
    print(f"{', '.join(column)}: {count}/{len(names)} ({count/len(names):.1%})")


def print_stats(data: Data) -> None:
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path, nargs="?")
    args = parser.parse_args()
    data = get_data(args.file)
    print_percentages(data)
    print_stats(data)


if __name__ == "__main__":
    main()
