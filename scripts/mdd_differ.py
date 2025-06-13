"""Compare two versions of the MDD."""

import argparse
import csv
import functools
from collections.abc import Iterable
from pathlib import Path
from typing import Any, TypedDict, cast

import gspread

from taxonomy.config import get_options


class MDDSpeciesRow(TypedDict):
    sciName: str
    id: str
    phylosort: str
    mainCommonName: str
    otherCommonNames: str
    subclass: str
    infraclass: str
    magnorder: str
    superorder: str
    order: str
    suborder: str
    infraorder: str
    parvorder: str
    superfamily: str
    family: str
    subfamily: str
    tribe: str
    genus: str
    subgenus: str
    specificEpithet: str
    authoritySpeciesAuthor: str
    authoritySpeciesYear: str
    authorityParentheses: str
    originalNameCombination: str
    authoritySpeciesCitation: str
    authoritySpeciesLink: str
    typeVoucher: str
    typeKind: str
    typeVoucherURIs: str
    typeLocality: str
    typeLocalityLatitude: str
    typeLocalityLongitude: str
    nominalNames: str
    taxonomyNotes: str
    taxonomyNotesCitation: str
    distributionNotes: str
    distributionNotesCitation: str
    subregionDistribution: str
    countryDistribution: str
    continentDistribution: str
    biogeographicRealm: str
    iucnStatus: str
    extinct: str
    domestic: str
    flagged: str
    CMW_sciName: str
    diffSinceCMW: str
    MSW3_matchtype: str
    MSW3_sciName: str
    diffSinceMSW3: str


class DiffLine(TypedDict):
    species_id: str
    old_name: str
    new_name: str
    category: str
    column: str
    old_value: str
    new_value: str


@functools.cache
def get_sheet() -> Any:
    options = get_options()
    gc = gspread.oauth()
    return gc.open(options.mdd_sheet)


def get_mdd_species(input_csv: str | None = None) -> dict[str, MDDSpeciesRow]:
    if input_csv is None:
        sheet = get_sheet()
        options = get_options()
        worksheet = sheet.get_worksheet_by_id(options.mdd_species_worksheet_gid)
        raw_rows = worksheet.get()
    else:
        with Path(input_csv).open() as f:
            raw_rows = list(csv.reader(f))
    headings = raw_rows[0]
    species = [
        cast("MDDSpeciesRow", dict(zip(headings, row, strict=False)))
        for row in raw_rows[1:]
    ]
    return {sp["id"]: sp for sp in species}


def generate_diff_lines(
    old: dict[str, MDDSpeciesRow], new: dict[str, MDDSpeciesRow]
) -> Iterable[DiffLine]:
    added = set(new.keys()) - set(old.keys())
    for species_id in added:
        new_row = new[species_id]
        yield DiffLine(
            species_id=species_id,
            old_name="",
            new_name=new_row["sciName"],
            category="added",
            column="",
            old_value="",
            new_value="",
        )
    removed = set(old.keys()) - set(new.keys())
    for species_id in removed:
        old_row = old[species_id]
        yield DiffLine(
            species_id=species_id,
            old_name=old_row["sciName"],
            new_name="",
            category="removed",
            column="",
            old_value="",
            new_value="",
        )

    for species_id in set(old.keys()).intersection(new.keys()):
        old_row = old[species_id]
        new_row = new[species_id]
        for column in MDDSpeciesRow.__annotations__:
            old_value = old_row[column].strip()  # type: ignore[literal-required]
            new_value = new_row[column].strip()  # type: ignore[literal-required]
            if old_value != new_value:
                yield DiffLine(
                    species_id=species_id,
                    old_name=old_row["sciName"],
                    new_name=new_row["sciName"],
                    category="changed",
                    column=column,
                    old_value=old_value,
                    new_value=new_value,
                )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare two versions of the MDD and report differences."
    )
    parser.add_argument("old", help="Path to the old MDD CSV file", type=str)
    parser.add_argument(
        "new",
        help="Path to the new MDD CSV file (omit to use the online version)",
        nargs="?",
        type=str,
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Path to output file for differences",
        type=str,
        default=None,
    )

    args = parser.parse_args()
    old_data = get_mdd_species(args.old)
    new_data = get_mdd_species(args.new)

    diff = list(generate_diff_lines(old_data, new_data))
    if args.output is not None:
        with Path(args.output).open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=DiffLine.__annotations__.keys())
            writer.writeheader()
            for line in diff:
                writer.writerow(line)
        print(f"Differences written to {args.output}")
    else:
        for line in diff:
            print(
                f"{line['category'].capitalize()} - {line['species_id']}: {line['old_name']} -> {line['new_name']} ({line['column']}: '{line['old_value']}' -> '{line['new_value']}')"
            )
        if not diff:
            print("No differences found.")


if __name__ == "__main__":
    main()
