"""Script to sync the journals sheet."""

import argparse
import csv
import datetime
import itertools
import pprint
import re
import sys
import time
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TypedDict, TypeVar, cast

import gspread

from taxonomy import getinput
from taxonomy.config import get_options
from taxonomy.db import export, models
from taxonomy.db.constants import AgeClass, Group, Rank, RegionKind
from taxonomy.db.models import Taxon
from taxonomy.db.models.citation_group import CitationGroup, CitationGroupTag


class Row(TypedDict):
    MDD_citation_group: str
    BHL_biblio: str
    Other_BHL_biblio: str
    Non_BHL_link: str
    Connor_Comments: str
    Jelle_Comments: str
    Hesp_cg_id: str
    num_extant_mammals: str
    num_without_verified_citation: str
    start_year: str
    end_year: str
    country: str


OMITTED_COLUMNS = {"Connor_Comments", "Jelle_Comments", "Other_BHL_biblio"}


def get_cg_country(cg: CitationGroup) -> str:
    if cg.region is None:
        return ""
    region = cg.region
    while region is not None and region.kind not in (
        RegionKind.planet,
        RegionKind.continent,
        RegionKind.country,
    ):
        region = region.parent
    if region is None:
        return ""
    return region.name


def get_hesp_row(
    cg: CitationGroup,
    cg_to_extant_mammals: Mapping[int, int],
    cg_to_extant_mammals_without_original: Mapping[int, int],
) -> Row:
    year_range = cg.get_active_year_range()
    if year_range is None:
        start_year = end_year = ""
    else:
        start_year, end_year = map(str, year_range)
    title_ids = cg.get_bhl_title_ids()
    url_tag = cg.get_tag(models.citation_group.CitationGroupTag.CitationGroupURL)
    return {
        "MDD_citation_group": cg.name,
        "BHL_biblio": " | ".join(
            f"https://www.biodiversitylibrary.org/bibliography/{title_id}"
            for title_id in title_ids
        ),
        "Other_BHL_biblio": "",
        "Non_BHL_link": url_tag.text if url_tag else "",
        "Connor_Comments": "",
        "Jelle_Comments": "",
        "Hesp_cg_id": str(cg.id),
        "num_extant_mammals": str(cg_to_extant_mammals.get(cg.id, 0)),
        "num_without_verified_citation": str(
            cg_to_extant_mammals_without_original.get(cg.id, 0)
        ),
        "start_year": start_year,
        "end_year": end_year,
        "country": get_cg_country(cg),
    }


@dataclass
class FixableDifference:
    row_idx: int
    col_idx: int
    explanation: str | None
    column: str
    hesp_value: str
    mdd_value: str
    hesp_row: Row
    mdd_row: Row
    cg: CitationGroup

    def summary(self) -> str:
        if self.explanation is None:
            return f"{self.hesp_value} (H) / {self.mdd_value} (M) [{self.cg}]"
        return f"{self.explanation}: {self.hesp_value} (H) / {self.mdd_value} (M) [{self.cg}]"

    def print(self) -> None:
        expl = f": {self.explanation}" if self.explanation else ""
        print(f"- {self.column}{expl} ({self.cg})")
        if self.hesp_value:
            print(f"    - H: {self.hesp_value}")
        if self.mdd_value:
            print(f"    - M: {self.mdd_value}")


def compare_column(
    hesp_row: Row,
    mdd_row: Row,
    *,
    column: str,
    counts: dict[str, int],
    row_idx: int,
    col_idx: int,
    cg: CitationGroup,
) -> FixableDifference | None:
    mdd_value = mdd_row[column]  # type: ignore[literal-required]
    hesp_value = str(hesp_row.get(column, ""))
    explanation: str | None = None
    match (bool(hesp_value), bool(mdd_value)):
        case (True, False):
            counts[f"{column} missing in MDD"] += 1
            explanation = "H only"
        case (False, True):
            counts[f"{column} missing in Hesperomys"] += 1
            explanation = "M only"
        case (True, True):
            if hesp_value != mdd_value:
                counts[f"{column} differences"] += 1
            else:
                return None
        case (False, False):
            return None
    return FixableDifference(
        row_idx=row_idx,
        col_idx=col_idx,
        explanation=explanation,
        column=column,
        hesp_value=hesp_value,
        mdd_value=mdd_value,
        hesp_row=hesp_row,
        mdd_row=mdd_row,
        cg=cg,
    )


def compare_year(hesp_year: str, mdd_year: str) -> int:
    try:
        return abs(int(hesp_year) - int(mdd_year))
    except ValueError:
        return 1000


COLUMN_RENAMES = {"MDD original combination": "MDD_original_combination"}
REMOVED_COLUMNS = {"citation_status", "author_status", "type_locality_status"}

T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Iterable[list[T]]:
    it = iter(iterable)
    while chunk := list(itertools.islice(it, n)):
        yield chunk


def process_value_for_sheets(value: str) -> str | int:
    if value.isdigit():
        return int(value)
    return value


def get_rows() -> Iterable[tuple[Row, CitationGroup]]:
    taxon = Taxon.getter("valid_name")("Mammalia")
    assert taxon is not None
    hesp_names = export.get_names_for_export(
        taxon,
        ages={AgeClass.extant, AgeClass.recently_extinct},
        group=Group.species,
        min_rank_for_age_filtering=Rank.species,
    )
    cg_count = Counter(
        nam_cg.id for nam in hesp_names if (nam_cg := nam.get_citation_group())
    )
    cg_count_without_original = Counter(
        nam_cg.id
        for nam in hesp_names
        if nam.original_citation is None and (nam_cg := nam.get_citation_group())
    )
    for cg in CitationGroup.select_valid():
        if cg.id in cg_count:
            yield get_hesp_row(cg, cg_count, cg_count_without_original), cg


def run(*, dry_run: bool = True, taxon: Taxon) -> None:
    options = get_options()
    backup_path = (
        options.data_path
        / "mdd_cg_updater"
        / datetime.datetime.now(datetime.UTC).isoformat()
    )
    backup_path.mkdir(parents=True, exist_ok=True)

    print("downloading MDD sheet... ")
    gc = gspread.oauth()
    sheet = gc.open(options.mdd_sheet)
    worksheet = sheet.get_worksheet_by_id(options.mdd_journals_worksheet_gid)
    rows = worksheet.get()
    headings = rows[0]
    column_to_idx = {heading: i for i, heading in enumerate(headings, start=1)}
    print(f"done, {len(rows)} found")

    print("backing up MDD names... ")
    with (backup_path / "mdd_journals.csv").open("w") as file:
        writer = csv.writer(file)
        for row in rows:
            writer.writerow(row)
    print(f"done, backup at {backup_path}")

    hesp_rows = list(get_rows())
    name_to_row = {row["MDD_citation_group"]: (row, cg) for row, cg in hesp_rows}
    id_to_row = {row["Hesp_cg_id"]: (row, cg) for row, cg in hesp_rows}
    unused_cg_ids = set(id_to_row.keys())
    counts: dict[str, int] = Counter()
    fixable_differences: list[FixableDifference] = []

    for row_idx, mdd_row_as_list in getinput.print_every_n(
        enumerate(rows[1:], start=2), label="MDD names"
    ):
        mdd_row = cast(
            Row,
            {
                headings[i]: (mdd_row_as_list[i] if i < len(mdd_row_as_list) else "")
                for i in range(len(headings))
            },
        )
        if mdd_row["Hesp_cg_id"]:
            hesp_row, cg = id_to_row.get(mdd_row["Hesp_cg_id"], (None, None))
        else:
            hesp_row, cg = name_to_row.get(mdd_row["MDD_citation_group"], (None, None))
        if hesp_row is None or cg is None:
            print("Missing:", mdd_row)
            continue
        hesp_id = hesp_row["Hesp_cg_id"]
        if hesp_id not in unused_cg_ids:
            print("Already used", hesp_id, "in Hesp")
            continue
        unused_cg_ids.remove(hesp_id)

        for column in headings:
            if column in OMITTED_COLUMNS:
                continue
            if diff := compare_column(
                hesp_row,
                mdd_row,
                column=column,
                counts=counts,
                row_idx=row_idx,
                col_idx=column_to_idx[column],
                cg=cg,
            ):
                fixable_differences.append(diff)

    missing_in_mdd = []
    for hesp_id in unused_cg_ids:
        row, _ = id_to_row[hesp_id]
        missing_in_mdd.append(row)

    with (backup_path / "summary.txt").open("w") as file:
        for f in (sys.stdout, file):
            print("Report:", file=f)
            print(f"Total MDD names: {len(rows) - 1}", file=f)
            print(f"Total Hesp names: {len(hesp_rows)}", file=f)
            print(f"Missing in MDD: {len(missing_in_mdd)}", file=f)
            for key, value in sorted(counts.items()):
                print(f"{key}: {value}", file=f)

    if missing_in_mdd:
        getinput.print_header(f"Missing in MDD {len(missing_in_mdd)}")
        for row in missing_in_mdd:
            pprint.pp(row)
        add_all = getinput.yes_no("Add all?")
        if not add_all:
            ask_individually = getinput.yes_no("Ask individually?")
        else:
            ask_individually = False

        for row in missing_in_mdd:
            if add_all:
                should_add = True
            elif ask_individually:
                pprint.pp(row)
                should_add = getinput.yes_no("Add?")
            else:
                should_add = False
            if not should_add:
                continue
            row_list = [
                process_value_for_sheets(row.get(column, "")) for column in headings
            ]
            if not dry_run:
                worksheet.append_row(row_list)

        with (backup_path / "missing-in-mdd.csv").open("w") as file:
            writer = csv.writer(file)
            missing_in_mdd_headings = list(missing_in_mdd[0])
            writer.writerow(missing_in_mdd_headings)
            for row in missing_in_mdd:
                writer.writerow(
                    [row.get(column, "") for column in missing_in_mdd_headings]
                )

    fixable_differences = sorted(
        fixable_differences, key=lambda x: (x.column, x.hesp_value, x.mdd_value)
    )
    if fixable_differences:
        with (backup_path / "fixable-differences.csv").open("w") as file:
            dict_writer = csv.DictWriter(
                file,
                fieldnames=[
                    "row_idx",
                    "col_idx",
                    "explanation",
                    "column",
                    "hesp_value",
                    "mdd_value",
                    "hesp_id",
                    "name",
                    "applied",
                ],
            )
            dict_writer.writeheader()
            for mdd_column, group_iter in itertools.groupby(
                fixable_differences, key=lambda x: x.column
            ):
                group = list(group_iter)
                getinput.print_header(f"Differences for {mdd_column} ({len(group)})")
                for diff in group:
                    diff.print()
                add_all = getinput.yes_no("Accept all?")
                if not add_all:
                    ask_individually = getinput.yes_no("Ask individually?")
                else:
                    ask_individually = False
                if not ask_individually and not add_all:
                    accept_hesp_only = getinput.yes_no("Accept Hesp-only values?")
                else:
                    accept_hesp_only = False
                updates_to_make = []
                for diff in group:
                    if not diff.hesp_value:
                        print("No Hesp value", diff.summary())
                    if add_all:
                        should_add = True
                    elif accept_hesp_only:
                        should_add = not diff.mdd_value
                    elif ask_individually:
                        diff.print()
                        should_add = getinput.yes_no("Apply?")
                    else:
                        should_add = False
                    if should_add:
                        skip_sheet_update = False
                        if mdd_column == "BHL_biblio" and not diff.hesp_value:
                            for piece in diff.mdd_value.split(" | "):
                                match = re.fullmatch(
                                    r"https://www\.biodiversitylibrary\.org/bibliography/(\d+)",
                                    piece,
                                )
                                if not match:
                                    print("Invalid BHL link", piece)
                                else:
                                    bhl_biblio = match.group(1)
                                    tag = CitationGroupTag.BHLBibliography(bhl_biblio)
                                    print(f"Add tag to {diff.cg}: {tag}")
                                    diff.cg.add_tag(tag)
                                    skip_sheet_update = True
                        if mdd_column == "Non_BHL_link" and not diff.hesp_value:
                            tag = CitationGroupTag.CitationGroupURL(diff.mdd_value)
                            print(f"Add tag to {diff.cg}: {tag}")
                            diff.cg.add_tag(tag)
                            skip_sheet_update = True

                        if not skip_sheet_update:
                            updates_to_make.append(
                                gspread.cell.Cell(
                                    row=diff.row_idx,
                                    col=diff.col_idx,
                                    value=process_value_for_sheets(diff.hesp_value),
                                )
                            )
                    dict_writer.writerow(
                        {
                            "row_idx": diff.row_idx,
                            "col_idx": diff.col_idx,
                            "explanation": diff.explanation,
                            "column": diff.column,
                            "hesp_value": diff.hesp_value,
                            "mdd_value": diff.mdd_value,
                            "hesp_id": diff.hesp_row["Hesp_cg_id"],
                            "name": diff.hesp_row["MDD_citation_group"],
                            "applied": str(int(should_add)),
                        }
                    )

                if dry_run:
                    print("Make change:", updates_to_make)
                else:
                    done = 0
                    print("Applying changes for column", mdd_column)
                    for batch in batched(updates_to_make, 500):
                        worksheet.update_cells(batch)
                        done += len(batch)
                        print(f"Done {done}/{len(updates_to_make)}")
                        if len(batch) == 500:
                            time.sleep(30)

    print("Done. Data saved at", backup_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--taxon", nargs="?", default="Mammalia")
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()
    root = Taxon.getter("valid_name")(args.taxon)
    if root is None:
        print("Invalid taxon", args.taxon)
        sys.exit(1)
    run(taxon=root, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
