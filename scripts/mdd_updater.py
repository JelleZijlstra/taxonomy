"""

Notes for auto-gsheet updating:

- Use https://pypi.org/project/gspread/
- For now, leave alone species status (_S. borbonicus_, squirrels) and spelling (bats)
- Take a backup of the sheet before any changes
- Generate a summary file with changes applied
- For each category of changes, first show them all and ask whether to apply them all
  at once. If yes, apply them. Else, ask whether to go over them one by one.
- For new names, first get the max() of the existing MDD ids and start from there.

To set up gspread, follow the instructions in https://docs.gspread.org/en/latest/oauth2.html#oauth-client-id
to get an OAuth client id. The tokens appears to expire after a week. Notes for next time:

- Go to  https://console.cloud.google.com/apis/api/sheets.googleapis.com/credentials?authuser=1&project=directed-tracer-123911&supportedpurview=project
- Add an "OAuth 2.0 Client ID" credential for a desktop app
- Download the credentials and put them in ~/.config/gspread/credentials.json
- Delete ~/.config/gspread/authorized_user.json (maybe just deleting this file is enough? try it next time)
- Run this script with --gspread-test to re-authorize. Make sure to fix cell A1 in the MDD sheet back afterwards.

"""

import argparse
import csv
import datetime
import enum
import functools
import itertools
import pprint
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, TypeVar

import gspread
import Levenshtein

from scripts import mdd_diff
from taxonomy import getinput
from taxonomy.config import get_options
from taxonomy.db import export, models
from taxonomy.db.constants import AgeClass, Group, Rank, RegionKind, Status
from taxonomy.db.models import Name, Taxon
from taxonomy.db.models.name import TypeTag


@functools.cache
def resolve_hesp_id(hesp_id_str: str) -> int | None:
    if hesp_id_str:
        hesp_id = int(hesp_id_str)
        row = Name.select().filter(Name.id == hesp_id).first()
        if row is None:
            return None
        while row.target is not None:
            row = row.target
        return row.id
    return None


def get_mdd_like_species_name(name: Name) -> str:
    if name.taxon.base_name.status is not Status.valid:
        try:
            genus = name.taxon.parent_of_rank(Rank.genus)
        except ValueError:
            return "incertae_sedis incertae_sedis"
        else:
            if genus.base_name.status is Status.valid:
                return f"{genus.valid_name} incertae_sedis"
            else:
                return "incertae_sedis incertae_sedis"
    try:
        return name.taxon.parent_of_rank(Rank.species).valid_name
    except ValueError:
        return ""


def get_mdd_status(name: Name) -> str:
    match name.status:
        case Status.valid:
            match name.taxon.rank:
                case Rank.species:
                    return "species"
                case Rank.subspecies:
                    if name.taxon.parent.base_name == name:
                        return "species"
                    return "synonym"
                case _:
                    return f"unexpected ({name.taxon.rank})"
        case Status.synonym:
            match name.taxon.base_name.status:
                case Status.valid:
                    return "synonym"
                case _:
                    return name.taxon.base_name.status.name
        case _:
            return name.status.name


def get_type_locality_country_and_subregion(nam: Name) -> tuple[str, str]:
    if nam.type_locality is None:
        return "", ""
    region = nam.type_locality.region
    regions = [region.name]
    while region is not None and region.kind not in (
        RegionKind.planet,
        RegionKind.continent,
        RegionKind.country,
    ):
        region = region.parent
        regions.append(region.name)
    regions.reverse()
    if len(regions) == 1:
        return regions[0], ""
    return regions[0], regions[1]


def get_authority_link(nam: Name) -> str:
    tags = nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink)
    return " | ".join(sorted({tag.url for tag in tags}))


OMITTED_COLUMNS = {
    "MDD_old_type_locality",
    "MDD_emended_type_locality",
    "MDD_type_latitude",
    "MDD_type_longitude",
    "MDD_subspecificEpithet",
    "MDD_comments",
    # Identifiers
    "MDD_syn_ID",
    "Hesp_id",
}


def get_hesp_row(name: Name, need_initials: set[str]) -> dict[str, Any]:
    row = {**export.data_for_name(name)}
    row["Hesp_species"] = get_mdd_like_species_name(name)
    row["Hesp_root_name"] = row["root_name"]
    mdd_style_author = mdd_diff.get_mdd_style_authority(name, need_initials)
    row["Hesp_author"] = mdd_style_author
    row["Hesp_year"] = row["year"]
    row["Hesp_nomenclature_status"] = row["nomenclature_status"]
    row["Hesp_validity"] = get_mdd_status(name)
    row["Hesp_original_combination"] = row["original_name"]

    # Citation
    row["Hesp_authority_citation"] = row["original_citation"]
    row["Hesp_unchecked_authority_citation"] = row["verbatim_citation"]
    row["Hesp_citation_group"] = row["citation_group"]
    row["Hesp_authority_page"] = row["page_described"]
    authority_link = get_authority_link(name)
    row["Hesp_authority_page_link"] = authority_link
    if name.original_citation is not None:
        url = name.original_citation.geturl()
        row["Hesp_authority_link"] = url or ""
    else:
        row["Hesp_authority_link"] = ""
    if authority_link:
        row["Hesp_unchecked_authority_page_link"] = ""
    else:
        # At most 3
        candidates = models.name.lint.get_candidate_bhl_pages(name)
        row["Hesp_unchecked_authority_page_link"] = " | ".join(
            sorted(page.page_url for page in candidates)[:3]
        )

    # Type locality
    # Omit: MDD_old_type_locality
    # Omit: MDD_emended_type_locality
    verbatim_tl = []
    emended_tl = []
    for tag in name.type_tags:
        if isinstance(tag, TypeTag.LocationDetail):
            if tag.source == name.original_citation:
                verbatim_tl.append(tag.text)
            else:
                citation = ", ".join(tag.source.taxonomicAuthority())
                emended_tl.append(f'"{tag.text}" ({citation})')
        elif isinstance(tag, TypeTag.Coordinates):
            row["Hesp_type_latitude"] = tag.latitude
            row["Hesp_type_longitude"] = tag.longitude
    if verbatim_tl:
        row["Hesp_original_type_locality"] = " | ".join(verbatim_tl)
    if emended_tl:
        row["Hesp_unchecked_type_locality"] = " | ".join(emended_tl)
    row["Hesp_type_country"], row["Hesp_type_subregion"] = (
        get_type_locality_country_and_subregion(name)
    )

    # Type specimen
    row["Hesp_holotype"] = row["type_specimen"]
    row["Hesp_type_kind"] = row["species_type_kind"]

    # Higher classification
    taxon = name.taxon
    order = taxon.get_derived_field("order")
    if order is not None and order.rank is Rank.order:
        row["Hesp_order"] = order.valid_name
    else:
        row["Hesp_order"] = "incertae_sedis"
    family = taxon.get_derived_field("family")
    if family is not None and family.rank is Rank.family:
        row["Hesp_family"] = family.valid_name
    else:
        row["Hesp_family"] = "incertae_sedis"
    try:
        genus = taxon.parent_of_rank(Rank.genus)
    except ValueError:
        genus = None
    if genus is None or genus.base_name.status is not Status.valid:
        row["Hesp_genus"] = "incertae_sedis"
    else:
        row["Hesp_genus"] = genus.valid_name
    try:
        species = taxon.parent_of_rank(Rank.species)
    except ValueError:
        species = None
    if species is None or species.base_name.status is not Status.valid:
        row["Hesp_specificEpithet"] = "incertae_sedis"
    else:
        row["Hesp_specificEpithet"] = species.base_name.root_name

    # Other
    # TODO: MDD_subspecificEpithet
    # TODO: MDD_comments
    return row


class DifferenceKind(enum.StrEnum):
    missing_in_mdd = "missing in MDD"
    missing_in_hesp = "missing in Hesp"
    differences = "differences"


@dataclass
class FixableDifference:
    row_idx: int
    col_idx: int
    explanation: str | None
    mdd_column: str
    hesp_value: str
    mdd_value: str
    hesp_row: dict[str, str] = field(repr=False)
    mdd_row: dict[str, str] = field(repr=False)
    hesp_name: Name

    def summary(self) -> str:
        if self.explanation is None:
            return f"{self.hesp_value} (H) / {self.mdd_value} (M) [{self.hesp_name}]"
        return f"{self.explanation}: {self.hesp_value} (H) / {self.mdd_value} (M) [{self.hesp_name}]"

    def print(self) -> None:
        expl = f": {self.explanation}" if self.explanation else ""
        print(f"- {self.mdd_column}{expl} ({self.hesp_name})")
        if self.hesp_value:
            print(f"    - H: {self.hesp_value}")
        if self.mdd_value:
            print(f"    - M: {self.mdd_value}")

    @property
    def kind(self) -> DifferenceKind:
        if not self.hesp_value:
            return DifferenceKind.missing_in_hesp
        if not self.mdd_value:
            return DifferenceKind.missing_in_mdd
        return DifferenceKind.differences

    def apply_to_hesp(self, *, dry_run: bool) -> None:
        match self.mdd_column:
            case "MDD_authority_link":
                if self.hesp_name.original_citation is None:
                    print(f"{self}: skip applying because no original citation")
                    return
                if self.hesp_name.original_citation.geturl() is not None:
                    print(f"{self}: skip applying because already has a link")
                    return
                print(
                    f"{self}: set url to {self.mdd_value} for {self.hesp_name.original_citation}"
                )
                if not dry_run:
                    self.hesp_name.original_citation.url = self.mdd_value
            case "MDD_authority_page_link":
                if self.hesp_name.page_described is None:
                    tag = models.name.TypeTag.AuthorityPageLink(
                        self.mdd_value, True, ""
                    )
                else:
                    pages = list(
                        models.name.lint.extract_pages(self.hesp_name.page_described)
                    )
                    if len(pages) == 1:
                        tag = models.name.TypeTag.AuthorityPageLink(
                            self.mdd_value, True, pages[0]
                        )
                    else:
                        tag = models.name.TypeTag.AuthorityPageLink(
                            self.mdd_value, True, ""
                        )
                print(f"{self}: add tag {tag}")
                if not dry_run:
                    self.hesp_name.add_type_tag(tag)

            case _:
                print(
                    f"{self}: skip applying because no action defined for {self.mdd_column}"
                )

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = {**self.hesp_name.get_adt_callbacks()}
        if self.mdd_column in ("MDD_authority_link", "MDD_authority_page_link"):
            if self.hesp_value:
                callbacks["open_hesp"] = functools.partial(_open_urls, self.hesp_value)
            if self.mdd_value:
                callbacks["open_mdd"] = functools.partial(_open_urls, self.mdd_value)
        return callbacks


def _open_urls(urls: str) -> None:
    for url in urls.split(" | "):
        subprocess.run(["open", url])


def compare_column(
    hesp_row: dict[str, str],
    mdd_row: dict[str, str],
    *,
    hesp_name: Name,
    mdd_column: str,
    compare_func: Callable[[str, str], object] | None = Levenshtein.distance,
    counts: dict[str, int],
    row_idx: int,
    col_idx: int,
) -> FixableDifference | None:
    mdd_value = mdd_row[mdd_column]
    hesp_column = mdd_column.replace("MDD", "Hesp")
    hesp_value = hesp_row.get(hesp_column, "")
    match (bool(hesp_value), bool(mdd_value)):
        case (True, False):
            counts[f"{mdd_column} missing in MDD"] += 1
            explanation: str | None = "H only"
        case (False, True):
            counts[f"{mdd_column} missing in Hesperomys"] += 1
            explanation = "M only"
        case (True, True):
            if hesp_value != mdd_value:
                comparison = f"{hesp_value} (H) / {mdd_value} (M)"
                if compare_func is not None:
                    extra = compare_func(hesp_value, mdd_value)
                    comparison = f"{extra}: {comparison}"
                    explanation = comparison
                else:
                    explanation = None
                counts[f"{mdd_column} differences"] += 1
            else:
                return None
        case (False, False):
            return None
    return FixableDifference(
        row_idx=row_idx,
        col_idx=col_idx,
        explanation=explanation,
        mdd_column=mdd_column,
        hesp_value=hesp_value,
        mdd_value=mdd_value,
        hesp_row=hesp_row,
        mdd_row=mdd_row,
        hesp_name=hesp_name,
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


def run_gspread_test() -> None:
    options = get_options()
    gc = gspread.oauth()
    sheet = gc.open(options.mdd_sheet)
    worksheet = sheet.get_worksheet_by_id(options.mdd_worksheet_gid)
    worksheet.update_cell(1, 1, "MDD_syn_ID_test")


def run(*, dry_run: bool = True, taxon: Taxon) -> None:
    options = get_options()
    backup_path = (
        options.data_path / "mdd_updater" / datetime.datetime.now().isoformat()
    )
    backup_path.mkdir(parents=True, exist_ok=True)

    print("downloading MDD names... ")
    gc = gspread.oauth()
    sheet = gc.open(options.mdd_sheet)
    worksheet = sheet.get_worksheet_by_id(options.mdd_worksheet_gid)
    rows = worksheet.get()
    headings = rows[0]
    column_to_idx = {heading: i for i, heading in enumerate(headings, start=1)}
    print(f"done, {len(rows)} found")

    print("backing up MDD names... ")
    with (backup_path / "mdd_names.csv").open("w") as file:
        writer = csv.writer(file)
        for row in rows:
            writer.writerow(row)
    print(f"done, backup at {backup_path}")

    hesp_names = export.get_names_for_export(
        taxon,
        ages={AgeClass.extant, AgeClass.recently_extinct},
        group=Group.species,
        min_rank_for_age_filtering=Rank.species,
    )
    need_initials = mdd_diff.get_need_initials_authors(hesp_names)
    hesp_id_to_name = {name.id: name for name in hesp_names}
    unused_hesp_ids = set(hesp_id_to_name.keys())
    counts: dict[str, int] = Counter()
    missing_in_hesp: list[tuple[str, dict[str, str]]] = []
    fixable_differences: list[FixableDifference] = []
    max_mdd_id = 0

    for row_idx, mdd_row_as_list in getinput.print_every_n(
        enumerate(rows[1:], start=2), label="MDD names"
    ):
        mdd_row = dict(zip(headings, mdd_row_as_list, strict=False))
        mdd_id = int(mdd_row["MDD_syn_ID"])
        max_mdd_id = max(max_mdd_id, mdd_id)
        hesp_id = resolve_hesp_id(mdd_row["Hesp_id"])
        if hesp_id is None:
            missing_in_hesp.append(("missing in H", mdd_row))
            continue
        if hesp_id not in unused_hesp_ids:
            if hesp_id in hesp_id_to_name:
                message = "already matched"
            else:
                message = "invalid Hesp id"
            missing_in_hesp.append((message, mdd_row))
            continue
        unused_hesp_ids.remove(hesp_id)
        name = hesp_id_to_name[hesp_id]
        hesp_row = get_hesp_row(name, need_initials)

        for column in headings:
            if column in OMITTED_COLUMNS:
                continue
            compare_func: Callable[[str, str], object] | None
            match column:
                case "MDD_year":
                    compare_func = compare_year
                case _:
                    compare_func = None
            if diff := compare_column(
                hesp_row,
                mdd_row,
                mdd_column=column,
                counts=counts,
                row_idx=row_idx,
                col_idx=column_to_idx[column],
                compare_func=compare_func,
                hesp_name=name,
            ):
                fixable_differences.append(diff)

        # Can happen if Hesp name got redirected
        if name.id != hesp_id:
            fixable_differences.append(
                FixableDifference(
                    row_idx=row_idx,
                    col_idx=column_to_idx["Hesp_id"],
                    explanation="Hesp id is different from name id",
                    mdd_column="Hesp_id",
                    hesp_value=str(name.id),
                    mdd_value=mdd_row["Hesp_id"],
                    hesp_row=hesp_row,
                    mdd_row=mdd_row,
                    hesp_name=name,
                )
            )

    missing_in_mdd = []
    for hesp_id in unused_hesp_ids:
        name = hesp_id_to_name[hesp_id]
        hesp_row = get_hesp_row(name, need_initials)
        new_mdd_row = {"MDD_syn_ID": str(max_mdd_id + 1), "Hesp_id": str(hesp_id)}
        max_mdd_id += 1
        for mdd_column in headings:
            hesp_column = mdd_column.replace("MDD", "Hesp")
            if hesp_column in hesp_row:
                new_mdd_row[mdd_column] = hesp_row[hesp_column]
        missing_in_mdd.append(new_mdd_row)

    with (backup_path / "summary.txt").open("w") as file:
        for f in (sys.stdout, file):
            print("Report:", file=f)
            print(f"Total MDD names: {len(rows) - 1}", file=f)
            print(f"Total Hesp names: {len(hesp_names)}", file=f)
            print(f"Missing in Hesp: {len(missing_in_hesp)}", file=f)
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

    if missing_in_hesp:
        with (backup_path / "missing-in-hesp.csv").open("w") as file:
            writer = csv.writer(file)
            missing_in_hesp_headings = ["match_status", *missing_in_hesp[0][1]]
            writer.writerow(missing_in_hesp_headings)
            for match_status, row in missing_in_hesp:
                writer.writerow(
                    [
                        match_status,
                        *[row.get(column, "") for column in missing_in_hesp_headings],
                    ]
                )

    fixable_differences = sorted(
        fixable_differences,
        key=lambda x: (x.mdd_column, x.kind, x.hesp_value, x.mdd_value),
    )
    if fixable_differences:
        with (backup_path / "fixable-differences.csv").open("w") as file:
            dict_writer = csv.DictWriter(
                file,
                fieldnames=[
                    "row_idx",
                    "col_idx",
                    "explanation",
                    "mdd_column",
                    "hesp_value",
                    "mdd_value",
                    "hesp_id",
                    "mdd_id",
                    "applied",
                ],
            )
            dict_writer.writeheader()
            for (mdd_column, kind), group_iter in itertools.groupby(
                fixable_differences, key=lambda x: (x.mdd_column, x.kind)
            ):
                group = list(group_iter)
                header = f"{kind.name} for {mdd_column} ({len(group)})"
                getinput.print_header(header)
                for diff in group:
                    diff.print()
                print(header)
                choice = getinput.choose_one_by_name(
                    ["mdd_edit", "ask_individually", "hesp_edit", "skip"],
                    allow_empty=False,
                    history_key="overall_choice",
                )
                updates_to_make = []
                for diff in group:
                    should_add_to_mdd = False
                    should_add_to_hesp = False
                    match choice:
                        case "mdd_edit":
                            should_add_to_mdd = True
                        case "hesp_edit":
                            should_add_to_hesp = True
                        case "ask_individually":
                            diff.hesp_name.display()
                            if diff.hesp_name.original_citation:
                                diff.hesp_name.original_citation.display()
                            diff.print()
                            if diff.mdd_value.startswith("http"):
                                subprocess.check_call(["open", diff.mdd_value])
                            individual_choice = getinput.choose_one_by_name(
                                ["mdd_edit", "hesp_edit", "skip"],
                                allow_empty=False,
                                history_key="individual_choice",
                                callbacks=diff.get_adt_callbacks(),
                            )
                            match individual_choice:
                                case "mdd_edit":
                                    should_add_to_mdd = True
                                case "hesp_edit":
                                    should_add_to_hesp = True
                    if should_add_to_mdd:
                        updates_to_make.append(
                            gspread.cell.Cell(
                                row=diff.row_idx,
                                col=diff.col_idx,
                                value=process_value_for_sheets(diff.hesp_value),
                            )
                        )
                    elif should_add_to_hesp:
                        diff.apply_to_hesp(dry_run=dry_run)
                    dict_writer.writerow(
                        {
                            "row_idx": diff.row_idx,
                            "col_idx": diff.col_idx,
                            "explanation": diff.explanation,
                            "mdd_column": diff.mdd_column,
                            "hesp_value": diff.hesp_value,
                            "mdd_value": diff.mdd_value,
                            "hesp_id": diff.hesp_row["id"],
                            "mdd_id": diff.mdd_row["MDD_syn_ID"],
                            "applied": str(int(should_add_to_hesp)),
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
    parser.add_argument("--gspread-test", action="store_true", default=False)
    args = parser.parse_args()
    if args.gspread_test:
        run_gspread_test()
        return
    root = Taxon.getter("valid_name")(args.taxon)
    if root is None:
        print("Invalid taxon", args.taxon)
        sys.exit(1)
    run(taxon=root, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
