"""Script to check for and fix issues in the MDD species sheet."""

import argparse
import csv
import datetime
import functools
import itertools
import re
import time
from collections import defaultdict
from collections.abc import Generator, Iterable, Sequence
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, TypedDict, TypeVar, cast

import gspread

from taxonomy import getinput
from taxonomy.config import get_options
from taxonomy.db.constants import Rank, Status
from taxonomy.db.models import Name, Taxon
from taxonomy.db.models.tags import TaxonTag

Syn = dict[str, str]


RANKS = ["higherTaxon", "order", "family"]

POSSIBLE_VALIDITIES = {
    "class",
    "subclass",
    "infraclass",
    "magnorder",
    "superorder",
    "order",
    "suborder",
    "infraorder",
    "parvorder",
    "superfamily",
    "family",
    "subfamily",
    "tribe",
    "subtribe",
    "genus",
    "subgenus",
}


T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Iterable[list[T]]:
    it = iter(iterable)
    while chunk := list(itertools.islice(it, n)):
        yield chunk


class MDDHigherTaxonRow(TypedDict):
    sciName: str
    id: str
    rank: str
    higherTaxon: str
    order: str
    family: str
    authorityAuthor: str
    authorityYear: str
    originalCombination: str
    authorityCitation: str
    authorityLink: str
    typeTaxon: str
    taxonomyNotes: str
    taxonomyNotesCitation: str


@dataclass
class Issue:
    row_idx: int
    mdd_id: str
    sci_name: str
    mdd_column: str
    mdd_value: str
    description: str
    suggested_change: str | None = None
    extra_key: str | None = None

    def describe(self) -> str:
        text = f"{self.sci_name} ({self.mdd_id or 'no id'}): {self.mdd_column}: {self.mdd_value!r}: {self.description}"
        if self.suggested_change is not None:
            text += f" (suggested fix: {self.suggested_change!r})"
        return text

    def group_description(self) -> str:
        match bool(self.mdd_value), bool(self.suggested_change):
            case True, True:
                return f"{self.mdd_column}: textual differences"
            case True, False:
                return f"{self.mdd_column}: species sheet unexpectedly has data"
            case _:
                return f"{self.mdd_column}: add data to species sheet"


@dataclass
class MDDHigherTaxon:
    row_idx: int
    row: MDDHigherTaxonRow

    def make_issue(
        self, col_name: str, description: str, suggested_value: str | None = None
    ) -> Issue:
        return Issue(
            self.row_idx,
            self.row["id"],
            self.row["sciName"],
            col_name,
            str(self.row.get(col_name, "")),
            description,
            suggested_value,
        )

    def lint_standalone(self) -> Iterable[Issue]:
        yield from ()


def _next_of_included_rank(taxon: Taxon | None) -> str:
    if taxon is None:
        return "NA"
    if taxon.rank.name.strip("_") in POSSIBLE_VALIDITIES:
        return taxon.valid_name
    return _next_of_included_rank(taxon.parent)


@dataclass
class TaxonWithSyns:
    taxon: MDDHigherTaxon
    base_name: Syn

    def get_hesp_name(self) -> Name | None:
        combined_hesp_id = self.base_name.get("Hesp_id", "")
        if not combined_hesp_id:
            return None
        hesp_id = int(str(combined_hesp_id)[3:])
        return Name(hesp_id)

    def get_hesp_taxon(self) -> Taxon | None:
        hesp_name = self.get_hesp_name()
        if hesp_name is None:
            return None
        rank = self.base_name["MDD_validity"]
        if rank == "class":
            rank_obj = Rank.class_
        else:
            try:
                rank_obj = Rank[rank]
            except KeyError:
                return None
        taxa = list(
            Taxon.select_valid().filter(
                Taxon.rank == rank_obj, Taxon.base_name == hesp_name
            )
        )
        if len(taxa) == 1:
            return taxa[0]
        return None

    def get_expected_row(self) -> MDDHigherTaxonRow:
        if self.base_name["MDD_authority_page_link"]:
            link = self.base_name["MDD_authority_page_link"]
        elif self.base_name["MDD_authority_link"]:
            link = self.base_name["MDD_authority_link"]
        else:
            link = ""
        if self.base_name["MDD_authority_citation"]:
            citation = self.base_name["MDD_authority_citation"]
        elif self.base_name["MDD_unchecked_authority_citation"]:
            citation = self.base_name["MDD_unchecked_authority_citation"]
        else:
            citation = ""

        hesp_name = self.get_hesp_name()
        if hesp_name is not None and hesp_name.type is not None:
            type_taxon = hesp_name.type.description()
        else:
            type_taxon = "NA"

        taxon = self.get_hesp_taxon()
        if taxon is None:
            higher_taxon = order = family = "TODO"
        else:
            try:
                order_obj = taxon.parent_of_rank(Rank.order)
            except ValueError:
                order = "NA"
            else:
                order = order_obj.valid_name
            try:
                family_obj = taxon.parent_of_rank(Rank.family)
            except ValueError:
                family = "NA"
            else:
                family = family_obj.valid_name
            if taxon.rank is Rank.class_:
                higher_taxon = "NA"
            else:
                higher_taxon = _next_of_included_rank(taxon.parent)

        return {
            "sciName": _clean_mdd_taxon(self.base_name),
            "id": self.taxon.row["id"],
            "rank": self.base_name["MDD_validity"],
            "higherTaxon": higher_taxon,
            "order": order,
            "family": family,
            "authorityAuthor": self.base_name["MDD_author"],
            "authorityYear": self.base_name["MDD_year"],
            "originalCombination": self.base_name["MDD_original_combination"],
            "authorityCitation": citation,
            "authorityLink": link,
            "typeTaxon": type_taxon,
            "taxonomyNotes": self.taxon.row.get("taxonomyNotes", ""),
            "taxonomyNotesCitation": self.taxon.row.get("taxonomyNotesCitation", ""),
        }

    def compare_against_expected(self) -> Iterable[Issue]:
        for mdd_col, expected_val in self.get_expected_row().items():
            if mdd_col in ("taxonomyNotes", "taxonomyNotesCitation"):
                continue
            try:
                actual_val = self.taxon.row[mdd_col]  # type: ignore[literal-required]
            except KeyError:
                actual_val = ""
            if expected_val != actual_val:
                assert expected_val is None or isinstance(expected_val, str)
                description = f"Expected {expected_val!r}, found {actual_val!r}"
                yield self.taxon.make_issue(mdd_col, description, expected_val)


@functools.cache
def get_sheet() -> Any:
    options = get_options()
    gc = gspread.oauth()
    return gc.open(options.mdd_sheet)


def _clean_mdd_taxon(syn: Syn) -> str:
    return re.sub(r" \([A-Z][a-z]+\)$", "", syn["MDD_taxon"])


def generate_match(
    taxa: list[MDDHigherTaxon], syns: list[Syn]
) -> Generator[Issue, None, list[TaxonWithSyns]]:
    sci_name_to_validity_to_sins: dict[tuple[str, str], list[dict[str, str]]] = (
        defaultdict(list)
    )
    for syn in syns:
        if syn["MDD_validity"] in POSSIBLE_VALIDITIES:
            sci_name_to_validity_to_sins[
                (_clean_mdd_taxon(syn), syn["MDD_validity"])
            ].append(syn)
    remaining_sci_names = set(sci_name_to_validity_to_sins)
    output: list[TaxonWithSyns] = []
    for taxon in taxa:
        key = (taxon.row["sciName"], taxon.row["rank"])
        if key not in remaining_sci_names:
            yield taxon.make_issue("sciName", "cannot find species in synonyms sheet")
            continue
        remaining_sci_names.discard(key)
        syns = sci_name_to_validity_to_sins[key]
        if len(syns) != 1:
            description = (
                f"Found {len(syns)} matches in synonyms sheet for {key[0]} ({key[1]})"
            )
            yield taxon.make_issue("sciName", description)
            continue
        output.append(TaxonWithSyns(taxon, syns[0]))
    for sci_name, rank in remaining_sci_names:
        yield Issue(
            0,
            "",
            sci_name,
            "sciName",
            "",
            f"Name {sci_name} (rank {rank}) in synonyms sheet but not in species sheet",
        )
    return output


def check_with_syns_match(
    species: list[MDDHigherTaxon], syns: list[Syn]
) -> Iterable[Issue]:
    spp_with_syns = yield from generate_match(species, syns)
    for sp in spp_with_syns:
        yield from sp.compare_against_expected()


def check_id_field(species: list[MDDHigherTaxon]) -> Iterable[Issue]:
    species = sorted(species, key=lambda sp: sp.row["id"])
    for mdd_id, group_iter in itertools.groupby(species, lambda sp: sp.row["id"]):
        group = list(group_iter)
        if mdd_id != "" and len(group) != 1:
            description = f"multiple species with id {mdd_id}: {', '.join(sp.row['sciName'] for sp in group)}"
            for sp in group:
                yield sp.make_issue("id", description)
    id_less = [sp for sp in species if not sp.row["id"]]
    if id_less:
        max_id = max(
            int(sp.row["id"])
            for sp in species
            if sp.row["id"] and sp.row["id"].isnumeric()
        )
        for sp in id_less:
            max_id += 1
            yield sp.make_issue("id", "missing MDD id", str(max_id))


def lint_taxa(taxa: list[MDDHigherTaxon]) -> Iterable[Issue]:
    for taxon in taxa:
        yield from taxon.lint_standalone()
    yield from check_id_field(taxa)


def maybe_fix_issues(
    issues: list[Issue], column_to_idx: dict[str, int], *, dry_run: bool
) -> None:
    def _issue_sort_key(issue: Issue) -> tuple[int, bool, bool]:
        return (
            column_to_idx[issue.mdd_column],
            bool(issue.mdd_value),
            bool(issue.suggested_change),
        )

    issues = sorted(issues, key=_issue_sort_key)
    sheet = get_sheet()
    worksheet = sheet.get_worksheet_by_id(get_options().mdd_higher_taxa_worksheet_gid)

    for (_, _, fixable), group_iter in itertools.groupby(issues, _issue_sort_key):
        group = list(group_iter)
        sample = group[0]
        header = f"{sample.group_description()} ({len(group)})"
        if not fixable:
            header = f"[unfixable] {header}"
        getinput.print_header(header)
        for issue in group:
            print(issue.describe())
        print(header)
        if not fixable:
            getinput.yes_no("Acknowledge and continue: ", default=True)
            continue
        choice = getinput.choose_one_by_name(
            ["edit", "ask_individually", "skip"],
            allow_empty=False,
            history_key="overall_choice",
        )
        updates_to_make = []
        for diff in group:
            should_edit = False
            match choice:
                case "edit":
                    should_edit = True
                case "ask_individually":
                    print(issue.describe())
                    individual_choice = getinput.choose_one_by_name(
                        ["edit", "skip"],
                        allow_empty=False,
                        history_key="individual_choice",
                    )
                    match individual_choice:
                        case "edit":
                            should_edit = True
            if should_edit:
                updates_to_make.append(
                    gspread.cell.Cell(
                        row=diff.row_idx,
                        col=column_to_idx[diff.mdd_column],
                        value=process_value_for_sheets(diff.suggested_change),  # type: ignore[arg-type]
                    )
                )

        if dry_run:
            print("Make change:", updates_to_make)
        elif updates_to_make:
            done = 0
            print(
                f"Applying {len(updates_to_make)} changes for column {sample.mdd_column}"
            )
            for batch in batched(updates_to_make, 500):
                worksheet.update_cells(batch)
                done += len(batch)
                print(f"Done {done}/{len(updates_to_make)}")
                if len(batch) == 500:
                    time.sleep(30)


def process_value_for_sheets(value: str) -> str | int:
    if value.isdigit():
        return int(value)
    return value


def check_species_tags(species: Sequence[MDDHigherTaxon]) -> None:
    for sp in species:
        name = sp.row["sciName"]
        rank = sp.row["rank"]
        if rank == "class":
            rank_obj = Rank.class_
        else:
            rank_obj = Rank[rank]
        possible_names = [name, f"{name} ({name})"]
        taxa = [
            taxon
            for taxon in Taxon.select_valid().filter(
                Taxon.rank == rank_obj, Taxon.valid_name.is_in(possible_names)
            )
            if taxon.base_name.status is Status.valid
        ]
        if len(taxa) == 1:
            taxon = taxa[0]
            existing_tags = [tag for tag in taxon.tags if isinstance(tag, TaxonTag.MDD)]
            expected_tags = [TaxonTag.MDD(sp.row["id"])]
            if existing_tags != expected_tags:
                print(f"{name} ({sp.row['id']}): {existing_tags} -> {expected_tags}")
                taxon.tags = [
                    *[tag for tag in taxon.tags if not isinstance(tag, TaxonTag.MDD)],
                    *expected_tags,
                ]
        else:
            print(f"No single taxon found for {name}: {taxa}")


def write_grouped_differences(backup_path: Path, issues: list[Issue]) -> None:
    ranks = set(RANKS)
    grouped: dict[tuple[str, str, str | None, str | None], list[str]] = {}
    for issue in issues:
        if issue.mdd_column not in ranks:
            continue
        key = (
            issue.mdd_column,
            issue.mdd_value,
            issue.suggested_change,
            issue.extra_key,
        )
        grouped.setdefault(key, []).append(issue.sci_name)
    with (backup_path / "grouped_differences.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "column",
                "MDD_value",
                "Hesp_value",
                "species",
                "comment_Jelle",
                "comment_Connor",
            ]
        )
        for (column, value, suggested_change, _), species_list in grouped.items():
            writer.writerow(
                [column, value or "", suggested_change or "", ", ".join(species_list)]
            )


def run(
    *,
    dry_run: bool = True,
    input_csv: str | None = None,
    syn_sheet_csv: str | None = None,
) -> None:
    options = get_options()
    backup_path = (
        options.data_path
        / "mdd_higher_taxa"
        / datetime.datetime.now(datetime.UTC).isoformat()
    )
    backup_path.mkdir(parents=True, exist_ok=True)

    print("downloading MDD higher taxa... ")
    if input_csv is None:
        sheet = get_sheet()
        worksheet = sheet.get_worksheet_by_id(options.mdd_higher_taxa_worksheet_gid)
        raw_rows = worksheet.get()
    else:
        with Path(input_csv).open() as f:
            raw_rows = list(csv.reader(f))
    headings = raw_rows[0]
    column_to_idx = {heading: i for i, heading in enumerate(headings, start=1)}
    taxa = [
        MDDHigherTaxon(
            row_idx, cast(MDDHigherTaxonRow, dict(zip(headings, row, strict=False)))
        )
        for row_idx, row in enumerate(raw_rows[1:], start=2)
    ]
    print(f"done, {len(taxa)} found")

    print("backing up MDD names... ")
    with (backup_path / "mdd_higher_taxa.csv").open("w") as file:
        writer = csv.writer(file)
        for row in raw_rows:
            writer.writerow(row)
    print(f"done, backup at {backup_path}")

    issues = list(lint_taxa(taxa))

    if syn_sheet_csv is not None:
        with Path(syn_sheet_csv).open() as f:
            syn_sheet_rows = list(csv.reader(f))
    else:
        sheet = get_sheet()
        worksheet = sheet.get_worksheet_by_id(options.mdd_higher_worksheet_gid)
        syn_sheet_rows = worksheet.get()
    syn_sheet_headings = syn_sheet_rows[0]
    syns = [
        dict(zip(syn_sheet_headings, row, strict=False)) for row in syn_sheet_rows[1:]
    ]
    issues += check_with_syns_match(taxa, syns)
    check_species_tags(taxa)

    for issue in issues:
        print(issue.describe())

    with (backup_path / "differences.csv").open("w") as f:
        headings = [field.name for field in fields(Issue)]
        diff_writer = csv.DictWriter(f, headings)
        diff_writer.writeheader()
        for issue in issues:
            diff_writer.writerow(
                {heading: getattr(issue, heading) or "" for heading in headings}
            )
    write_grouped_differences(backup_path, issues)

    maybe_fix_issues(issues, column_to_idx, dry_run=dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=str, default=None)
    parser.add_argument("--syn-sheet-csv", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()
    run(
        input_csv=args.input_csv, dry_run=args.dry_run, syn_sheet_csv=args.syn_sheet_csv
    )
