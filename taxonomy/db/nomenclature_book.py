"""

Code for preparing the "Nomenclature of Mammals" book.

Todo:
- HMW treatments are missing authorities, higher taxa?
- Add MDD2, CH2 treatments
- Set up syncing with Google Sheet
- Set up system to know when comments are out of date
- Order things in a sensible way (Connor: "We could use the order level ordering we use on the MDD species sheet and have everything below alphabetized?")

"""

import csv
import pprint
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import google.auth.exceptions
import gspread

from taxonomy import getinput
from taxonomy.config import get_options
from taxonomy.db import helpers
from taxonomy.db.constants import (
    AgeClass,
    Group,
    NomenclatureStatus,
    Rank,
    RegionKind,
    SpeciesGroupType,
    Status,
    TypeSpeciesDesignation,
)
from taxonomy.db.models.article.article import Article
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import ClassificationEntry
from taxonomy.db.models.location import Location
from taxonomy.db.models.name.name import Name, NameTag, TypeTag
from taxonomy.db.models.taxon.lint import check_full_expected_base_name
from taxonomy.db.models.taxon.taxon import Taxon

COVERED_RANKS = {
    Rank.class_,
    Rank.subclass,
    Rank.infraclass,
    Rank.superorder,
    Rank.order,
    Rank.suborder,
    Rank.infraorder,
    Rank.superfamily,
    Rank.family,
    Rank.subfamily,
    Rank.tribe,
    Rank.subtribe,
    Rank.genus,
    Rank.species,
}
COVERED_AGES = {AgeClass.extant, AgeClass.recently_extinct}

PAST_TREATMENTS = [
    ("CH1", ["Mammalia (Corbet & Hill 1980).pdf"]),
    ("MSW1", ["Mammalia (Honacki et al. 1982).pdf"]),
    ("CH3", ["Mammalia (Corbet & Hill 1991).pdf"]),
    ("MSW2", ["Mammalia (Wilson & Reeder 1993).pdf"]),
    ("MSW3", ["Mammalia-review (MSW3)"]),
    (
        "HMW",
        [
            "Rodentia (HMW7)",
            "Placentalia-HMW 8",
            "Chiroptera (HMW)",
            "Marsupialia, Monotremata (HMW)",
            "Primates (HMW)",
            "Glires (HMW)",
            "Mammalia-marine (HMW)",
            "Ungulata (HMW)",
            "Carnivora (HMW)",
        ],
    ),
    ("MDD1", ["Mammalia-MDD 1.0.csv"]),
    # TODO MDD2
]
ORDERED_LABELS = {label: i for i, (label, _) in enumerate(PAST_TREATMENTS)}
ALL_LABELS = {label for label, _ in PAST_TREATMENTS}


def escape(text: str) -> str:
    """Escape text for markdown."""
    return text.replace("*", r"\*")


@dataclass
class ForInfo:
    """Fields to be shown in the spreadsheet purely for information."""

    hesp_taxon_link: str
    hesp_name_link: str
    order: str | None
    family: str | None
    todos: list[str]
    authority_link: str | None
    authority_page_link: str | None
    tags: list[object]

    def to_csv(self) -> dict[str, str]:
        return {
            "info_todos": "\n".join(self.todos),
            "info_hesp_taxon_link": self.hesp_taxon_link,
            "info_hesp_name_link": self.hesp_name_link,
            "info_order": self.order or "",
            "info_family": self.family or "",
            "info_authority_link": self.authority_link or "",
            "info_authority_page_link": self.authority_page_link or "",
            "info_tags": " ||| ".join(map(str, self.tags)),
        }


@dataclass
class ForBook:
    """Fields to be used in rendering the book text."""

    rank: Rank
    name: str
    original_combination: str | None
    authority: str
    year: str
    should_parenthesize: bool
    page: str | None
    nomenclature_text: str | None
    type_taxon: str | None
    type_locality: str | None
    type_specimen: str | None
    past_treatments: str | None

    def to_csv(self) -> dict[str, str]:
        return {
            "book_rank": self.rank.name,
            "book_name": self.name,
            "book_original_combination": self.original_combination or "",
            "book_authority": self.authority,
            "book_year": self.year,
            "book_should_parenthesize": str(int(self.should_parenthesize)),
            "book_page": self.page or "",
            "book_nomenclature_text": self.nomenclature_text or "",
            "book_type_taxon": self.type_taxon or "",
            "book_type_locality": self.type_locality or "",
            "book_type_specimen": self.type_specimen or "",
            "book_past_treatments": self.past_treatments or "",
        }


@dataclass
class ForEdit:
    """Fields that may be edited directly."""

    type_taxon: str | None
    type_locality: str | None
    verbatim_type_locality: str | None
    type_specimen: str | None
    common_name: str | None
    comments: str | None

    def to_csv(self) -> dict[str, str]:
        return {
            "edit_type_taxon": self.type_taxon or "",
            "edit_type_locality": self.type_locality or "",
            "edit_verbatim_type_locality": self.verbatim_type_locality or "",
            "edit_type_specimen": self.type_specimen or "",
            "edit_common_name": self.common_name or "",
            "edit_comments": self.comments or "",
        }


@dataclass
class Row:
    for_info: ForInfo
    for_book: ForBook
    for_edit: ForEdit

    def to_csv(self) -> dict[str, str]:
        return {
            **self.for_book.to_csv(),
            **self.for_edit.to_csv(),
            **self.for_info.to_csv(),
        }


def get_taxa(root_taxon: Taxon) -> Iterable[Taxon]:
    if root_taxon.age not in COVERED_AGES:
        return
    if root_taxon.base_name.status is not Status.valid:
        return
    if root_taxon.rank in COVERED_RANKS:
        yield root_taxon
    if root_taxon.rank is Rank.species:
        return
    for child in root_taxon.get_children():
        yield from get_taxa(child)


def get_names(taxa: Iterable[Taxon]) -> Iterable[Name]:
    for taxon in taxa:
        nam = taxon.base_name
        yield nam
        if nam.nomenclature_status is NomenclatureStatus.nomen_novum:
            original = nam.get_tag_target(NameTag.NomenNovumFor)
            assert original is not None, nam
            yield original
        elif nam.nomenclature_status is NomenclatureStatus.as_emended:
            original = nam.get_tag_target(NameTag.AsEmendedBy)
            assert original is not None, nam
            yield original


def display_name(name: Name) -> str:
    return f"_{name.corrected_original_name}_ {helpers.romanize_russian( name.taxonomic_authority())}, {name.numeric_year()}"


def get_type_locality_prefix(location: Location | None) -> str:
    if location is None:
        return ""
    region = location.region
    country = region.parent_of_kind(RegionKind.country)
    if country is None:
        country = region.parent_of_kind(RegionKind.continent)
        if country is None:
            return ""
    prefix = f"{region.name}: "
    while region != country and region.parent is not None:
        region = region.parent
        prefix = f"{region.name}: {prefix}"
    return prefix


type TaxonToCEs = dict[Taxon, set[tuple[str, ClassificationEntry]]]


def get_all_ces() -> TaxonToCEs:
    result: TaxonToCEs = {}
    for label, art_names in PAST_TREATMENTS:
        for art_name in art_names:
            art = Article.select_valid().filter(Article.name == art_name).get()
            for ce in art.get_classification_entries_with_children():
                if (
                    ce.mapped_name is None
                    or ce.rank.is_synonym
                    or ce.rank is Rank.subspecies
                ):
                    continue
                nam = ce.mapped_name.resolve_variant()
                for txn in Taxon.select_valid().filter(Taxon.base_name == nam):
                    result.setdefault(txn, set()).add((label, ce))
    return result


def sort_labels(labels: Iterable[str]) -> list[str]:
    """Sort labels according to the ORDERED_LABELS defined above."""
    return sorted(labels, key=lambda label: ORDERED_LABELS[label])


def get_past_treatments(taxon: Taxon, ces: set[tuple[str, ClassificationEntry]]) -> str:
    texts = []

    names = {ce.name for _, ce in ces} | {taxon.valid_name}
    should_italicize = taxon.rank <= Rank.genus
    if len(names) > 1:
        names_grouped: dict[str, list[str]] = {}
        for label, ce in ces:
            names_grouped.setdefault(ce.name, []).append(label)
        past_names = []
        for name, labels in sorted(names_grouped.items()):
            sorted_labels = sort_labels(labels)
            if should_italicize:
                name = f"_{name}_"
            past_names.append(f"{name} ({', '.join(sorted_labels)})")
        texts.append(f"Name: {'; '.join(past_names)}.")

    authors = {
        ce.authority.strip("()").replace(" and ", " & ").replace(",", "")
        for _, ce in ces
        if ce.authority
    } | {
        helpers.romanize_russian(taxon.base_name.taxonomic_authority()).replace(",", "")
    }
    if len(authors) > 1:
        authors_grouped: dict[str, list[str]] = {}
        for label, ce in ces:
            if ce.authority:
                authors_grouped.setdefault(ce.authority, []).append(label)
        past_authors = []
        for name, labels in sorted(authors_grouped.items()):
            sorted_labels = sort_labels(labels)
            past_authors.append(f"{name} ({', '.join(sorted_labels)})")
        texts.append(f"Author: {'; '.join(past_authors)}.")

    years = {ce.year for _, ce in ces if ce.year} | {
        str(taxon.base_name.numeric_year())
    }
    if len(years) > 1:
        years_grouped: dict[str, list[str]] = {}
        for label, ce in ces:
            if ce.year:
                years_grouped.setdefault(ce.year, []).append(label)
        past_years = []
        for name, labels in sorted(years_grouped.items()):
            sorted_labels = sort_labels(labels)
            past_years.append(f"{name} ({', '.join(sorted_labels)})")
        texts.append(f"Date: {'; '.join(past_years)}.")

    included_treatments = {treatment for treatment, _ in ces}
    missing = ALL_LABELS - included_treatments
    if missing:
        texts.append(f"Not listed in {', '.join(sort_labels(missing))}.")
    return " ".join(texts)


def get_row(taxon: Taxon, name: Name, taxon_to_ces: TaxonToCEs) -> Row:
    todos = []
    order = taxon.get_derived_field("order")
    family = taxon.get_derived_field("family")
    interpreted_tl = name.get_type_tag(TypeTag.InterpretedTypeLocality)
    interpreted_ts = name.get_type_tag(TypeTag.InterpretedTypeSpecimen)
    interpreted_tt = name.get_type_tag(TypeTag.InterpretedTypeTaxon)
    comments = name.get_type_tag(TypeTag.NomenclatureComments)
    if name.original_citation is None:
        todos.append("Original citation missing")
    different_authority = name.get_tag_target(TypeTag.DifferentAuthority)
    if different_authority:
        todos.append("Name authority differs from article; confirm this is correct")
    if name == taxon.base_name:
        if not name.can_be_valid_base_name():
            todos.append(
                f"Base name is not valid (status: {name.nomenclature_status.name})"
            )
        todos += check_full_expected_base_name.linter(taxon, LintConfig())

    nomenclature_text = ""
    if nomen_novum_for := name.get_tag_target(NameTag.NomenNovumFor):
        nomenclature_text += f"Nomen novum for {display_name(nomen_novum_for)}"
        if nomen_novum_for.has_name_tag(NameTag.PreoccupiedBy):
            nomenclature_text += ", preoccupied"
        nomenclature_text += ". "
    homonyms = list(name.get_tags(name.tags, NameTag.PreoccupiedBy))
    if homonyms:
        nomenclature_text += (
            "Not "
            + ", ".join(display_name(homonym.name) for homonym in homonyms)
            + ". "
        )
    if name.nomenclature_status is NomenclatureStatus.justified_emendation:
        nomenclature_text += "Justified emendation. "
    nomenclature_text = nomenclature_text.strip().rstrip(".")

    type_taxon = ""
    if interpreted_tt:
        type_taxon = interpreted_tt.text
    elif name.type:
        type_taxon = display_name(name.type)
        if name.genus_type_kind is not None:
            type_taxon += f", by {name.genus_type_kind.name.replace('_', ' ')}"
            if name.genus_type_kind not in (
                TypeSpeciesDesignation.absolute_tautonymy,
                TypeSpeciesDesignation.linnaean_tautonymy,
                TypeSpeciesDesignation.monotypy,
                TypeSpeciesDesignation.original_designation,
            ):
                todos.append(
                    f"Check type designation: {name.genus_type_kind.name.replace('_', ' ')}"
                )
        elif name.group is Group.genus:
            todos.append("Check type designation: genus without type designation")
    elif (
        name.group in (Group.family, Group.genus)
        and "type" in name.get_required_fields()
    ):
        todos.append("Missing type")

    tl_prefix = get_type_locality_prefix(name.type_locality)
    if tl_prefix:
        if interpreted_tl is not None:
            type_locality = tl_prefix + interpreted_tl.text
            verbatim_type_locality = "NA"
        else:
            original_tls = [
                tag
                for tag in name.type_tags
                if isinstance(tag, TypeTag.LocationDetail)
                and name.original_citation == tag.source
            ]
            if len(original_tls) > 1:
                todos.append(
                    f"Multiple original type localities found: {', '.join(repr(tag.text) for tag in original_tls)}"
                )
            if original_tls:
                verbatim_type_locality = original_tls[0].text
                type_locality = tl_prefix + f'"{original_tls[0].text}"'
            else:
                type_locality = tl_prefix + "TODO"
                todos.append("Verbatim type locality missing")
                verbatim_type_locality = ""
    else:
        type_locality = verbatim_type_locality = ""

    if interpreted_ts is not None:
        type_specimen = interpreted_ts.text
    elif name.type_specimen and name.species_type_kind is not None:
        type_specimen = f"{name.type_specimen} ({name.species_type_kind.name})"
        if name.species_type_kind not in (
            SpeciesGroupType.holotype,
            SpeciesGroupType.syntypes,
        ):
            todos.append(
                f"Check type specimen designation: {name.species_type_kind.name.replace('_', ' ')}"
            )
    else:
        if (
            name.group is Group.species
            and "type_specimen" in name.get_required_fields()
        ):
            todos.append("Missing type specimen")
        type_specimen = ""

    authority_page_link = None
    for tag in name.type_tags:
        if isinstance(tag, TypeTag.AuthorityPageLink):
            authority_page_link = tag.url
            break

    return Row(
        for_info=ForInfo(
            hesp_taxon_link=taxon.get_absolute_url(),
            hesp_name_link=name.get_absolute_url(),
            order=order.valid_name if order and order.rank is Rank.order else None,
            family=family.valid_name if family and family.rank is Rank.family else None,
            todos=todos,
            authority_link=(
                name.original_citation.url if name.original_citation else None
            ),
            authority_page_link=authority_page_link,
            tags=[*name.type_tags, *name.tags],
        ),
        for_book=ForBook(
            rank=taxon.rank,
            name=taxon.valid_name,
            original_combination=name.original_name,
            authority=helpers.romanize_russian(name.taxonomic_authority()),
            year=str(name.year),
            should_parenthesize=bool(name.should_parenthesize_authority()),
            page=(
                name.page_described.lstrip("@")
                if name.page_described is not None
                else None
            ),
            nomenclature_text=nomenclature_text,
            type_taxon=type_taxon,
            type_locality=type_locality,
            type_specimen=type_specimen,
            past_treatments=get_past_treatments(taxon, taxon_to_ces.get(taxon, set())),
        ),
        for_edit=ForEdit(
            type_locality=interpreted_tl.text if interpreted_tl else None,
            verbatim_type_locality=verbatim_type_locality,
            type_specimen=interpreted_ts.text if interpreted_ts else None,
            type_taxon=interpreted_tt.text if interpreted_tt else None,
            comments=comments.text if comments else None,
            common_name=None,  # TODO
        ),
    )


def get_rows() -> Iterable[Row]:
    root = Taxon.getter("valid_name")("Mammalia")
    assert root is not None
    taxa = get_taxa(root)
    taxon_to_ces = get_all_ces()
    rows = [
        get_row(taxon, name, taxon_to_ces)
        for taxon in taxa
        for name in get_names([taxon])
    ]
    return rows


def get_text_lines() -> Iterable[str]:
    root = Taxon.getter("valid_name")("Mammalia")
    assert root is not None
    taxon_to_ces = get_all_ces()
    for taxon in get_taxa(root):
        names = get_names([taxon])
        rows = [get_row(taxon, name, taxon_to_ces) for name in names]
        base_row = rows[0]
        if taxon.rank > Rank.genus:
            header = f"## {taxon.rank.name.title().strip('_')} {taxon.valid_name} "
        else:
            header = f"## _{taxon.valid_name}_ "
        if base_row.for_book.should_parenthesize:
            header += "("
        header += f"{base_row.for_book.authority}, {base_row.for_book.year[:4]}"
        if base_row.for_book.should_parenthesize:
            header += ")"
        if base_row.for_edit.common_name:
            header += f". {base_row.for_edit.common_name}."
        yield header
        for row in rows:
            if row.for_book.rank > Rank.genus:
                name = row.for_book.original_combination
            else:
                name = f"_{row.for_book.original_combination}_"
            name = f"{name} {row.for_book.authority}, {row.for_book.year}:{row.for_book.page}"
            pieces = [name]
            if row.for_book.nomenclature_text:
                pieces.append(escape(row.for_book.nomenclature_text))
            if row.for_book.type_taxon:
                pieces.append(f"T: {escape(row.for_book.type_taxon)}")
            if row.for_book.type_locality:
                pieces.append(f"TL: {escape(row.for_book.type_locality)}")
            if row.for_book.type_specimen:
                pieces.append(f"TS: {escape(row.for_book.type_specimen)}")
            yield ""
            yield ". ".join(pieces) + "."
        if base_row.for_book.past_treatments:
            yield ""
            yield f"*Past treatments*: {base_row.for_book.past_treatments}"
        if base_row.for_edit.comments:
            yield ""
            yield f"*Comments*: {base_row.for_book.past_treatments}"
        yield ""


def write_csv(path: Path) -> None:
    rows = list(get_rows())
    csv_rows = [row.to_csv() for row in rows]
    columns = list(csv_rows[0])
    with path.open("w", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)


@dataclass
class SheetRow:
    row_idx: int
    column_to_idx: dict[str, int]
    data: dict[str, str]


def process_value_for_sheets(value: str) -> str | int:
    if value.isdigit():
        return int(value)
    return value


def sync_sheet() -> None:

    print("Downloading sheet...")
    options = get_options()
    try:
        gc = gspread.oauth()
        sheet = gc.open(options.book_sheet)
    except google.auth.exceptions.RefreshError:
        print("need to refresh token")
        token_path = Path("~/.config/gspread/authorized_user.json").expanduser()
        token_path.unlink(missing_ok=True)
        gc = gspread.oauth()
        sheet = gc.open(options.book_sheet)

    worksheet = sheet.get_worksheet_by_id(options.book_sheet_gid)
    raw_rows = worksheet.get()
    headings = raw_rows[0]
    column_to_idx = {heading: i for i, heading in enumerate(headings, start=1)}
    sheet_rows = [
        SheetRow(i, column_to_idx, dict(zip(headings, row, strict=False)))
        for i, row in enumerate(raw_rows[1:], start=2)
    ]
    sheet_row_dict = {row.data["info_hesp_name_link"]: row for row in sheet_rows}
    print("Done")

    print("Generating CSV...")
    rows = list(get_rows())
    row_dict = {row.for_info.hesp_name_link: row for row in rows}
    print("Done")

    column_to_differences: dict[str, list[tuple[int, Row, str, str]]] = {}
    for sheet_row in sheet_rows:
        computed_row = row_dict.get(sheet_row.data["info_hesp_name_link"])
        if not computed_row:
            continue
        csv_row = computed_row.to_csv()
        for column, value in sheet_row.data.items():
            if column not in csv_row:
                continue
            if value != csv_row[column]:
                column_to_differences.setdefault(column, []).append(
                    (sheet_row.row_idx, computed_row, value, csv_row[column])
                )
    for column, differences in column_to_differences.items():
        getinput.print_header(f"Column: {column} ({len(differences)} differences)")
        differences = sorted(differences, key=lambda x: (x[2], x[3]))
        updates_to_make = []
        for i, (row_idx, row, sheet_value, computed_value) in enumerate(differences):
            if i < 5:
                print(
                    f"{row.for_book.name} ({row.for_book.rank.name}): {sheet_value} -> {computed_value}"
                )
            updates_to_make.append(
                gspread.cell.Cell(
                    row=row_idx,
                    col=column_to_idx[column],
                    value=process_value_for_sheets(  # static analysis: ignore[incompatible_argument]
                        computed_value
                    ),
                )
            )
        if getinput.yes_no("Update these rows?"):
            print(f"Apply {len(updates_to_make)} updates...")
            worksheet.update_cells(updates_to_make)
            print("Done")

    rows_to_add = [
        row for row in rows if row.for_info.hesp_name_link not in sheet_row_dict
    ]
    if rows_to_add:
        getinput.print_header(f"Adding {len(rows_to_add)} rows")
        for row in rows_to_add[:5]:
            pprint.pp(row.to_csv())
        if len(rows_to_add) > 5:
            print(f"and {len(rows_to_add) - 5} more")
        if getinput.yes_no("Add these rows?"):
            worksheet.append_rows(
                [
                    [process_value_for_sheets(value) for value in row.to_csv().values()]
                    for row in rows_to_add
                ]
            )

    rows_to_remove = [
        row for row in sheet_rows if row.data["info_hesp_name_link"] not in row_dict
    ]
    if rows_to_remove:
        getinput.print_header(f"Removing {len(rows_to_remove)} rows")
        for sheet_row in rows_to_remove[:5]:
            pprint.pp(sheet_row.data)
            if getinput.yes_no("Remove this row?"):
                worksheet.delete_rows(sheet_row.row_idx)


if __name__ == "__main__":
    if len(sys.argv) == 0:
        command = "md"
    else:
        command = sys.argv[1]
    match command:
        case "md":
            text = "\n".join(get_text_lines())
            Path("book.md").write_text(text)
        case "csv":
            write_csv(Path("book.csv"))
        case "sync":
            sync_sheet()
        case _:
            raise ValueError(f"Unknown command: {command}")
