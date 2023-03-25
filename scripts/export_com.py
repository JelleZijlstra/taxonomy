from __future__ import annotations

import argparse
import csv
from collections.abc import Iterable

from typing_extensions import TypedDict

from taxonomy import getinput
from taxonomy.db.constants import Rank
from taxonomy.db.models import Name, Taxon
from taxonomy.db.models.range_summary import RangeSummary
from taxonomy.db.models.tags import TaxonTag


class Row(TypedDict):
    sequence_number: str
    rank: str
    taxon_name: str
    taxon_id: str
    original_name: str
    name_id: str
    root_name: str
    age: str
    status: str
    nomenclature_status: str
    authority: str
    year: str
    page_described: str
    original_citation: str
    original_citation_id: str
    verbatim_citation: str
    citation_group_id: str
    type: str
    type_id: str
    parent: str
    parent_id: str
    distribution: str
    distribution_detail: str
    common_name: str
    species_list: str
    comments: str
    key_references: str
    key_references_ids: str
    connor_comments: str
    jelle_comments: str
    changes_to_make: str


def make_row(taxon: Taxon, nam: Name, sequence_number: int) -> Row:
    is_base = taxon.base_name == nam
    if is_base:
        rs = RangeSummary.from_taxon(taxon)
        distribution, support = rs.summarize()
        name_tags = taxon.get_tags(taxon.tags, TaxonTag.EnglishCommonName)
        common_name = ", ".join(tag.name for tag in name_tags)
        ref_tags = list(taxon.get_tags(taxon.tags, TaxonTag.KeyReference))
        key_references = ", ".join(
            tag.article.concise_markdown_link() for tag in ref_tags
        )
        key_references_ids = ", ".join(tag.article.get_url() for tag in ref_tags)
    else:
        distribution = support = common_name = key_references = key_references_ids = ""
    if is_base and taxon.rank is Rank.genus:
        species = taxon.children_of_rank(Rank.species)
        species_list = ", ".join(
            f"{sp.age.get_symbol()}{sp.base_name.root_name}" for sp in species
        )
    else:
        species_list = ""
    return {
        "sequence_number": str(sequence_number),
        "rank": taxon.rank.name if is_base else "synonym",
        "taxon_name": taxon.valid_name,
        "taxon_id": taxon.get_url(),
        "original_name": nam.original_name or "",
        "name_id": nam.get_url(),
        "root_name": nam.root_name,
        "age": nam.taxon.age.name,
        "status": nam.status.name,
        "nomenclature_status": nam.nomenclature_status.name,
        "authority": nam.taxonomic_authority(),
        "year": nam.year or "",
        "page_described": nam.page_described or "",
        "original_citation": (
            nam.original_citation.cite() if nam.original_citation else ""
        ),
        "original_citation_id": (
            nam.original_citation.get_url() if nam.original_citation else ""
        ),
        "verbatim_citation": nam.verbatim_citation or "",
        "citation_group_id": nam.citation_group.get_url() if nam.citation_group else "",
        "type": str(nam.type) if nam.type else "",
        "type_id": nam.type.get_url() if nam.type else "",
        "parent": taxon.parent.valid_name if is_base and taxon.parent else "",
        "parent_id": taxon.parent.get_url() if is_base and taxon.parent else "",
        "distribution": distribution,
        "distribution_detail": support,
        "common_name": common_name,
        "species_list": species_list,
        "comments": (taxon.comments or "") if is_base else "",
        "key_references": key_references,
        "key_references_ids": key_references_ids,
        "connor_comments": "",
        "jelle_comments": "",
        "changes_to_make": "",
    }


def get_ordered_names(taxon: Taxon) -> Iterable[tuple[Taxon, Name]]:
    if taxon.rank < Rank.genus:
        return
    base = taxon.base_name
    yield taxon, base
    for nam in taxon.sorted_names():
        if nam != base:
            yield taxon, nam
    for child in taxon.sorted_children():
        yield from get_ordered_names(child)


def generate_rows(taxon: Taxon) -> Iterable[Row]:
    for i, (child_taxon, nam) in enumerate(get_ordered_names(taxon), start=1):
        yield make_row(child_taxon, nam, i)


def generate_report(taxon_name: str, output_file: str) -> None:
    taxon = Taxon.getter("valid_name")(taxon_name)
    assert taxon is not None, f"cannot find {taxon_name}"

    with open(output_file, "w") as f:
        writer = csv.DictWriter(f, list(Row.__annotations__))
        writer.writeheader()
        for row in getinput.print_every_n(generate_rows(taxon), label="rows"):
            writer.writerow(row)  # static analysis: ignore[incompatible_argument]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("taxon", help="Taxon to report on")
    parser.add_argument("output_file", help=".csv file to generate")
    args = parser.parse_args()
    generate_report(args.taxon, args.output_file)
