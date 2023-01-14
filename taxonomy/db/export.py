"""

Exporting data.

"""
from .models import Taxon, Name, Article, Occurrence
from .models.name import TypeTag
from ..command_set import CommandSet
from .. import getinput
from .constants import AgeClass, Group, Rank

import csv
from typing import TypedDict, Protocol

CS = CommandSet("export", "Exporting data")


class NameData(TypedDict):
    # Name ID in Hesperomys
    id: str
    # Link to Hesperomys page
    link: str
    class_: str
    order: str
    family: str
    genus: str
    species: str
    subspecies: str
    # Name as used in the original description
    original_name: str
    # Last part of the name, e.g. species name for species
    root_name: str
    authors: str
    author_links: str
    year: str
    page_described: str
    original_citation: str
    original_citation_link: str
    verbatim_citation: str
    citation_group_link: str
    type_locality_region: str
    type_locality_detail: str
    type_specimen: str
    species_type_kind: str
    collection_link: str
    type_specimen_detail: str
    nomenclature_status: str


class DetailTag(Protocol):
    text: str
    source: Article


@CS.register
def export_names(
    filename: str,
    taxon: Taxon,
    age: AgeClass | None = AgeClass.extant,
    group: Group | None = Group.species,
) -> None:
    """Export data about names to a CSV file."""
    print("collecting names...")
    names = taxon.all_names(age=age)
    if group is not None:
        names = [name for name in names if name.group is group]
    print(f"done, {len(names)} found")

    with open(filename, "w") as f:
        writer = csv.DictWriter(f, list(NameData.__annotations__))
        writer.writeheader()
        for name in getinput.print_every_n(names, label="names"):
            writer.writerow(data_for_name(name))


def data_for_name(name: Name) -> NameData:
    taxon = name.taxon
    class_ = taxon.get_derived_field("class_")
    order = taxon.get_derived_field("order")
    family = taxon.get_derived_field("family")
    try:
        genus = taxon.parent_of_rank(Rank.genus)
    except ValueError:
        genus = None
    try:
        species = taxon.parent_of_rank(Rank.species)
    except ValueError:
        species = None
    author_links = ", ".join(pers.get_absolute_url() for pers in name.get_authors())
    citation = name.original_citation
    cg = name.citation_group
    coll = name.collection
    loc_detail = "; ".join(
        stringify_detail_tag(tag)
        for tag in name.get_tags(name.type_tags, TypeTag.LocationDetail)
    )
    specimen_detail = "; ".join(
        stringify_detail_tag(tag)
        for tag in name.get_tags(name.type_tags, TypeTag.SpecimenDetail)
    )
    return {
        "id": str(name.id),
        "link": name.get_absolute_url(),
        "class_": class_.valid_name if class_ else "",
        "order": order.valid_name if order else "",
        "family": family.valid_name if family else "",
        "genus": genus.valid_name if genus else "",
        "species": species.valid_name if species else "",
        "subspecies": taxon.valid_name if taxon.rank is Rank.subspecies else "",
        "original_name": name.original_name or "",
        "root_name": name.root_name,
        "authors": name.taxonomic_authority(),
        "author_links": author_links,
        "year": name.year or "",
        "page_described": name.page_described or "",
        "original_citation": citation.cite("paper") if citation else "",
        "original_citation_link": citation.get_absolute_url() if citation else "",
        "verbatim_citation": name.verbatim_citation or "",
        "citation_group_link": cg.get_absolute_url() if cg else "",
        "type_locality_region": name.type_locality.name if name.type_locality else "",
        "type_locality_detail": loc_detail,
        "type_specimen": name.type_specimen or "",
        "species_type_kind": name.species_type_kind.name
        if name.species_type_kind
        else "",
        "collection_link": coll.get_absolute_url() if coll else "",
        "type_specimen_detail": specimen_detail,
        "nomenclature_status": name.nomenclature_status.name,
    }


def stringify_detail_tag(tag: DetailTag) -> str:
    if not tag.source:
        return f'"{tag.text}"'
    authors, year = tag.source.taxonomicAuthority()
    url = tag.source.get_absolute_url()
    return f'"{tag.text}" ({authors}, {year}, {url})'


class OccurrenceData(TypedDict):
    taxon: str
    taxon_link: str
    region: str
    region_link: str
    source: str
    source_link: str
    status: str
    comment: str


@CS.register
def export_occurrences(filename: str) -> None:
    with open(filename, "w") as f:
        writer = csv.DictWriter(f, list(OccurrenceData.__annotations__))
        writer.writeheader()
        for occ in getinput.print_every_n(
            Occurrence.select_valid(), label="occurrences", n=100
        ):
            writer.writerow(data_for_occ(occ))


def data_for_occ(occ: Occurrence):
    return {
        "taxon": occ.taxon.valid_name,
        "taxon_link": occ.taxon.get_absolute_url(),
        "region": occ.location.name,
        "region_link": occ.location.get_absolute_url(),
        "source": occ.source.cite() if occ.source else "",
        "source_link": occ.source.get_absolute_url() if occ.source else "",
        "status": occ.status.name,
        "comment": occ.comment or "",
    }
