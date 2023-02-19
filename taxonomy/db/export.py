"""

Exporting data.

"""
import csv
from collections.abc import Container
from typing import Protocol, TypedDict

from .. import getinput
from ..command_set import CommandSet
from .constants import AgeClass, Group, Rank, RegionKind
from .models import Article, Collection, Name, Occurrence, Taxon
from .models.name import TypeTag

CS = CommandSet("export", "Exporting data")


class NameData(TypedDict):
    # Name ID in Hesperomys
    id: str
    # Link to Hesperomys page
    link: str
    status: str
    age_class: str
    class_: str
    order: str
    family: str
    genus: str
    species: str
    subspecies: str
    taxon_name: str
    taxon_link: str
    # Name as used in the original description
    original_name: str
    # Same, but with diacritics/capitalization/etc. cleaned up
    corrected_original_name: str
    # Last part of the name, e.g. species name for species
    root_name: str
    original_rank: str
    group: str
    authority: str
    author_links: str
    year: str
    page_described: str
    original_citation: str
    original_citation_link: str
    verbatim_citation: str
    citation_group: str
    citation_group_link: str
    type_locality: str
    type_locality_detail: str
    type_specimen: str
    species_type_kind: str
    collection_link: str
    type_specimen_detail: str
    type_name: str
    type_link: str
    genus_type_kind: str
    nomenclature_status: str
    name_complex: str
    name_complex_link: str
    species_name_complex: str
    species_name_complex_link: str
    tags: str


class DetailTag(Protocol):
    text: str
    source: Article


@CS.register
def export_names(
    filename: str,
    taxon: Taxon | None = None,
    ages: Container[AgeClass] | None = {AgeClass.extant, AgeClass.recently_extinct},
    group: Group | None = Group.species,
    limit: int | None = None,
) -> None:
    """Export data about names to a CSV file."""
    print("collecting names...")
    if taxon is None:
        names = list(Name.select_valid().limit(limit))
    else:
        names = list(taxon.all_names())
        if limit is not None:
            names = names[:limit]
    print(f"done, {len(names)} found")
    print("filtering names...")
    if group is not None:
        names = [name for name in names if name.group is group]
    if ages is not None:
        names = [name for name in names if name.taxon.age in ages]
    print(f"done, {len(names)} remaining")

    with open(filename, "w") as f:
        writer: "csv.DictWriter[str]" = csv.DictWriter(
            f, list(NameData.__annotations__), escapechar="\\"
        )
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
    tags: list[object] = []
    if name.tags:
        tags += name.tags
    if name.type_tags:
        tags += name.type_tags
    return {
        "id": str(name.id),
        "link": name.get_absolute_url(),
        "status": name.status.name,
        "age_class": taxon.age.name,
        "class_": class_.valid_name if class_ else "",
        "order": order.valid_name if order else "",
        "family": family.valid_name if family else "",
        "genus": genus.valid_name if genus else "",
        "species": species.valid_name if species else "",
        "subspecies": taxon.valid_name if taxon.rank is Rank.subspecies else "",
        "taxon_name": taxon.valid_name,
        "taxon_link": taxon.get_absolute_url(),
        "original_name": name.original_name or "",
        "corrected_original_name": name.corrected_original_name or "",
        "root_name": name.root_name,
        "original_rank": name.original_rank.name if name.original_rank else "",
        "group": name.group.name,
        "authority": name.taxonomic_authority(),
        "author_links": author_links,
        "year": name.year or "",
        "page_described": name.page_described or "",
        "original_citation": citation.cite("paper") if citation else "",
        "original_citation_link": citation.get_absolute_url() if citation else "",
        "verbatim_citation": name.verbatim_citation or "",
        "citation_group": cg.name if cg else "",
        "citation_group_link": cg.get_absolute_url() if cg else "",
        "type_locality": name.type_locality.name if name.type_locality else "",
        "type_locality_detail": loc_detail,
        "type_specimen": name.type_specimen or "",
        "species_type_kind": (
            name.species_type_kind.name if name.species_type_kind else ""
        ),
        "collection_link": coll.get_absolute_url() if coll else "",
        "type_specimen_detail": specimen_detail,
        "type_name": str(name.type) if name.type else "",
        "type_link": name.type.get_absolute_url() if name.type else "",
        "genus_type_kind": name.genus_type_kind.name if name.genus_type_kind else "",
        "nomenclature_status": name.nomenclature_status.name,
        "name_complex": str(name.name_complex) if name.name_complex else "",
        "name_complex_link": (
            name.name_complex.get_absolute_url() if name.name_complex else ""
        ),
        "species_name_complex": (
            str(name.species_name_complex) if name.species_name_complex else ""
        ),
        "species_name_complex_link": (
            name.species_name_complex.get_absolute_url()
            if name.species_name_complex
            else ""
        ),
        "tags": repr(tags),
    }


def stringify_detail_tag(tag: DetailTag) -> str:
    if not tag.source:
        return f'"{tag.text}"'
    authors, year = tag.source.taxonomicAuthority()
    url = tag.source.get_absolute_url()
    return f'"{tag.text}" ({authors}, {year}, {url})'


class TaxonData(TypedDict):
    # Name ID in Hesperomys
    id: str
    # Link to Hesperomys page
    link: str
    status: str
    age_class: str
    class_: str
    order: str
    family: str
    genus: str
    rank: str
    taxon_name: str
    base_name_link: str
    authority: str
    year: str


def data_for_taxon(taxon: Taxon) -> TaxonData:
    name = taxon.base_name
    class_ = taxon.get_derived_field("class_")
    order = taxon.get_derived_field("order")
    family = taxon.get_derived_field("family")
    try:
        genus = taxon.parent_of_rank(Rank.genus)
    except ValueError:
        genus = None
    return {
        "id": str(taxon.id),
        "link": taxon.get_absolute_url(),
        "status": name.status.name,
        "age_class": taxon.age.name,
        "class_": class_.valid_name if class_ else "",
        "order": order.valid_name if order else "",
        "family": family.valid_name if family else "",
        "genus": genus.valid_name if genus else "",
        "rank": taxon.rank.name,
        "taxon_name": taxon.valid_name,
        "base_name_link": name.get_absolute_url(),
        "authority": name.taxonomic_authority(),
        "year": name.year or "",
    }


@CS.register
def export_taxa(filename: str, *, limit: int | None = None) -> None:
    """Export data about taxa to a CSV file."""
    taxa = Taxon.select_valid().limit(limit)

    with open(filename, "w") as f:
        writer: "csv.DictWriter[str]" = csv.DictWriter(
            f, list(TaxonData.__annotations__), escapechar="\\"
        )
        writer.writeheader()
        for taxon in getinput.print_every_n(taxa, label="taxa"):
            writer.writerow(data_for_taxon(taxon))


class CollectionData(TypedDict):
    id: str
    link: str
    label: str
    name: str
    comment: str
    city: str
    location_link: str
    location: str
    state: str
    country: str
    num_type_specimens: str


def data_for_collection(collection: Collection) -> CollectionData:
    loc = collection.location
    state = loc.parent_of_kind(RegionKind.state)
    country = loc.parent_of_kind(RegionKind.country)
    return {
        "id": str(collection.id),
        "link": collection.get_absolute_url(),
        "label": collection.label,
        "name": collection.name,
        "comment": collection.comment or "",
        "city": collection.city or "",
        "location_link": loc.get_absolute_url(),
        "location": loc.name,
        "state": state.name if state else "",
        "country": country.name if country else "",
        "num_type_specimens": collection.type_specimens.count(),
    }


@CS.register
def export_collections(filename: str) -> None:
    with open(filename, "w") as f:
        writer: "csv.DictWriter[str]" = csv.DictWriter(
            f, list(CollectionData.__annotations__)
        )
        writer.writeheader()
        for occ in getinput.print_every_n(
            Collection.select_valid(), label="collections", n=100
        ):
            writer.writerow(data_for_collection(occ))


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
        writer: "csv.DictWriter[str]" = csv.DictWriter(
            f, list(OccurrenceData.__annotations__)
        )
        writer.writeheader()
        for occ in getinput.print_every_n(
            Occurrence.select_valid(), label="occurrences", n=100
        ):
            writer.writerow(data_for_occ(occ))


def data_for_occ(occ: Occurrence) -> OccurrenceData:
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
