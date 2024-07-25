"""Exporting data."""

import csv
from collections import Counter
from collections.abc import Container
from pathlib import Path
from typing import Protocol, TypedDict

from taxonomy import getinput
from taxonomy.command_set import CommandSet
from taxonomy.db.models.classification_entry.ce import ClassificationEntry

from .constants import AgeClass, Group, Rank, RegionKind, Status
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
    publication_date: str
    page_described: str
    page_links: str
    original_citation: str
    original_citation_link: str
    verbatim_citation: str
    citation_group: str
    citation_group_link: str
    type_locality: str
    type_locality_country: str
    type_locality_detail: str
    type_specimen: str
    type_specimen_links: str
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


def get_names_for_export(
    taxon: Taxon | None = None,
    ages: Container[AgeClass] | None = None,
    group: Group | None = None,
    limit: int | None = None,
    min_rank_for_age_filtering: Rank | None = None,
) -> list[Name]:
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
        new_names = []
        for name in names:
            if name.taxon.age in ages:
                new_names.append(name)
                continue
            if min_rank_for_age_filtering is None:
                continue
            try:
                parent = name.taxon.parent_of_rank(min_rank_for_age_filtering)
            except ValueError:
                continue
            if parent.age in ages:
                new_names.append(name)
        names = new_names
    print(f"done, {len(names)} remaining")
    return names


@CS.register
def export_names(
    filename: str,
    taxon: Taxon | None = None,
    ages: Container[AgeClass] | None = None,
    group: Group | None = None,
    limit: int | None = None,
    min_rank_for_age_filtering: Rank | None = None,
) -> None:
    """Export data about names to a CSV file."""
    names = get_names_for_export(taxon, ages, group, limit, min_rank_for_age_filtering)

    with Path(filename).open("w") as f:
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
    cg = name.get_citation_group()
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
    tl_country = ""
    if name.type_locality is not None:
        tl_country_object = name.type_locality.region.parent_of_kind(RegionKind.country)
        if tl_country_object is not None:
            tl_country = tl_country_object.name
    page_links = " | ".join(
        tag.url for tag in name.type_tags if isinstance(tag, TypeTag.AuthorityPageLink)
    )
    type_specimen_links = " | ".join(
        tag.url
        for tag in name.type_tags
        if isinstance(tag, TypeTag.TypeSpecimenLinkFor)
    )
    if name.status is Status.valid:
        status = f"valid {name.taxon.rank.name}"
    else:
        status = name.status.name
    return {
        "id": str(name.id),
        "link": name.get_absolute_url(),
        "status": status,
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
        "original_rank": (
            name.original_rank.name if name.original_rank is not None else ""
        ),
        "group": name.group.name,
        "authority": name.taxonomic_authority(),
        "author_links": author_links,
        "year": name.year[:4] if name.year else "",
        "publication_date": name.year or "",
        "page_described": name.page_described or "",
        "page_links": page_links,
        "original_citation": citation.cite("paper") if citation else "",
        "original_citation_link": citation.get_absolute_url() if citation else "",
        "verbatim_citation": name.verbatim_citation or "",
        "citation_group": cg.name if cg else "",
        "citation_group_link": cg.get_absolute_url() if cg else "",
        "type_locality": name.type_locality.name if name.type_locality else "",
        "type_locality_country": tl_country,
        "type_locality_detail": loc_detail,
        "type_specimen": name.type_specimen or "",
        "type_specimen_links": type_specimen_links,
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
    if tag.source is None:
        return f'"{tag.text}"'
    authors, year = tag.source.taxonomic_authority()
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

    with Path(filename).open("w") as f:
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
        "num_type_specimens": str(collection.type_specimens.count()),
    }


@CS.register
def export_collections(filename: str) -> None:
    with Path(filename).open("w") as f:
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
    with Path(filename).open("w") as f:
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


@CS.register
def export_type_catalog(filename: str, *collections: Collection) -> None:
    with Path(filename).open("w") as f:
        writer: "csv.DictWriter[str]" = csv.DictWriter(
            f, ["label", *NameData.__annotations__]
        )
        writer.writeheader()
        for coll in collections:
            nams: list[tuple[Name, str]] = []
            nams += [(nam, "primary") for nam in coll.type_specimens]
            nams += [
                (nam, "probable")
                for nam in coll.get_derived_field("probable_specimens") or ()
            ]
            nams += [
                (nam, "shared")
                for nam in coll.get_derived_field("shared_specimens") or ()
            ]
            nams += [
                (nam, "possible")
                for nam in coll.get_derived_field("guessed_specimens") or ()
            ]
            nams += [
                (nam, "future")
                for nam in coll.get_derived_field("future_specimens") or ()
            ]
            nams += [
                (nam, "former")
                for nam in coll.get_derived_field("former_specimens") or ()
            ]
            nams += [
                (nam, "extra")
                for nam in coll.get_derived_field("extra_specimens") or ()
            ]
            for nam, label in nams:
                data = data_for_name(nam)
                writer.writerow({"label": label, **data})


@CS.register
def export_ces(
    filename: str,
    taxon: Taxon,
    rank: Rank = Rank.species,
    ages: Container[AgeClass] = (AgeClass.extant, AgeClass.recently_extinct),
) -> None:
    taxa = [child for child in taxon.children_of_rank(rank) if child.age in ages]
    name_to_taxon = {}
    ce_articles: Counter[Article] = Counter()
    taxon_to_article_to_valid_ces: dict[
        Taxon, dict[Article, list[ClassificationEntry]]
    ] = {}
    taxon_to_article_to_synonym_ces: dict[
        Taxon, dict[Article, list[ClassificationEntry]]
    ] = {}
    for child_taxon in taxa:
        for nam in child_taxon.all_names():
            name_to_taxon[nam] = child_taxon
            for ce in nam.classification_entries:
                ce_articles[ce.article] += 1
                if ce.rank is rank:
                    taxon_to_article_to_valid_ces.setdefault(
                        child_taxon, {}
                    ).setdefault(ce.article, []).append(ce)
                else:
                    taxon_to_article_to_synonym_ces.setdefault(
                        child_taxon, {}
                    ).setdefault(ce.article, []).append(ce)
    columns = [
        "class",
        "order",
        "family",
        "genus",
        "status",
        "species",
        "authority",
        "date",
        "synonyms",
    ]
    for article, _ in ce_articles.most_common():
        citation = article.concise_citation()
        columns.append(f"{citation} {rank.name}")
        columns.append(f"{citation} synonyms")
    with Path(filename).open("w") as f:
        writer = csv.DictWriter(f, columns)
        writer.writeheader()
        for child_taxon in taxa:
            try:
                genus = child_taxon.parent_of_rank(Rank.genus).valid_name
            except ValueError:
                genus = ""
            class_ = child_taxon.get_derived_field("class_")
            order = child_taxon.get_derived_field("order")
            family = child_taxon.get_derived_field("family")
            row = {
                "class": class_.valid_name if class_ is not None else "",
                "order": order.valid_name if order is not None else "",
                "family": family.valid_name if family is not None else "",
                "genus": genus,
                "status": child_taxon.base_name.status.name,
                "species": child_taxon.valid_name,
                "authority": child_taxon.base_name.taxonomic_authority(),
                "date": child_taxon.base_name.year,
                "synonyms": ", ".join(
                    sorted(
                        {
                            nam.corrected_original_name
                            for nam in child_taxon.all_names()
                            if nam.corrected_original_name is not None
                        }
                    )
                ),
            }
            for article, _ in ce_articles.most_common():
                valid_ces = taxon_to_article_to_valid_ces.get(child_taxon, {}).get(
                    article, []
                )
                synonym_ces = taxon_to_article_to_synonym_ces.get(child_taxon, {}).get(
                    article, []
                )
                citation = article.concise_citation()
                row[f"{citation} {rank.name}"] = ", ".join(ce.name for ce in valid_ces)
                row[f"{citation} synonyms"] = ", ".join(ce.name for ce in synonym_ces)
            writer.writerow(row)
