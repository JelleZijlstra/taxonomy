"""Exporting data."""

import csv
from collections import Counter, defaultdict
from collections.abc import Container, Iterable
from pathlib import Path
from typing import Protocol, TypedDict

from taxonomy import getinput
from taxonomy.command_set import CommandSet
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryTag,
)
from taxonomy.db.models.tags import TaxonTag

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
        names = [
            name
            for name in names
            if is_of_right_age(name, ages, min_rank_for_age_filtering)
        ]
    print(f"done, {len(names)} remaining")
    return names


def is_of_right_age(
    name: Name,
    ages: Container[AgeClass],
    min_rank_for_age_filtering: Rank | None = None,
) -> bool:
    if name.taxon.age in ages:
        return True
    if min_rank_for_age_filtering is not None:
        try:
            parent = name.taxon.parent_of_rank(min_rank_for_age_filtering)
        except ValueError:
            pass
        else:
            if parent.age in ages:
                return True
    if name.group is Group.family or name.taxon.rank is Rank.subgenus:
        all_taxa = Taxon.select_valid().filter(Taxon.base_name == name)
        if any(taxon.age in ages for taxon in all_taxa):
            return True
    return False


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
        status = f"valid {name.taxon.rank.display_name}"
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
            name.original_rank.display_name if name.original_rank is not None else ""
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
        "rank": taxon.rank.display_name,
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


class CEData(TypedDict):
    id: str
    link: str
    name: str
    rank: str
    source: str
    source_link: str
    parent: str
    parent_link: str
    page: str
    mapped_name_link: str
    authority: str
    year: str
    citation: str
    type_locality: str
    page_link: str


def data_for_ce(ce: ClassificationEntry) -> CEData:
    return {
        "id": str(ce.id),
        "link": ce.get_absolute_url(),
        "name": ce.name,
        "rank": ce.get_rank_string(),
        "source": ce.article.cite(),
        "source_link": ce.article.get_absolute_url(),
        "parent": ce.parent.name if ce.parent else "",
        "parent_link": ce.parent.get_absolute_url() if ce.parent else "",
        "page": ce.page or "",
        "mapped_name_link": ce.mapped_name.get_absolute_url() if ce.mapped_name else "",
        "authority": ce.authority or "",
        "year": ce.year or "",
        "citation": ce.citation or "",
        "type_locality": ce.type_locality or "",
        "page_link": " | ".join(
            tag.url
            for tag in ce.tags
            if isinstance(tag, ClassificationEntryTag.PageLink)
        ),
    }


@CS.register
def export_all_ces(filename: str) -> None:
    with Path(filename).open("w") as f:
        writer: "csv.DictWriter[str]" = csv.DictWriter(f, list(CEData.__annotations__))
        writer.writeheader()
        for ce in getinput.print_every_n(
            ClassificationEntry.select_valid(), label="classification entries", n=1000
        ):
            writer.writerow(data_for_ce(ce))


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
    """Export all known classification entries for a taxon.

    This produces a CSV file containing a column with data from the currently
    accepted classification in the Hesperomys database, and additional columns
    representing the classifications in sources that have been entered into the
    database.

    """
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
        "mdd_link",
        "iucn_link",
    ]
    for article, _ in ce_articles.most_common():
        citation = article.concise_citation()
        columns.append(f"{citation} {rank.display_name}")
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
            mdd_link = ""
            for tag in child_taxon.tags:
                if isinstance(tag, TaxonTag.MDD):
                    mdd_link = f"https://www.mammaldiversity.org/taxon/{tag.id}"
                    break
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
                "mdd_link": mdd_link,
            }
            for article, _ in ce_articles.most_common():
                valid_ces = taxon_to_article_to_valid_ces.get(child_taxon, {}).get(
                    article, []
                )
                synonym_ces = taxon_to_article_to_synonym_ces.get(child_taxon, {}).get(
                    article, []
                )
                citation = article.concise_citation()
                row[f"{citation} {rank.display_name}"] = ", ".join(
                    ce.name for ce in valid_ces
                )
                row[f"{citation} synonyms"] = ", ".join(ce.name for ce in synonym_ces)

                for ce in valid_ces:
                    if ce.page is not None and ce.page.startswith(
                        "https://www.iucnredlist.org/"
                    ):
                        row["iucn_link"] = ce.page
            writer.writerow(row)


class ComparisonRow(TypedDict):
    order: str
    family: str
    genus: str
    old_name: str
    old_page: str
    new_name: str
    new_page: str
    comment: str


CEPair = tuple[ClassificationEntry | None, ClassificationEntry | None, list[str]]


def _get_all_ces(art: Article) -> Iterable[ClassificationEntry]:
    yield from art.get_classification_entries()
    for child in Article.select_valid().filter(Article.parent == art):
        yield from _get_all_ces(child)


@CS.register
def generate_classification_diff(
    filename: str,
    old: Article | None = None,
    new: Article | None = None,
    old_taxon: str | None = None,
    new_taxon: str | None = None,
    rank: Rank = Rank.species,
) -> None:
    """Generate a CSV file comparing classification entries in two articles.

    This CSV contains the classifications of two articles ("old" and "new")
    side by side, with differences noted. Differences are annotated with the
    kind of change applied (e.g., "name change", "genus change", "spelling").

    The CSV is written to the *filename*. *old* and *new* point to the articles
    to be compared. *old_taxon* and *new_taxon* are the names of the taxa in the
    two articles. If this is blank, the entire classification is compared. By
    default, species are compared, but this can be changed with the *rank*
    parameter.

    """
    if old is None:
        old = Article.getter(None).get_one("Old article to compare> ")
    if old is None:
        return
    if new is None:
        new = Article.getter(None).get_one("New article to compare> ")
    if new is None:
        return
    old_ces = list(_get_all_ces(old))
    if not old_ces:
        print(f"No classification entries found in {old}")
        return
    new_ces = list(_get_all_ces(new))
    if not new_ces:
        print(f"No classification entries found in {new}")
        return
    print(f"Found {len(old_ces)} in {old} and {len(new_ces)} in {new}")
    if old_taxon is not None:
        old_ce = next(
            (
                ce
                for ce in old_ces
                if ce.name == old_taxon and ce.rank is not Rank.synonym
            ),
            None,
        )
        if old_ce is None:
            print(f"Could not find {old_taxon} in {old}")
            return
    else:
        old_ce = getinput.choose_one_by_name(
            old_ces,
            message="Old taxon to compare (leave blank to compare entire classification)> ",
            display_fn=lambda ce: ce.name,
            print_choices=False,
        )
    if new_taxon is not None:
        new_ce = next(
            (
                ce
                for ce in new_ces
                if ce.name == new_taxon and ce.rank is not Rank.synonym
            ),
            None,
        )
        if new_ce is None:
            print(f"Could not find {new_taxon} in {new}")
            return
    else:
        new_ce = getinput.choose_one_by_name(
            new_ces,
            message="New taxon to compare (leave blank to use same taxon as for old)> ",
            display_fn=lambda ce: ce.name,
            print_choices=False,
        )
    if new_ce is None and old_ce is not None:
        new_ce = next(
            (
                ce
                for ce in new_ces
                if ce.name == old_ce.name and ce.rank is not Rank.synonym
            ),
            None,
        )
        if new_ce is None:
            print(f"Could not find {old_ce.name} in {new}")
            return

    if old_ce is None:
        old_ces_to_compare = [ce for ce in old_ces if ce.rank is rank]
    else:
        old_ces_to_compare = list(old_ce.get_children_of_rank(rank))
    if new_ce is None:
        new_ces_to_compare = [ce for ce in new_ces if ce.rank is rank]
    else:
        new_ces_to_compare = list(new_ce.get_children_of_rank(rank))

    variant_base_to_ces: dict[
        Name, tuple[list[ClassificationEntry], list[ClassificationEntry]]
    ] = defaultdict(lambda: ([], []))
    for ce in old_ces_to_compare:
        if ce.mapped_name is None:
            continue
        variant_base = ce.mapped_name.resolve_variant()
        variant_base_to_ces[variant_base][0].append(ce)
    for ce in new_ces_to_compare:
        if ce.mapped_name is None:
            continue
        variant_base = ce.mapped_name.resolve_variant()
        variant_base_to_ces[variant_base][1].append(ce)

    pairs: list[CEPair] = []
    done_old: set[ClassificationEntry] = set()
    done_new: set[ClassificationEntry] = set()
    for old_ce_list, new_ce_list in variant_base_to_ces.values():
        match (len(old_ce_list), len(new_ce_list)):
            case (0, _) | (_, 0):
                continue
            case 1, 1:
                pairs.append(_create_pair(old_ce_list[0], new_ce_list[0]))
            case 1, _:
                for new_ce in new_ce_list:
                    _, _, description = _create_pair(old_ce_list[0], new_ce)
                    extra = f"; also matched {', '.join(ce.name for ce in new_ce_list if ce != new_ce)} in new"
                    pairs.append((old_ce_list[0], new_ce, [*description, extra]))
            case _, 1:
                for old_ce in old_ce_list:
                    _, _, description = _create_pair(old_ce, new_ce_list[0])
                    extra = f"; also matched {', '.join(ce.name for ce in old_ce_list if ce != old_ce)} in old"
                    pairs.append((old_ce, new_ce_list[0], [*description, extra]))
            case _:
                # Multiple matches on both sides. Just give up and list them all unpaired.
                description = [
                    f"could not match ({', '.join(ce.name for ce in old_ce_list)} in old, {', '.join(ce.name for ce in new_ce_list)} in new)"
                ]
                for old_ce in old_ce_list:
                    pairs.append((old_ce, None, description))
                for new_ce in new_ce_list:
                    pairs.append((None, new_ce, description))
        done_old.update(old_ce_list)
        done_new.update(new_ce_list)

    taxon_to_ces: dict[
        Taxon, tuple[list[ClassificationEntry], list[ClassificationEntry]]
    ] = defaultdict(lambda: ([], []))
    for ce in old_ces_to_compare:
        if ce.mapped_name is None:
            continue
        taxon_to_ces[ce.mapped_name.taxon][0].append(ce)
    for ce in new_ces_to_compare:
        if ce.mapped_name is None:
            continue
        taxon_to_ces[ce.mapped_name.taxon][1].append(ce)
    for taxon, (old_ce_list, new_ce_list) in taxon_to_ces.items():
        match (len(old_ce_list), len(new_ce_list)):
            case 1, 1:
                old_ce = old_ce_list[0]
                new_ce = new_ce_list[0]
                if old_ce in done_old or new_ce in done_new:
                    continue
                _, _, description = _create_pair(old_ce, new_ce, is_same_name=False)
                description = [
                    *description,
                    f"both currently allocated to {taxon.valid_name}",
                ]
                pairs.append((old_ce, new_ce, description))
                done_old.add(old_ce)
                done_new.add(new_ce)

    for ce in old_ces_to_compare:
        if ce in done_old:
            continue
        pairs.append((ce, None, ["missing in new (possible lump)"]))
    for ce in new_ces_to_compare:
        if ce in done_new:
            continue
        pairs.append((None, ce, ["missing in old (possible split or new species)"]))

    rows = [create_comparison_row(pair) for pair in pairs]
    rows = sorted(
        rows,
        key=lambda row: (
            row["order"],
            row["family"],
            row["genus"],
            row["old_name"] or row["new_name"],
            row["new_name"],
        ),
    )
    with Path(filename).open("w") as f:
        writer = csv.DictWriter(f, list(ComparisonRow.__annotations__))
        header_row = {key: key for key in writer.fieldnames}
        header_row["old_name"] = old.concise_citation()
        header_row["new_name"] = new.concise_citation()
        writer.writerow(header_row)
        for row in rows:
            writer.writerow(row)  # static analysis: ignore[incompatible_argument]


def create_comparison_row(pair: CEPair) -> ComparisonRow:
    old_ce, new_ce, description = pair

    order_ce = None
    if new_ce is not None:
        order_ce = new_ce.parent_of_rank(Rank.order)
    if order_ce is None and old_ce is not None:
        order_ce = old_ce.parent_of_rank(Rank.order)
    order = order_ce.name if order_ce is not None else ""

    family_ce = None
    if new_ce is not None:
        family_ce = new_ce.parent_of_rank(Rank.family)
    if family_ce is None and old_ce is not None:
        family_ce = old_ce.parent_of_rank(Rank.family)
    family = family_ce.name if family_ce is not None else ""

    genus_ce = None
    genus_name = ""
    if new_ce is not None:
        genus_ce = new_ce.parent_of_rank(Rank.genus)
        if genus_ce is None and new_ce.rank in (Rank.species, Rank.subspecies):
            corrected_name = new_ce.get_corrected_name()
            if corrected_name is not None:
                genus_name = corrected_name.split()[0]
    if genus_ce is None and not genus_name and old_ce is not None:
        genus_ce = old_ce.parent_of_rank(Rank.genus)
        if genus_ce is None and old_ce.rank in (Rank.species, Rank.subspecies):
            corrected_name = old_ce.get_corrected_name()
            if corrected_name is not None:
                genus_name = corrected_name.split()[0]

    genus = genus_ce.name if genus_ce is not None else genus_name
    return {
        "order": order,
        "family": family,
        "genus": genus,
        "old_name": old_ce.name if old_ce else "",
        "old_page": old_ce.page or "" if old_ce else "",
        "new_name": new_ce.name if new_ce else "",
        "new_page": new_ce.page or "" if new_ce else "",
        "comment": "; ".join(description),
    }


def _create_pair(
    old_ce: ClassificationEntry,
    new_ce: ClassificationEntry,
    *,
    is_same_name: bool = True,
) -> CEPair:
    if old_ce.name == new_ce.name:
        return old_ce, new_ce, []
    if old_ce.rank is not new_ce.rank:
        return old_ce, new_ce, ["change in rank"]
    old_name = old_ce.get_corrected_name()
    new_name = new_ce.get_corrected_name()
    if old_name is None or new_name is None:
        return old_ce, new_ce, ["name change"]
    if old_name == new_name:
        return old_ce, new_ce, ["name normalization"]
    match old_ce.rank:
        case Rank.species | Rank.subspecies:
            if old_name is None or new_name is None:
                return old_ce, new_ce, ["name change"]
            old_parts = old_name.split()
            new_parts = new_name.split()
            if old_parts[0] != new_parts[0]:
                return old_ce, new_ce, ["genus change"]
            if old_ce.rank is Rank.subspecies and old_parts[1] != new_parts[1]:
                return old_ce, new_ce, ["species change"]
            if old_parts[-1] != new_parts[-1]:
                return (
                    old_ce,
                    new_ce,
                    ["spelling change" if is_same_name else "name change"],
                )
            raise ValueError("unexpected name change")
    return old_ce, new_ce, ["name change"]
