from collections.abc import Iterable, Sequence
from typing import assert_never

from taxonomy.db.constants import AgeClass, Group, NomenclatureStatus, Rank, Status
from taxonomy.db.models.article import Article, ArticleTag
from taxonomy.db.models.name.name import Name, NameTag
from taxonomy.db.models.taxon import Taxon


def get_parent(nam: Name, rank: Rank = Rank.genus) -> Taxon | None:
    try:
        return nam.taxon.parent_of_rank(rank)
    except ValueError:
        return None


def fix_type_specimen_link(url: str) -> str:
    if url.startswith(
        (
            "http://arctos.database.museum/",
            "http://researcharchive.calacademy.org/",
            "http://ucmpdb.berkeley.edu/cgi/",
        )
    ):
        return url.replace("http://", "https://")
    return url


EXCLUDED_TAXA = [6, 12]  # Nonorganic  # Legendary


def smallest_common_ancestor(nams: Sequence[Name]) -> Taxon:
    if len(nams) < 1:
        raise ValueError("need at least one name")
    elif len(nams) == 1:
        return get_effective_taxon(nams[0])
    parent_lists = [list(all_parents(nam)) for nam in nams]
    if any(not parents for parents in parent_lists):
        raise ValueError("one of the names has no taxon")

    # If any of the taxa are not real, use the real ones
    non_excluded_lists = [
        parent_list
        for parent_list in parent_lists
        if not any(t.id in EXCLUDED_TAXA for t in parent_list)
    ]
    if non_excluded_lists:
        parent_lists = non_excluded_lists
    if len(parent_lists) == 1:
        return parent_lists[0][0]
    first, *rest = parent_lists
    for common_ancestor in first:
        if all(common_ancestor in parents for parents in rest):
            return common_ancestor
    raise ValueError("no common ancestor found")


def all_parents(nam: Name) -> Iterable[Taxon]:
    yield from iter_parents(nam.taxon)


def iter_parents(taxon: Taxon) -> Iterable[Taxon]:
    seen: set[Taxon] = set()
    while True:
        yield taxon
        seen.add(taxon)
        if taxon.parent is None:
            break
        taxon = taxon.parent
        if taxon in seen:
            raise ValueError(f"cycle in taxon hierarchy involving {taxon}")


def get_effective_taxon(nam: Name) -> Taxon:
    if nam.group is not Group.family or nam.original_rank is None:
        return nam.taxon
    resolved_nam = nam.resolve_variant()
    try:
        candidate = (
            Taxon.select_valid()
            .filter(Taxon.base_name == resolved_nam, Taxon.rank == nam.original_rank)
            .get()
        )
    except Taxon.DoesNotExist:
        return nam.taxon
    else:
        return candidate


def is_high_or_invalid_family(nam: Name) -> bool:
    assert isinstance(nam.group, Group)
    match nam.group:
        case Group.high:
            return True
        case Group.family:
            return (
                nam.nomenclature_status
                is NomenclatureStatus.not_based_on_a_generic_name
            )
        case Group.genus | Group.species:
            return False
        case _:
            assert_never(nam.group)


def resolve_usage(nam: Name, *, resolve_unavailable_version_of: bool) -> Name:
    if target := nam.get_tag_target(NameTag.SubsequentUsageOf):
        return resolve_usage(
            target, resolve_unavailable_version_of=resolve_unavailable_version_of
        )
    if target := nam.get_tag_target(NameTag.MisidentificationOf):
        return resolve_usage(
            target, resolve_unavailable_version_of=resolve_unavailable_version_of
        )
    if target := nam.get_tag_target(NameTag.NameCombinationOf):
        return resolve_usage(
            target, resolve_unavailable_version_of=resolve_unavailable_version_of
        )
    if target := nam.get_tag_target(NameTag.RerankingOf):
        return resolve_usage(
            target, resolve_unavailable_version_of=resolve_unavailable_version_of
        )
    if resolve_unavailable_version_of:
        if target := nam.get_tag_target(NameTag.UnavailableVersionOf):
            return resolve_usage(
                target, resolve_unavailable_version_of=resolve_unavailable_version_of
            )
    return nam


def is_valid_mammal(nam: Name) -> bool:
    if nam.status is not Status.valid:
        if nam.nomenclature_status is NomenclatureStatus.preoccupied:
            valid_name = nam.taxon.base_name
            if (
                valid_name.nomenclature_status is NomenclatureStatus.nomen_novum
                and valid_name.get_tag_target(NameTag.NomenNovumFor) == nam
            ):
                return is_valid_mammal(valid_name)
        return False
    taxon = nam.taxon
    if taxon.age not in (AgeClass.extant, AgeClass.recently_extinct):
        return False
    if taxon.rank is Rank.subspecies and not taxon.is_nominate_subspecies():
        return False
    return taxon.get_derived_field("class_").valid_name == "Mammalia"


def has_classification(art: Article) -> bool:
    return any(art.get_classification_entries()) and not art.has_tag(
        ArticleTag.PartialClassification
    )
