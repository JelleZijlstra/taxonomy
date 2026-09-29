import functools
import itertools
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import assert_never

import Levenshtein

from taxonomy import getinput
from taxonomy.db.constants import Group, Rank
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.name.name import Name, NameTag, SelectionReason
from taxonomy.db.models.name_complex import (
    SpeciesNameComplex,
    normalize_root_name_for_homonymy,
)
from taxonomy.db.models.taxon import Taxon

from .base import LINT
from .helpers import get_parent


@dataclass
class PossibleHomonym:
    exact_name_match: bool = False
    fuzzy_name_match: bool = False
    same_original_genus: bool = False
    same_current_genus: bool = False
    edit_distance: int | None = None
    related_genus: bool = False

    def is_current_homonym(self) -> bool:
        return (self.same_original_genus or self.same_current_genus) and (
            self.exact_name_match or self.fuzzy_name_match
        )


def get_possible_homonyms(
    genus_name: str, root_name: str, sc: SpeciesNameComplex | None = None
) -> tuple[Iterable[Name], Iterable[tuple[Name, PossibleHomonym]]]:
    name_dict: dict[Name, PossibleHomonym] = defaultdict(PossibleHomonym)
    normalized_root_name = normalize_root_name_for_homonymy(root_name, sc)
    genera = {
        genus.resolve_variant(misidentification=True)
        for genus in Name.select_valid().filter(
            Name.group == Group.genus, Name.root_name == genus_name
        )
    }
    for genus in genera:
        primary_nonfuzzy = _get_primary_names_of_genus_and_variants(genus, fuzzy=False)
        for nam in primary_nonfuzzy.get(root_name, []):
            name_dict[nam].exact_name_match = True
            name_dict[nam].same_original_genus = True
        for other_root_name, nams in primary_nonfuzzy.items():
            distance = Levenshtein.distance(root_name, other_root_name)
            if distance <= 2:
                for nam in nams:
                    name_dict[nam].same_original_genus = True
                    name_dict[nam].edit_distance = distance
        primary_fuzzy = _get_primary_names_of_genus_and_variants(genus, fuzzy=True)
        for nam in primary_fuzzy.get(normalized_root_name, []):
            name_dict[nam].fuzzy_name_match = True
            name_dict[nam].same_original_genus = True
        taxon = get_parent(genus)
        if taxon is not None:
            secondary_nonfuzzy = _get_secondary_names_of_genus(taxon, fuzzy=False)
            for nam in secondary_nonfuzzy.get(root_name, []):
                name_dict[nam].exact_name_match = True
                name_dict[nam].same_current_genus = True
            for other_root_name, nams in secondary_nonfuzzy.items():
                distance = Levenshtein.distance(root_name, other_root_name)
                if distance <= 2:
                    for nam in nams:
                        name_dict[nam].same_current_genus = True
                        name_dict[nam].edit_distance = distance

            secondary_fuzzy = _get_secondary_names_of_genus(taxon, fuzzy=True)
            for nam in secondary_fuzzy.get(normalized_root_name, []):
                name_dict[nam].fuzzy_name_match = True
                name_dict[nam].same_current_genus = True

            if taxon.parent is not None:
                for related_genus in taxon.parent.get_children().filter(
                    Taxon.rank == Rank.genus
                ):
                    secondary_nonfuzzy = _get_secondary_names_of_genus(
                        related_genus, fuzzy=False
                    )
                    for other_root_name, nams in secondary_nonfuzzy.items():
                        distance = Levenshtein.distance(root_name, other_root_name)
                        if distance <= 2:
                            for nam in nams:
                                name_dict[nam].related_genus = True
                                name_dict[nam].edit_distance = distance

    return genera, name_dict.items()


def _clear_homonym_caches() -> None:
    _get_primary_names_of_genus.cache_clear()
    _get_secondary_names_of_genus.cache_clear()


@LINT.add(
    "species_secondary_homonym",
    required_resources={LintResource.SLOW},
    clear_caches=_clear_homonym_caches,
)
def check_species_group_secondary_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.secondary_homonymy, fuzzy=False, cfg=cfg
    )


@LINT.add("species_primary_homonym", required_resources={LintResource.SLOW})
def check_species_group_primary_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.primary_homonymy, fuzzy=False, cfg=cfg
    )


@LINT.add("species_mixed_homonym", disabled=True)
def check_species_group_mixed_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.mixed_homonymy, fuzzy=False, cfg=cfg
    )


@LINT.add("species_reverse_mixed_homonym", disabled=True)
def check_species_group_reverse_mixed_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.reverse_mixed_homonymy, fuzzy=False, cfg=cfg
    )


@LINT.add("species_fuzzy_secondary_homonym", required_resources={LintResource.SLOW})
def check_species_group_fuzzy_secondary_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.secondary_homonymy, fuzzy=True, cfg=cfg
    )


@LINT.add("species_fuzzy_primary_homonym", required_resources={LintResource.SLOW})
def check_species_group_fuzzy_primary_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.primary_homonymy, fuzzy=True, cfg=cfg
    )


@LINT.add("species_fuzzy_mixed_homonym", disabled=True)
def check_species_group_fuzzy_mixed_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.mixed_homonymy, fuzzy=True, cfg=cfg
    )


@LINT.add("species_fuzzy_reverse_mixed_homonym", disabled=True)
def check_species_group_fuzzy_reverse_mixed_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.reverse_mixed_homonymy, fuzzy=True, cfg=cfg
    )


@LINT.add("genus_homonym")
def check_genus_group_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.genus:
        return
    if not nam.can_preoccupy():
        return
    possible_homonyms = list(
        Name.select_valid().filter(
            Name.root_name == nam.root_name, Name.group == Group.genus
        )
    )
    yield from _check_homonym_list(nam, possible_homonyms, cfg=cfg)


def _check_species_group_homonyms(
    nam: Name, *, reason: SelectionReason, fuzzy: bool, cfg: LintConfig
) -> Iterable[str]:
    if nam.group is not Group.species:
        return
    if not nam.can_preoccupy():
        return
    match reason:
        case SelectionReason.primary_homonymy:
            genus = nam.original_parent
            if genus is None:
                return
            genus = genus.resolve_variant(misidentification=True)
            name_dict = _get_primary_names_of_genus_and_variants(genus, fuzzy=fuzzy)
        case SelectionReason.secondary_homonymy:
            genus = get_parent(nam)
            if genus is None:
                return
            name_dict = _get_secondary_names_of_genus(genus, fuzzy=fuzzy)
        case SelectionReason.mixed_homonymy:
            genus = get_parent(nam)
            if genus is None:
                return
            genus = genus.base_name.resolve_variant(misidentification=True)
            name_dict = _get_primary_names_of_genus_and_variants(genus, fuzzy=fuzzy)
        case SelectionReason.reverse_mixed_homonymy:
            genus = nam.original_parent
            if genus is None:
                return
            name_dict = _get_secondary_names_of_genus(genus.taxon, fuzzy=fuzzy)
        case SelectionReason.synonymy:
            return
        case _:
            assert_never(reason)
    root = (
        nam.get_normalized_root_name_for_homonymy()
        if fuzzy
        else nam.get_normalized_root_name()
    )
    possible_homonyms = name_dict.get(root, [])
    yield from _check_homonym_list(
        nam, possible_homonyms, reason=reason, fuzzy=fuzzy, cfg=cfg
    )


def _check_homonym_list(
    nam: Name,
    possible_homonyms: Sequence[Name],
    *,
    reason: SelectionReason = SelectionReason.primary_homonymy,
    fuzzy: bool = False,
    cfg: LintConfig,
) -> Iterable[str]:
    relevant_names = [
        other_nam
        for other_nam in possible_homonyms
        if nam != other_nam and other_nam.can_preoccupy()
        # TODO: not nam.has_priority_over(other_nam)
        and other_nam.has_priority_over(nam)
    ]
    if fuzzy:
        # Exclude non-fuzzy matches, the regular check will catch them.
        relevant_names = [
            other_nam
            for other_nam in relevant_names
            if other_nam.get_normalized_root_name() != nam.get_normalized_root_name()
        ]

    if not relevant_names:
        return
    relevant_tags: tuple[type[NameTag], ...] = (
        NameTag.PreoccupiedBy,
        NameTag.UnjustifiedEmendationOf,
        NameTag.JustifiedEmendationOf,
        NameTag.PrimaryHomonymOf,
        NameTag.SecondaryHomonymOf,
        NameTag.VariantOf,
        NameTag.IncorrectSubsequentSpellingOf,
        NameTag.SubsequentUsageOf,
        NameTag.MisidentificationOf,
        NameTag.NameCombinationOf,
        NameTag.AsEmendedBy,
        NameTag.RerankingOf,
    )
    if fuzzy:
        # Allow ignoring preoccupation only for fuzzy matches
        relevant_tags += (NameTag.IgnorePreoccupationBy,)
    already_variant_of = {
        # Types don't communicate that these are all tags that have a name attribute
        # static analysis: ignore[attribute_is_never_set]
        tag.name
        for tag in nam.tags
        if isinstance(tag, relevant_tags)
    }

    for senior_homonym in relevant_names:
        # Ignore secondary homonyms that are also primary homonyms
        if (
            reason is SelectionReason.secondary_homonymy
            and nam.original_parent is not None
            and senior_homonym.original_parent == nam.original_parent
        ):
            continue

        # Ignore nomina oblita
        if any(
            isinstance(tag, NameTag.NomenOblitum) and tag.name == nam
            for tag in senior_homonym.tags
        ):
            continue

        # Already marked as a homonym
        if senior_homonym in already_variant_of:
            continue

        # Marked as needing resolution through First Reviser action
        if nam.get_date_object() == senior_homonym.get_date_object() and any(
            isinstance(tag, NameTag.NeedsPrioritySelection)
            and tag.reason == reason
            and tag.over == senior_homonym
            for tag in nam.tags
        ):
            continue

        if cfg.interactive:
            getinput.print_header(nam)
            print(f"{nam}: preoccupied by {senior_homonym}")
            if getinput.yes_no("Accept preoccupation? "):
                if nam.group is Group.species:
                    if reason is SelectionReason.primary_homonymy:
                        tag = NameTag.PrimaryHomonymOf
                    else:
                        tag = NameTag.SecondaryHomonymOf
                else:
                    tag = NameTag.PreoccupiedBy
                nam.preoccupied_by(senior_homonym, tag=tag)
            elif (
                nam.get_date_object() == senior_homonym.get_date_object()
                and getinput.yes_no("Mark as needing First Reviser selection instead? ")
            ):
                nam.add_tag(
                    NameTag.NeedsPrioritySelection(over=senior_homonym, reason=reason)
                )
            elif getinput.yes_no("Mark as subsequent usage instead? "):
                nam.add_tag(NameTag.SubsequentUsageOf(senior_homonym))
        yield f"preoccupied by {senior_homonym}"


def _get_primary_names_of_genus_and_variants(
    genus: Name, *, fuzzy: bool = False
) -> dict[str, list[Name]]:
    all_genera = {genus}
    stack = [genus]
    while stack:
        current = stack.pop()
        for nam in itertools.chain(
            current.get_derived_field("variants") or (),
            current.get_derived_field("unjustified_emendations") or (),
            current.get_derived_field("justified_emendations") or (),
            current.get_derived_field("incorrect_original_spellings") or (),
            current.get_derived_field("subsequent_usages") or (),
            current.get_derived_field("mandatory_changes") or (),
            current.get_derived_field("incorrect_subsequent_spellings") or (),
        ):
            if nam not in all_genera:
                all_genera.add(nam)
                stack.append(nam)
    if len(all_genera) == 1:
        return _get_primary_names_of_genus(genus, fuzzy=fuzzy)
    root_name_to_names: dict[str, list[Name]] = {}
    for nam in all_genera:
        for root_name, names in _get_primary_names_of_genus(nam, fuzzy=fuzzy).items():
            root_name_to_names.setdefault(root_name, []).extend(names)
    return root_name_to_names


@functools.lru_cache(maxsize=8192)
def _get_primary_names_of_genus(
    genus: Name, *, fuzzy: bool = False
) -> dict[str, list[Name]]:
    root_name_to_names: dict[str, list[Name]] = {}
    for nam in Name.add_validity_check(genus.original_children):
        if nam.group is Group.species and nam.year is not None:
            if fuzzy:
                root_name = nam.get_normalized_root_name_for_homonymy()
            else:
                root_name = nam.get_normalized_root_name()
            root_name_to_names.setdefault(root_name, []).append(nam)
    return root_name_to_names


@functools.lru_cache(maxsize=8192)
def _get_secondary_names_of_genus(
    genus: Taxon, *, fuzzy: bool = False
) -> dict[str, list[Name]]:
    root_name_to_names: dict[str, list[Name]] = {}
    for nam in genus.all_names():
        if nam.group is Group.species and nam.year is not None:
            if fuzzy:
                root_name = nam.get_normalized_root_name_for_homonymy()
            else:
                root_name = nam.get_normalized_root_name()
            root_name_to_names.setdefault(root_name, []).append(nam)
    return root_name_to_names
