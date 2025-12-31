"""Lint steps for Taxon."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Container, Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Self

from taxonomy.db import helpers, models
from taxonomy.db.constants import AgeClass, Group, NomenclatureStatus, Rank, Status
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint

from .taxon import Taxon


def remove_unused_ignores(taxon: Taxon, unused: Container[str]) -> None:
    new_tags = []
    for tag in taxon.tags:
        if (
            isinstance(tag, models.tags.TaxonTag.IgnoreLintTaxon)
            and tag.label in unused
        ):
            print(f"{taxon}: removing unused IgnoreLint tag: {tag}")
        else:
            new_tags.append(tag)
    taxon.tags = new_tags  # type: ignore[assignment]


def get_ignores(taxon: Taxon) -> Iterable[IgnoreLint]:
    return taxon.get_tags(taxon.tags, models.tags.TaxonTag.IgnoreLintTaxon)


LINT = Lint(Taxon, get_ignores, remove_unused_ignores)


@LINT.add("parent")
def check_parent(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    if taxon.parent is None and taxon.id != 1:
        yield f"{taxon}: missing parent"
    if taxon.parent is not None and not taxon.age.can_have_parent_of_age(
        taxon.parent.age
    ):
        yield (
            f"{taxon}: is {taxon.age!r}, but its parent {taxon.parent} is"
            f" {taxon.parent.age!r}"
        )
    parent = _first_ranked_parent(taxon.parent)
    if (
        parent is not None
        and taxon.rank is not Rank.unranked
        and taxon.rank >= parent.rank
    ):
        yield (
            f"{taxon}: is of rank {taxon.rank.name}, but parent {parent} is of rank"
            f" {parent.rank.name}"
        )


def _first_ranked_parent(taxon: Taxon | None, *, depth: int = 20) -> Taxon | None:
    if depth == 0 or taxon is None:
        return None
    if taxon.rank is not Rank.unranked:
        return taxon
    return _first_ranked_parent(taxon.parent, depth=depth - 1)


@LINT.add("parent_cycle")
def check_parent_cycle(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    if taxon.parent is None:
        return
    current = taxon
    seen = set()
    while current is not None:
        seen.add(current)
        current = current.parent
        if current == taxon or current in seen:
            yield f"{taxon}: parent cycle detected: {current} -> {taxon}"
            return


@LINT.add("rank")
def check_rank(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    if not taxon.rank.is_allowed_for_taxon:
        yield f"{taxon}: has disallowed rank {taxon.rank!r}"
    group = helpers.group_of_rank(taxon.rank)
    if group != taxon.base_name.group:
        yield f"{taxon}: rank {taxon.rank.name} does not match group {taxon.base_name.group.name}"


@LINT.add("base_name")
def check_base_name(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    if not taxon.base_name.status.is_base_name():
        yield f"base name has invalid status {taxon.base_name.status}"
    expected_group = helpers.group_of_rank(taxon.rank)
    if expected_group != taxon.base_name.group:
        rank = taxon.rank.name
        group = taxon.base_name.group.name
        yield f"group mismatch: rank {rank} but group {group}"
    resolved = taxon.base_name.resolve_variant()
    if resolved != taxon.base_name:
        message = f"base name is a variant: {taxon.base_name} -> {resolved}"
        if cfg.autofix:
            print(f"{taxon}: {message}")
            _switch_basename(taxon, resolved)
        else:
            yield message


@LINT.add("nominal_genus")
def check_nominal_genus(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    nominal_genus_tags = list(
        taxon.get_tags(taxon.tags, models.tags.TaxonTag.NominalGenus)
    )
    if len(nominal_genus_tags) > 1:
        yield f"has multiple nominal genus tags: {nominal_genus_tags}"
    elif len(nominal_genus_tags) == 1:
        nominal_genus = nominal_genus_tags[0].genus
        if nominal_genus.group is not Group.genus:
            yield f"nominal genus {nominal_genus} is not a genus"
    if (
        taxon.base_name.group is Group.species
        and taxon.base_name.status is Status.valid
        and not nominal_genus_tags
        and not taxon.has_parent_of_rank(Rank.genus)
    ):
        if cfg.autofix:
            orig_nam = taxon.base_name.corrected_original_name
            if orig_nam:
                orig_genus, *_ = orig_nam.split()
                candidates = list(
                    models.Name.select_valid().filter(
                        models.Name.group == Group.genus,
                        models.Name.root_name == orig_genus,
                    )
                )
                if len(candidates) == 1:
                    print(f"adding NominalGenus tag: {candidates[0]}")
                    taxon.add_tag(
                        models.tags.TaxonTag.NominalGenus(genus=candidates[0])
                    )
                    return
        yield "should have NominalGenus tag"


@LINT.add("valid_name")
def check_valid_name(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    computed = taxon.compute_valid_name()
    if computed is None or taxon.valid_name == computed:
        return
    message = (
        f"{taxon}: valid name mismatch: {taxon.valid_name} (actual) vs."
        f" {computed} (computed)"
    )
    # For species-group taxa, we always trust the computed name. Usually these
    # have been reassigned to a different genus, or changed between species and
    # subspecies, or they have become nomina dubia (in which case we use the
    # corrected original name). For family-group names we don't always trust the
    # computed name, because stems may be arbitrary.
    can_fix = cfg.autofix and (
        taxon.base_name.group == Group.species or taxon.is_nominate_subgenus()
    )
    if can_fix:
        print(message)
        taxon.recompute_name()
    else:
        yield message


@LINT.add("basal_tags")
def check_basal_tags(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    has_is = taxon.has_tag(models.tags.TaxonTag.IncertaeSedis)
    has_basal = taxon.has_tag(models.tags.TaxonTag.Basal)
    if has_is and has_basal:
        yield "has both IncertaeSedis and Basal tags"
    if taxon.needs_basal_tag():
        if not has_is and not has_basal:
            yield (
                f"parent taxon {taxon.parent} has higher-ranked children,"
                " but child lacks 'incertae sedis' or 'basal' tag"
            )
    else:
        if has_is:
            yield "has unnecessary IncertaeSedis tag"
        if has_basal:
            yield "has unnecessary Basal tag"


@LINT.add("age")
def check_age(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    if taxon.age is AgeClass.extant:
        children = list(taxon.get_children())
        if children and not any(child.age is AgeClass.extant for child in children):
            yield "extant taxon has no extant children"


@LINT.add("valid_base_name")
def check_valid_base_name(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    if (
        taxon.base_name.status is Status.valid
        and not taxon.base_name.can_be_valid_base_name()
    ):
        yield f"{taxon}: base name {taxon.base_name} is not valid (status {taxon.base_name.nomenclature_status.name})"


@LINT.add("expected_base_name")
def check_conservative_expected_base_name(
    taxon: Taxon, cfg: LintConfig
) -> Iterable[str]:
    expected_base = get_conservative_expected_base_name(taxon)
    if expected_base is None:
        return
    if taxon.base_name == expected_base:
        return
    message = f"expected base name to be {expected_base}, but is {taxon.base_name}"
    if cfg.autofix:
        print(f"{taxon}: {message}")
        _switch_basename(taxon, expected_base)
    else:
        yield message


def _switch_basename(taxon: Taxon, new_base_name: models.Name) -> None:
    if taxon.base_name.taxon == taxon:
        taxon.switch_basename(new_base_name)
    else:
        taxon.base_name = new_base_name


def get_conservative_expected_base_name(taxon: Taxon) -> models.Name | None:
    # For now, only include family-group taxa.
    if taxon.group() != Group.family:
        return None
    if taxon.base_name.type is None:
        return None
    names = set(taxon.get_names())
    if taxon.base_name.taxon != taxon:
        names |= set(taxon.base_name.taxon.get_names())
    group = taxon.group()
    # Don't worry about the case where there is an older name based on a different type
    available_names = {
        nam
        for nam in names
        if nam.group == group
        and nam.can_be_valid_base_name()
        and nam.type == taxon.base_name.type
    }
    if not available_names:
        return None
    names_and_dates = sorted(
        [(nam, nam.get_date_object()) for nam in available_names],
        key=lambda pair: pair[1],
    )
    selected_pair = names_and_dates[0]
    if selected_pair[0] != taxon.base_name:
        possible = {nam for nam, date in names_and_dates if date == selected_pair[1]}
        if taxon.base_name in possible:
            # If there are multiple names from the same year, assume we got the priority right
            return taxon.base_name
    return selected_pair[0]


@dataclass
class NameWithPriority:
    name: models.Name
    priority_date: date
    takes_priority_of: list[models.Name]
    nomen_oblitum: list[models.Name]
    reversal_of_priority: list[models.Name]
    selection_of_priority: list[models.Name]
    selection_of_spelling: list[models.Name]

    @classmethod
    def from_name(cls, name: models.Name) -> Self:
        takes_prio = list(name.get_names_taking_priority())
        nomen_oblitum = list(name.get_tag_targets(models.name.NameTag.NomenOblitum))
        reversal_of_prio = list(
            name.get_tag_targets(models.name.NameTag.ReversalOfPriority)
        )
        selection_of_prio = list(
            name.get_tag_targets(models.name.NameTag.SelectionOfPriority)
        )
        selection_of_spelling = list(
            name.get_tag_targets(models.name.NameTag.SelectionOfSpelling)
        )
        dates = [
            name.get_date_object(),
            *[takes_prio.get_date_object() for takes_prio in takes_prio],
        ]
        return cls(
            name=name,
            priority_date=min(dates),
            takes_priority_of=takes_prio,
            nomen_oblitum=nomen_oblitum,
            reversal_of_priority=reversal_of_prio,
            selection_of_priority=selection_of_prio,
            selection_of_spelling=selection_of_spelling,
        )

    def dominates(self, other: NameWithPriority) -> bool:
        if other is self:
            return True

        # Reversals where we win
        if other.name in self.selection_of_priority:
            return True
        if other.name in self.selection_of_spelling:
            return True
        if self.name in other.nomen_oblitum:
            return True
        if other.name in self.reversal_of_priority:
            return True
        if other.name in self.takes_priority_of:
            return True

        # Reversals where we lose
        if self.name in other.selection_of_priority:
            return False
        if self.name in other.selection_of_spelling:
            return False
        if other.name in self.nomen_oblitum:
            return False
        if self.name in other.reversal_of_priority:
            return False
        if self.name in other.takes_priority_of:
            return False

        # Normal priority
        if self.priority_date < other.priority_date:
            return True
        if self.priority_date > other.priority_date:
            return False
        if self.name.original_rank is not None and other.name.original_rank is not None:
            if (
                self.name.original_rank.comparison_value
                > other.name.original_rank.comparison_value
            ):
                return True
        return False


@dataclass
class BaseNameReport:
    possibilities: list[models.Name]
    comments: Sequence[str] = ()


def get_expected_base_name(txn: Taxon) -> models.Name:
    report = get_expected_base_name_report(txn)
    return report.possibilities[0]


@LINT.add("full_expected_base_name", disabled=True)
def check_full_expected_base_name(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    if taxon.base_name.group is Group.high:
        return  # Ignore priority for unregulated names
    report = get_expected_base_name_report(taxon)
    if len(report.possibilities) == 1:
        (expected_base_name,) = report.possibilities
        if taxon.base_name != expected_base_name:
            message = f"senior synonym is {expected_base_name} (current base name: {taxon.base_name})"
            if report.comments:
                message += f" ({', '.join(report.comments)})"
            yield message
        return
    if taxon.base_name in report.possibilities:
        if cfg.verbose:
            message = f"base name {taxon.base_name} is correct, but ambiguous (possibilities: {', '.join(map(str, report.possibilities))})"
            if report.comments:
                message += f" ({', '.join(report.comments)})"
            print(f"{taxon}: {message}")
    else:
        message = f"senior synonym is one of {', '.join(map(str, report.possibilities))} (current base name: {taxon.base_name})"
        if report.comments:
            message += f" ({', '.join(report.comments)})"
        yield message


def get_expected_base_name_report(txn: Taxon) -> BaseNameReport:
    nams = list(get_possible_base_names(txn))
    if len(nams) == 1:
        return BaseNameReport(nams, ["Only one possible name"])
    names_plus = [
        NameWithPriority.from_name(nam) for nam in get_possible_base_names(txn)
    ]
    dates = sorted({name.priority_date for name in names_plus})
    by_date = defaultdict(list)
    for name in names_plus:
        by_date[name.priority_date].append(name)

    comments = []
    possible_base_names: list[NameWithPriority] = []
    for date_obj in dates:
        names = by_date[date_obj]
        if len(names) > 1:
            # Keep undominated names within this date slice: names for which no
            # other name in the same slice dominates them. This allows multiple
            # co-maximal names (e.g., 2 dominate the other 2, or 2 dominate the
            # third but not each other).
            undominated = [
                n
                for n in names
                if not any(o is not n and o.dominates(n) for o in names)
            ]
            if len(undominated) != len(names):
                comments.append(
                    f"Undominated for {date_obj} (among {len(names)} total): {undominated}"
                )
                names = undominated
        possible_base_names.extend(names)
        has_later_dominators = any(
            later_name.dominates(our_name)
            for later_name in names_plus
            if later_name.priority_date > date_obj
            for our_name in names
        )
        if not has_later_dominators:
            break

    if len(possible_base_names) == 1:
        return BaseNameReport([name.name for name in possible_base_names], comments)
    # Across all accumulated candidates, again keep those not dominated by
    # any other candidate.
    undominated_final = [
        n
        for n in possible_base_names
        if not any(o is not n and o.dominates(n) for o in possible_base_names)
    ]
    comments.append(
        f"Undominated among {len(possible_base_names)} final candidates: {undominated_final}"
    )
    return BaseNameReport([n.name for n in undominated_final], comments)


LIMITATIONS: list[Callable[[models.Name], bool]] = [
    lambda nam: nam.can_be_valid_base_name() and nam.year is not None,  # type: ignore[has-type]
    lambda nam: (
        nam.can_be_valid_base_name()
        or nam.nomenclature_status is NomenclatureStatus.preoccupied  # type: ignore[has-type]
    )
    and nam.year is not None,  # type: ignore[has-type]
    lambda nam: nam.year is not None,  # type: ignore[has-type]
]


def get_possible_base_names(txn: Taxon) -> set[models.Name]:
    group = txn.base_name.group
    names = set(_get_possible_base_names(txn, group=group))
    for predicate in LIMITATIONS:
        filtered_names = {nam for nam in names if predicate(nam)}
        if filtered_names:
            return filtered_names
    return names


def _get_possible_base_names(txn: Taxon, *, group: Group) -> Iterable[models.Name]:
    my_group = txn.base_name.group
    match group:
        case Group.family:
            if my_group in {Group.genus, Group.species}:
                return
        case Group.genus:
            if my_group is Group.species:
                return

    for nam in txn.get_names():
        if nam.group is group:
            yield nam
    if group is Group.high:
        return
    for child in txn.get_children():
        yield from _get_possible_base_names(child, group=group)
