"""Lint steps for Taxon."""

from __future__ import annotations

from collections.abc import Container, Iterable

from taxonomy.db import helpers, models
from taxonomy.db.constants import Group, Rank, Status
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
    parent = taxon.parent
    if (
        parent is not None
        and parent.rank is not Rank.unranked
        and taxon.rank is not Rank.unranked
    ):
        if taxon.rank >= parent.rank:
            yield (
                f"{taxon}: is of rank {taxon.rank.name}, but parent is of rank"
                f" {parent.rank.name}"
            )


@LINT.add("base_name")
def check_base_name(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    if not taxon.base_name.status.is_base_name():
        yield f"{taxon}: base name has invalid status {taxon.base_name.status}"
    expected_group = helpers.group_of_rank(taxon.rank)
    if expected_group != taxon.base_name.group:
        rank = taxon.rank.name
        group = taxon.base_name.group.name
        yield f"{taxon}: group mismatch: rank {rank} but group {group}"


@LINT.add("nominal_genus")
def check_nominal_genus(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    nominal_genus_tags = list(
        taxon.get_tags(taxon.tags, models.tags.TaxonTag.NominalGenus)
    )
    if len(nominal_genus_tags) > 1:
        yield f"{taxon}: has multiple nominal genus tags: {nominal_genus_tags}"
    elif len(nominal_genus_tags) == 1:
        nominal_genus = nominal_genus_tags[0].genus
        if nominal_genus.group is not Group.genus:
            yield f"{taxon}: nominal genus {nominal_genus} is not a genus"
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
                    print(f"{taxon}: adding NominalGenus tag: {candidates[0]}")
                    taxon.add_tag(
                        models.tags.TaxonTag.NominalGenus(genus=candidates[0])
                    )
                    return
        yield f"{taxon}: should have NominalGenus tag"


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
        yield f"{taxon}: has both IncertaeSedis and Basal tags"
    if taxon.needs_basal_tag():
        if not has_is and not has_basal:
            yield (
                f"{taxon}: parent taxon {taxon.parent} has higher-ranked children,"
                " but child lacks 'incertae sedis' or 'basal' tag"
            )
    else:
        if has_is:
            yield f"{taxon}: has unnecessary IncertaeSedis tag"
        if has_basal:
            yield f"{taxon}: has unnecessary Basal tag"


@LINT.add("expected_base_name")
def check_expected_base_name(taxon: Taxon, cfg: LintConfig) -> Iterable[str]:
    expected_base = get_conservative_expected_base_name(taxon)
    if expected_base is None:
        return
    if taxon.base_name == expected_base:
        return
    message = f"expected base name to be {expected_base}, but is {taxon.base_name}"
    yield message


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
