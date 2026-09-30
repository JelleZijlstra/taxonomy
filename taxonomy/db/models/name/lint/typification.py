"""Lint checks for type taxa and type designations."""

from collections.abc import Iterable

from taxonomy.db import models
from taxonomy.db.constants import Group, SpeciesGroupType, TypeSpeciesDesignation
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import field_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import ExperimentalLintCondition as Exp
from taxonomy.db.models.name.name import Name, NameTag, TypeTag
from taxonomy.db.models.name.type_specimen import type_specimens_equal

from .base import LINT
from .helpers import is_valid_mammal


@LINT.add("type_designation_strict")
def check_type_designation_strict(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # Move to check_type_designation below if this is fixed for all names
    if not is_valid_mammal(nam) or nam.has_type_tag(TypeTag.InterpretedTypeSpecimen):
        return
    match nam.genus_type_kind:
        case TypeSpeciesDesignation.subsequent_designation:
            if not any(
                tag.type == nam.type
                for tag in nam.get_tags(nam.type_tags, TypeTag.TypeDesignation)
            ):
                yield "missing a reference for type species designation"
        case TypeSpeciesDesignation.subsequent_monotypy:
            if not any(
                tag.type == nam.type
                for tag in nam.get_tags(nam.type_tags, TypeTag.TypeDesignation)
            ):
                yield "missing a reference for type species designation"

    match nam.species_type_kind:
        case SpeciesGroupType.lectotype:
            if nam.type_specimen is None:
                yield "type specimen is not set"
                return
            if not any(
                type_specimens_equal(tag.lectotype, nam.type_specimen)
                for tag in nam.get_tags(nam.type_tags, TypeTag.LectotypeDesignation)
            ):
                yield "missing a reference for lectotype designation"
        case SpeciesGroupType.neotype:
            if nam.type_specimen is None:
                yield "type specimen is not set"
                return
            if not any(
                type_specimens_equal(tag.neotype, nam.type_specimen)
                for tag in nam.get_tags(nam.type_tags, TypeTag.NeotypeDesignation)
            ):
                yield "missing a reference for neotype designation"
        case None:
            if (
                nam.original_citation is not None
                and nam.group is Group.species
                and models.name.required_fields.is_desired(nam, "type_specimen")
                and Exp().should_apply(nam, cfg)
            ):
                yield "missing type specimen"


@LINT.add("type_designation")
def check_type_designation(nam: Name, cfg: LintConfig) -> Iterable[str]:
    match nam.genus_type_kind:
        case TypeSpeciesDesignation.designated_by_the_commission:
            tag = nam.get_type_tag(TypeTag.CommissionTypeDesignation)
            if tag is None:
                yield "type species is set to designated_by_the_commission, but missing CommissionTypeDesignation tag"

    match nam.species_type_kind:
        case SpeciesGroupType.lectotype:
            if (
                nam.type_specimen is not None
                and nam.has_type_tag(TypeTag.LectotypeDesignation)
                and not any(
                    type_specimens_equal(tag.lectotype, nam.type_specimen)
                    for tag in nam.get_tags(nam.type_tags, TypeTag.LectotypeDesignation)
                )
            ):
                yield "missing a reference for lectotype designation"
        case SpeciesGroupType.neotype:
            if (
                nam.type_specimen is not None
                and nam.has_type_tag(TypeTag.NeotypeDesignation)
                and not any(
                    type_specimens_equal(tag.neotype, nam.type_specimen)
                    for tag in nam.get_tags(nam.type_tags, TypeTag.NeotypeDesignation)
                )
            ):
                yield "missing a reference for neotype designation"


@LINT.add("type_taxon")
def correct_type_taxon(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    """Moves names to child taxa if the type allows it."""
    if nam.group not in (Group.genus, Group.family):
        return
    if nam.type is None:
        return
    if nam.taxon == nam.type.taxon:
        return
    expected_taxon = nam.type.taxon.parent
    while (
        expected_taxon is not None
        and expected_taxon.base_name.group != nam.group
        and expected_taxon != nam.taxon
    ):
        expected_taxon = expected_taxon.parent
    if expected_taxon is None:
        return
    if nam.taxon != expected_taxon:
        message = f"expected taxon to be {expected_taxon} not {nam.taxon}"
        if expected_taxon.is_child_of(nam.taxon):
            yield field_issue(message, nam, "taxon", expected_taxon)
        else:
            yield message


@LINT.add("type")
def check_type(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    """Checks for the type taxon."""
    if nam.type is None:
        return
    if not nam.type.taxon.is_child_of(nam.taxon):
        yield f"type {nam.type} is not a child of {nam.taxon}"
    if nam.type.get_date_object() > nam.get_date_object() and not nam.has_type_tag(
        TypeTag.GenusCoelebs
    ):
        yield f"type {nam.type} ({nam.type.get_date_object()}) is younger than {nam} ({nam.get_date_object()})"

    if (
        (target := nam.type.get_tag_target(NameTag.UnavailableVersionOf)) is not None
        and nam.get_date_object() >= target.get_date_object()
        and not nam.has_name_tag(NameTag.UnavailableVersionOf)
    ):
        message = f"type is an unavailable version of {target}"
        yield field_issue(message, nam, "type", target)

    match nam.group:
        case Group.family:
            if nam.type.group is not Group.genus:
                yield f"type {nam.type} is in group {nam.type.group!r}, not genus"
        case Group.genus:
            if nam.type.group is not Group.species:
                yield f"type {nam.type} is in group {nam.type.group!r}, not species"


@LINT.add("infer_family_group_type")
def infer_family_group_type(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if (
        nam.group is not Group.family
        or nam.type is not None
        or not models.name.required_fields.is_desired(nam, "type")
    ):
        return
    stem = nam.get_family_group_stem()
    possible_types = [
        child_nam
        for child_nam in nam.taxon.all_names()
        if child_nam.group is Group.genus
        and child_nam.name_complex is not None
        and child_nam.safe_get_stem() == stem
    ]
    if len(possible_types) != 1:
        if cfg.verbose:
            print(f"{nam}: could not infer type for family {nam.root_name}")
        return
    message = f"inferred type {possible_types[0]} for family {nam}"
    yield field_issue(message, nam, "type", possible_types[0])
