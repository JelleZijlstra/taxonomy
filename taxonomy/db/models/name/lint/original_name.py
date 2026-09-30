import re
from collections.abc import Iterable
from typing import assert_never

from taxonomy.db import helpers, models
from taxonomy.db.constants import Group, NomenclatureStatus, SpeciesNameKind, Status
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import field_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import Name, NameTag, TypeTag

from .base import LINT
from .tags import get_applicable_nomenclature_statuses_from_tags


def _make_con_messsage(nam: Name, text: str) -> str:
    return f"corrected original name {nam.corrected_original_name!r} {text}"


CON_REGEX = re.compile(r"^[A-Z][a-z]+( [a-z]+){0,3}$")
CON_GENUS_FAMILY_REGEX = re.compile(r"^[A-Z][a-z]+$")
CON_HIGH_REGEX = re.compile(r"^(Pan-|Apo-|Zoo-)?[A-Z][a-z]+$")


@LINT.add("corrected_original_name")
def check_corrected_original_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check that corrected_original_names are correct."""
    if nam.corrected_original_name is None:
        return
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    inferred = nam.infer_corrected_original_name()
    if (
        inferred is not None
        and inferred != nam.corrected_original_name
        and inferred.replace(" ", "") != nam.corrected_original_name.replace(" ", "")
    ):
        yield _make_con_messsage(
            nam,
            f"inferred name {inferred!r} does not match current name"
            f" {nam.corrected_original_name!r}",
        )
    if nam.group is Group.species:
        con_regex = CON_REGEX
    elif nam.group is Group.high:
        con_regex = CON_HIGH_REGEX
    else:
        con_regex = CON_GENUS_FAMILY_REGEX
    if not con_regex.fullmatch(nam.corrected_original_name):
        yield _make_con_messsage(nam, "contains unexpected characters")
        return
    if (
        nam.original_name is not None
        and nam.original_name != nam.corrected_original_name
        and nam.original_name.count(" ") == nam.corrected_original_name.count(" ")
        and con_regex.fullmatch(nam.original_name)
    ):
        yield _make_con_messsage(
            nam, f"is different from original name {nam.original_name!r}"
        )
    if nam.group in (Group.high, Group.genus):
        if " " in nam.corrected_original_name:
            yield _make_con_messsage(nam, "contains whitespace")
        elif nam.corrected_original_name != nam.root_name:
            emended = nam.get_tag_target(NameTag.AsEmendedBy)
            if emended is not None and emended.root_name == nam.root_name:
                return
            yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")
    elif nam.group is Group.species:
        parts = nam.corrected_original_name.split(" ")
        if len(parts) not in (2, 3, 4):
            yield _make_con_messsage(nam, "is not a valid species or subspecies name")
        elif parts[-1] != nam.root_name:
            emended = nam.get_tag_target(NameTag.AsEmendedBy)
            if emended is not None and emended.root_name == nam.root_name:
                return
            if nam.species_name_complex is not None:
                try:
                    forms = list(nam.species_name_complex.get_forms(nam.root_name))
                except ValueError as e:
                    yield _make_con_messsage(nam, f"has invalid name complex: {e!r}")
                    return
                if parts[-1] in forms:
                    return
            yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")


def _make_rn_message(nam: Name, text: str) -> str:
    return f"root name {nam.root_name!r} {text}"


@LINT.add("root_name")
def check_root_name(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    """Check that root_names are correct."""
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    match nam.group:
        case Group.high:
            if not re.match(CON_HIGH_REGEX, nam.root_name):
                yield _make_rn_message(nam, "contains unexpected characters")
        case Group.genus | Group.family:
            if not re.match(CON_GENUS_FAMILY_REGEX, nam.root_name):
                yield _make_rn_message(nam, "contains unexpected characters")
        case Group.species:
            if not re.match(r"^[a-z]+$", nam.root_name):
                yield _make_rn_message(nam, "contains unexpected characters")
                return
            yield from _check_species_name_gender(nam, cfg)
        case _:
            assert_never(nam.group)


def _check_rn_matches_original(
    nam: Name, corrected_original_name: str, cfg: LintConfig, reason: str
) -> Iterable[LintResult]:
    con_root = corrected_original_name.rsplit(maxsplit=1)[-1]
    if con_root == nam.root_name:
        return
    message = _make_con_messsage(
        nam, f"does not match root_name {nam.root_name!r} ({reason})"
    )
    yield field_issue(message, nam, "root_name", con_root)


def _check_species_name_gender(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    emending_name = nam.get_tag_target(NameTag.AsEmendedBy)
    if emending_name is not None:
        corrected_original_name = emending_name.corrected_original_name
    else:
        corrected_original_name = nam.corrected_original_name
    if corrected_original_name is not None:
        con_root = corrected_original_name.split()[-1]
    else:
        con_root = None
    # For nomina dubia we always follow the original name
    if nam.status in (Status.nomen_dubium, Status.species_inquirenda):
        if corrected_original_name is not None:
            yield from _check_rn_matches_original(
                nam, corrected_original_name, cfg, nam.status.name
            )
        return
    # If there is no name complex, the root_name should match exactly
    if nam.species_name_complex is None:
        if corrected_original_name is not None:
            yield from _check_rn_matches_original(
                nam, corrected_original_name, cfg, "no name complex"
            )
        return
    if nam.species_name_complex.kind is not SpeciesNameKind.adjective:
        if corrected_original_name is not None:
            yield from _check_rn_matches_original(
                nam, corrected_original_name, cfg, "not an adjective"
            )
        return
    if nam.species_name_complex.is_invariant_adjective():
        if corrected_original_name is not None:
            yield from _check_rn_matches_original(
                nam, corrected_original_name, cfg, "invariant adjective"
            )
        return
    if not nam.nomenclature_status.can_preoccupy() and nam.nomenclature_status not in (
        NomenclatureStatus.subsequent_usage,
        NomenclatureStatus.name_combination,
    ):
        if nam.corrected_original_name is None:
            return
        expected_form = nam.corrected_original_name.split()[-1]
        rn_message = "for original name"
    else:
        # Now we have an adjective that needs to agree in gender with its genus, so we
        # have to find the genus. But first we check whether the name even makes sense.
        try:
            forms = list(nam.species_name_complex.get_forms(nam.root_name))
        except ValueError as e:
            yield _make_con_messsage(nam, f"has invalid name complex: {e!r}")
            return
        if con_root is not None and con_root not in forms:
            yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")
            return

        taxon = nam.taxon
        genus = taxon.get_current_genus()
        if genus is None or genus.name_complex is None:
            return

        genus_gender = genus.name_complex.gender
        expected_form = nam.species_name_complex.get_form(nam.root_name, genus_gender)
        rn_message = f"for {genus_gender.name} genus {genus}"
    if expected_form != nam.root_name:
        message = _make_rn_message(
            nam, f"does not match expected form {expected_form!r} {rn_message}"
        )
        yield field_issue(message, nam, "root_name", expected_form)


@LINT.add("family_root_name")
def check_family_root_name(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.group is not Group.family:
        return
    if set(get_applicable_nomenclature_statuses_from_tags(nam)) & {
        NomenclatureStatus.not_based_on_a_generic_name,
        NomenclatureStatus.not_intended_as_a_scientific_name,
    }:
        if nam.root_name != nam.corrected_original_name:
            yield _make_rn_message(nam, "does not match corrected original name")
        return
    if nam.original_rank is None:
        return
    expected_suffix = helpers.SUFFIXES.get(nam.original_rank)
    if expected_suffix is not None and not nam.root_name.endswith(expected_suffix):
        yield _make_rn_message(
            nam, f"does not match original rank ending -{expected_suffix}"
        )
    if nam.type is None:
        return
    resolved_type = nam.type.resolve_variant()
    try:
        stem_name = resolved_type.get_stem()
    except ValueError:
        yield f"{resolved_type} has bad name complex: {resolved_type.name_complex}"
        return
    if stem_name is None:
        yield f"type {resolved_type} has no stem"
        return
    if expected_suffix is None:
        expected_suffix = ""
    expected_root_name = stem_name + expected_suffix
    if nam.root_name == expected_root_name:
        return
    if stem_name.endswith("id"):
        # The Code allows eliding -id- from the stem.
        if nam.root_name == stem_name.removesuffix("id") + expected_suffix:
            return
    if nam.has_type_tag(TypeTag.IncorrectGrammar):
        return
    if nam.root_name == stem_name:
        message = f"Autofixing root name: {nam.root_name} -> {expected_root_name}"
        yield field_issue(message, nam, "root_name", expected_root_name)
    elif stem_name.endswith("id") and nam.root_name == stem_name.removesuffix("id"):
        message = f"Autofixing root name: {nam.root_name} -> {expected_root_name}"
        yield field_issue(
            message, nam, "root_name", stem_name.removesuffix("id") + expected_suffix
        )
    else:
        yield _make_rn_message(
            nam, f"does not match expected stem {expected_root_name!r}"
        )


@LINT.add("infer_corrected_original_name")
def autoset_corrected_original_name(
    nam: Name, cfg: LintConfig, *, aggressive: bool = False
) -> Iterable[LintResult]:
    if nam.original_name is None or nam.corrected_original_name is not None:
        return
    if not models.name.required_fields.is_desired(nam, "corrected_original_name"):
        return
    inferred = nam.infer_corrected_original_name(aggressive=aggressive)
    if inferred:
        message = (
            f"inferred corrected_original_name to be {inferred!r} from"
            f" {nam.original_name!r}"
        )
        yield field_issue(message, nam, "corrected_original_name", inferred)
    else:
        yield f"could not infer corrected original name from {nam.original_name!r}"


@LINT.add("infer_original_name")
def infer_original_name(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.original_name is not None:
        return
    if nam.group not in (Group.genus, Group.high):
        return
    message = f"inferred original name from root name {nam.root_name}"
    yield field_issue(message, nam, "original_name", nam.root_name)
