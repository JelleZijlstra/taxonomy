"""Availability review for purported replacement names."""

from collections.abc import Callable

import pytest

from taxonomy.db.constants import (
    EmendationJustification,
    Group,
    NomenclatureStatus,
    Status,
)
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.taxon import Taxon

from .lint import _check_variant_tag, check_nomen_novum_for_unavailable_name
from .name import Name, NameTag


def _name(
    status: NomenclatureStatus = NomenclatureStatus.available,
    *,
    label: str = "original",
    tags: tuple[NameTag, ...] = (),
) -> Name:
    return Name.virtual(
        root_name=label,
        corrected_original_name=f"Mus {label}",
        nomenclature_status=status,
        tags=tags,
        type_tags=(),
        type_locality=None,
        type_specimen=None,
        year="1800",
        group=Group.species,
        status=Status.synonym,
        species_name_complex=None,
        taxon=Taxon.virtual(),
    )


def _replacement(target: Name) -> Name:
    name = _name(
        NomenclatureStatus.nomen_novum,
        label="replacement",
        tags=(NameTag.NomenNovumFor(target),),
    )
    name.year = "1900"
    name.taxon = target.taxon
    return name


def _messages(name: Name) -> list[str]:
    issues = list(
        check_nomen_novum_for_unavailable_name.linter(
            name, LintConfig(autofix=True, interactive=False)
        )
    )
    # A string diagnostic cannot carry a deterministic fix.
    assert all(isinstance(issue, str) for issue in issues)
    return [str(issue) for issue in issues]


@pytest.mark.parametrize(
    "status",
    [
        NomenclatureStatus.available,
        NomenclatureStatus.preoccupied,
        NomenclatureStatus.as_emended,
        NomenclatureStatus.partially_suppressed,
        NomenclatureStatus.hybrid_name,
        NomenclatureStatus.art_13_nomen_oblitum,
        NomenclatureStatus.collective_group,
    ],
)
def test_available_replacement_target(status: NomenclatureStatus) -> None:
    assert _messages(_replacement(_name(status))) == []


@pytest.mark.parametrize(
    "status",
    [
        NomenclatureStatus.nomen_nudum,
        NomenclatureStatus.misidentification,
        NomenclatureStatus.infrasubspecific,
        NomenclatureStatus.variety_or_form,
        NomenclatureStatus.not_used_as_valid,
        NomenclatureStatus.not_based_on_a_generic_name,
        NomenclatureStatus.unpublished,
        NomenclatureStatus.before_1758,
    ],
)
def test_unavailable_replacement_target_is_not_autofixed(
    status: NomenclatureStatus,
) -> None:
    target = _name(status)
    name = _replacement(target)
    before = [dict(n._clirm_virtual_values) for n in (name, target)]

    messages = _messages(name)

    assert len(messages) == 1
    assert status.name in messages[0]
    assert str(target.id) in messages[0]
    assert "independent description" in messages[0]
    assert "does not itself establish availability or shared types" in messages[0]
    assert [dict(n._clirm_virtual_values) for n in (name, target)] == before


@pytest.mark.parametrize(
    ("status", "tag_type"),
    [
        (NomenclatureStatus.nomen_novum, NameTag.NomenNovumFor),
        (
            NomenclatureStatus.justified_emendation,
            lambda name: NameTag.JustifiedEmendationOf(
                name, EmendationJustification.inadvertent_error
            ),
        ),
        (NomenclatureStatus.unjustified_emendation, NameTag.UnjustifiedEmendationOf),
        (
            NomenclatureStatus.incorrect_original_spelling,
            NameTag.IncorrectOriginalSpellingOf,
        ),
        (
            NomenclatureStatus.incorrect_subsequent_spelling,
            NameTag.IncorrectSubsequentSpellingOf,
        ),
        (NomenclatureStatus.name_combination, NameTag.NameCombinationOf),
        (NomenclatureStatus.mandatory_change, NameTag.MandatoryChangeOf),
        (NomenclatureStatus.subsequent_usage, NameTag.SubsequentUsageOf),
        (NomenclatureStatus.reranking, NameTag.RerankingOf),
    ],
)
@pytest.mark.parametrize(
    "base_status", [NomenclatureStatus.preoccupied, NomenclatureStatus.nomen_nudum]
)
def test_replacement_chain_checks_underlying_availability(
    status: NomenclatureStatus,
    tag_type: Callable[[Name], NameTag],
    base_status: NomenclatureStatus,
) -> None:
    base = _name(base_status)
    tag = tag_type(base)
    intermediate = _name(status, label="intermediate", tags=(tag,))

    messages = _messages(_replacement(intermediate))

    if base_status is NomenclatureStatus.preoccupied:
        assert messages == []
    else:
        assert len(messages) == 1
        assert "nomen_nudum" in messages[0]
        assert str(intermediate.id) in messages[0]
        assert str(base.id) in messages[0]


def test_unavailable_intermediate_is_not_skipped() -> None:
    intermediate = _replacement(_name())
    intermediate.nomenclature_status = NomenclatureStatus.nomen_nudum

    assert "nomen_nudum" in _messages(_replacement(intermediate))[0]


def test_checks_relationship_even_when_replacement_has_another_status() -> None:
    name = _replacement(_name(NomenclatureStatus.nomen_nudum))
    name.nomenclature_status = NomenclatureStatus.preoccupied
    assert "nomen_nudum" in _messages(name)[0]


@pytest.mark.parametrize(
    "status",
    [
        NomenclatureStatus.nomen_nudum,
        NomenclatureStatus.infrasubspecific,
        NomenclatureStatus.variety_or_form,
        NomenclatureStatus.not_used_as_valid,
        NomenclatureStatus.not_based_on_a_generic_name,
        NomenclatureStatus.unpublished,
        NomenclatureStatus.before_1758,
        NomenclatureStatus.fully_suppressed,
    ],
)
def test_unavailable_replacement_is_already_resolved(
    status: NomenclatureStatus,
) -> None:
    target = _name(NomenclatureStatus.nomen_nudum)
    name = _replacement(target)
    name.nomenclature_status = status
    name.tags = (*name.tags, NameTag.Condition(status))  # type: ignore[assignment]
    before = [dict(n._clirm_virtual_values) for n in (name, target)]

    assert _messages(name) == []
    assert [dict(n._clirm_virtual_values) for n in (name, target)] == before


def test_unavailable_replacement_still_warns_as_a_later_replacement_target() -> None:
    unavailable = _replacement(_name(NomenclatureStatus.nomen_nudum))
    unavailable.nomenclature_status = NomenclatureStatus.nomen_nudum
    unavailable.tags = (  # type: ignore[assignment]
        *unavailable.tags,
        NameTag.Condition(NomenclatureStatus.nomen_nudum),
    )

    assert _messages(unavailable) == []
    assert "nomen_nudum" in _messages(_replacement(unavailable))[0]


def test_no_replacement_relationship() -> None:
    name = _name(NomenclatureStatus.nomen_nudum)
    assert _messages(name) == []


@pytest.mark.parametrize(
    ("status", "tag_type"),
    [
        (NomenclatureStatus.misidentification, NameTag.MisidentificationOf),
        (NomenclatureStatus.nomen_nudum, NameTag.UnavailableVersionOf),
    ],
)
def test_does_not_cross_non_type_sharing_links(
    status: NomenclatureStatus, tag_type: Callable[[Name], NameTag]
) -> None:
    available = _name()
    target = _name(status, tags=(tag_type(available),))
    assert status.name in _messages(_replacement(target))[0]


@pytest.mark.parametrize(
    "status", [NomenclatureStatus.variant, NomenclatureStatus.unpublished_pending]
)
def test_uncertain_target_is_not_declared_unavailable(
    status: NomenclatureStatus,
) -> None:
    target = _name(status, tags=(NameTag.VariantOf(_name()),))
    message = _messages(_replacement(target))[0]
    assert "availability is unresolved" in message
    assert "marked unavailable" not in message


def test_suppression_requires_chronological_review() -> None:
    target = _name(NomenclatureStatus.fully_suppressed)
    message = _messages(_replacement(target))[0]
    assert "Commission decision and its chronology" in message
    assert "marked unavailable" not in message


@pytest.mark.parametrize(
    "status",
    [NomenclatureStatus.nomen_novum, NomenclatureStatus.incorrect_subsequent_spelling],
)
def test_missing_intermediate_link(status: NomenclatureStatus) -> None:
    assert (
        "missing replacement/variant link" in _messages(_replacement(_name(status)))[0]
    )


def test_ambiguous_intermediate_links() -> None:
    target = _name(
        NomenclatureStatus.name_combination,
        tags=(NameTag.NameCombinationOf(_name()), NameTag.NameCombinationOf(_name())),
    )
    assert "ambiguous replacement/variant links" in _messages(_replacement(target))[0]


def test_ambiguous_initial_replacement_links() -> None:
    name = _replacement(_name())
    name.tags = (*name.tags, NameTag.NomenNovumFor(_name()))  # type: ignore[assignment]
    assert "ambiguous NomenNovumFor targets" in _messages(name)[0]


def test_replacement_cycle() -> None:
    first = _replacement(_name())
    second = _replacement(first)
    first.tags = (NameTag.NomenNovumFor(second),)  # type: ignore[assignment]
    assert "contains a cycle" in _messages(first)[0]


def test_variant_cycle() -> None:
    first = _name(NomenclatureStatus.name_combination)
    second = _name(
        NomenclatureStatus.name_combination, tags=(NameTag.NameCombinationOf(first),)
    )
    first.tags = (NameTag.NameCombinationOf(second),)  # type: ignore[assignment]
    assert "contains a cycle" in _messages(_replacement(first))[0]


def test_long_acyclic_replacement_chain() -> None:
    target = _name()
    for _ in range(15):
        target = _replacement(target)
    assert _messages(target) == []


def test_existing_tag_lint_does_not_redirect_replacement_to_available_version() -> None:
    available = _name()
    available.year = "1850"
    target = _name(
        NomenclatureStatus.nomen_nudum, tags=(NameTag.UnavailableVersionOf(available),)
    )
    name = _replacement(target)
    tag = name.tags[0]
    checker = _check_variant_tag(tag, name, LintConfig(autofix=True))

    with pytest.raises(StopIteration) as exc:
        next(checker)

    assert exc.value.value == tag
    assert name.tags == (tag,)
    assert "nomen_nudum" in _messages(name)[0]
