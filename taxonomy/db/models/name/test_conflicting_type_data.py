from collections.abc import Callable, Iterable

import pytest

from taxonomy.db.constants import (
    AgeClass,
    AltitudeUnit,
    ArticleKind,
    Group,
    NamingConvention,
    NomenclatureStatus,
    PersonType,
    SpeciesGroupType,
    SpecimenAge,
    SpecimenGender,
    SpecimenOrgan,
)
from taxonomy.db.models import Article, Location, Name, Person, Taxon
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint_types import LintIssue

from .lint import derived_tags
from .lint.type_locality import check_location_detail_coordinates
from .lint.type_tags import check_conflicting_type_data
from .name import CONFLICTING_TYPE_DATA_FIELDS, TypeTag
from .required_fields import Requiredness, get_missing_requirements, get_type_tag_status

CFG = LintConfig(autofix=False, interactive=False, available_resources=frozenset())


def make_name(*tags: TypeTag) -> Name:
    return Name.virtual(
        group=Group.species,
        year="2000",
        nomenclature_status=NomenclatureStatus.available,
        species_type_kind=SpeciesGroupType.holotype,
        type_specimen="BMNH 1",
        type_locality=Location.virtual(name="Example locality"),
        original_citation=Article.virtual(
            name="original.pdf", kind=ArticleKind.electronic, tags=()
        ),
        taxon=Taxon.virtual(age=AgeClass.extant),
        type_tags=tags,
    )


def test_conflict_serialization() -> None:
    tag = TypeTag.ConflictingTypeData("Gender", "Original: male; catalogue: female.")
    assert tag.serialize() == [78, "Gender", "Original: male; catalogue: female."]
    assert TypeTag.unserialize(tag.serialize()) == tag


@pytest.mark.parametrize(
    "tag",
    [
        TypeTag.Gender(SpecimenGender.male),
        TypeTag.Age(SpecimenAge.adult),
        TypeTag.Date("1900"),
        TypeTag.Altitude("100", AltitudeUnit.m),
        TypeTag.Coordinates("10°N", "20°E"),
        TypeTag.Organ(SpecimenOrgan.skin),
        TypeTag.CollectedBy(Person.virtual(family_name="Example")),
    ],
)
def test_conflict_cannot_coexist_with_derived_tag(tag: TypeTag) -> None:
    field = type(tag).__name__
    conflict = TypeTag.ConflictingTypeData(field, "Sources disagree.")
    nam = make_name(conflict)
    assert list(check_conflicting_type_data(nam, CFG)) == []
    nam.type_tags = (*nam.type_tags, tag)  # type: ignore[assignment]
    (issue,) = check_conflicting_type_data(nam, CFG)
    assert f"has both {field} and ConflictingTypeData({field!r})" in issue.message
    assert issue.fix is None
    assert nam.type_tags == (conflict, tag)


@pytest.mark.parametrize(
    "field", ["gender", "Collector", "ImaginaryTag", "SpecimenDetail"]
)
def test_conflict_requires_exact_derived_tag_name(field: str) -> None:
    nam = make_name(TypeTag.ConflictingTypeData(field, "Sources disagree."))
    (issue,) = check_conflicting_type_data(nam, CFG)
    assert "invalid ConflictingTypeData field" in issue.message
    assert not get_type_tag_status(nam, TypeTag.Gender).is_nonempty
    assert not get_type_tag_status(nam, TypeTag.SpecimenDetail).is_nonempty


def test_conflict_comment_and_duplicates() -> None:
    nam = make_name(
        TypeTag.ConflictingTypeData("Gender", "Sources disagree."),
        TypeTag.ConflictingTypeData("Gender", " "),
    )
    issues = list(check_conflicting_type_data(nam, CFG))
    assert len(issues) == 2
    assert "multiple ConflictingTypeData" in issues[0].message
    assert "needs a comment" in issues[1].message


@pytest.mark.parametrize("field", sorted(CONFLICTING_TYPE_DATA_FIELDS))
def test_conflict_satisfies_only_matching_field(field: str) -> None:
    conflict = TypeTag.ConflictingTypeData(field, "Sources disagree.")
    nam = make_name(conflict)
    for candidate in CONFLICTING_TYPE_DATA_FIELDS:
        status = get_type_tag_status(nam, getattr(TypeTag, candidate))
        assert status.is_nonempty is (candidate == field)
        assert status.value == ((conflict,) if candidate == field else ())
    assert not get_type_tag_status(nam, TypeTag.SpecimenDetail).is_nonempty
    assert not get_type_tag_status(nam, TypeTag.LocationDetail).is_nonempty


def test_conflict_preserves_requiredness_and_completeness() -> None:
    nam = make_name()
    before = get_type_tag_status(nam, TypeTag.Gender)
    assert before.requiredness is Requiredness.desired
    assert not before.is_nonempty
    nam.type_tags = (TypeTag.ConflictingTypeData("Gender", "Sources disagree."),)  # type: ignore[assignment]
    after = get_type_tag_status(nam, TypeTag.Gender)
    assert after.requiredness is before.requiredness
    missing = dict(get_missing_requirements(nam, levels=None, include_fields=False))
    assert "type tag Gender" not in missing
    assert "type tag Age" in missing
    assert (
        get_type_tag_status(nam, TypeTag.ConflictingTypeData).requiredness
        is Requiredness.optional
    )
    nam.group = Group.genus
    assert (
        get_type_tag_status(nam, TypeTag.ConflictingTypeData).requiredness
        is Requiredness.disallowed
    )
    assert not get_type_tag_status(nam, TypeTag.Gender).is_nonempty


@pytest.mark.parametrize(
    ("field", "text", "check"),
    [
        ("Gender", "Type. Adult male.", derived_tags.check_specimen_detail_gender),
        ("Age", "Type. Adult male.", derived_tags.check_specimen_detail_age),
        ("Date", "Collected 3 March 1900.", derived_tags.check_specimen_detail_date),
        ("Organ", "Skin; skull.", derived_tags.check_specimen_detail_organ),
        ("Altitude", "Altitude 100 m.", derived_tags.check_location_detail_altitude),
        ("Coordinates", "10°N, 20°E.", check_location_detail_coordinates),
        (
            "CollectedBy",
            "Collected by Jane Smith.",
            derived_tags.check_specimen_detail_collector,
        ),
    ],
)
def test_conflict_blocks_inference(
    field: str,
    text: str,
    check: Callable[[Name, LintConfig], Iterable[LintIssue]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = Person.virtual(
        family_name="Smith",
        given_names="Jane",
        naming_convention=NamingConvention.english,
        type=PersonType.checked,
    )
    monkeypatch.setattr(derived_tags, "_person_index", lambda: {"janesmith": {person}})
    source = Article.virtual(name="source.pdf")
    cls = (
        TypeTag.LocationDetail
        if field in {"Altitude", "Coordinates"}
        else TypeTag.SpecimenDetail
    )
    nam = make_name(
        cls(text, source), TypeTag.ConflictingTypeData(field, "Sources disagree.")
    )
    before = nam.type_tags
    assert list(check(nam, LintConfig(autofix=True, interactive=False))) == []
    assert nam.type_tags == before
    # The same quotation must be actionable when no conflict is recorded.
    nam.type_tags = before[:-1]
    assert any(issue.fix is not None for issue in check(nam, CFG))


def test_reviewed_gender_disagreement_does_not_recur() -> None:
    source = Article.virtual(name="source.pdf")
    nam = make_name(
        TypeTag.SpecimenDetail("Type. Male.", source),
        TypeTag.SpecimenDetail(
            "Holotype, female.", Article.virtual(name="catalog.pdf")
        ),
        TypeTag.ConflictingTypeData(
            "Gender", "Original description and catalogue disagree."
        ),
    )
    assert list(derived_tags.check_specimen_detail_gender(nam, CFG)) == []
    assert list(check_conflicting_type_data(nam, CFG)) == []
    # A conflict in another field cannot conceal an unreviewed disagreement.
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags[:2],
        TypeTag.ConflictingTypeData("Age", "Age uncertain."),
    )
    (issue,) = derived_tags.check_specimen_detail_gender(nam, CFG)
    assert "conflicting source evidence" in issue.message


def test_other_fields_still_infer() -> None:
    nam = make_name(
        TypeTag.SpecimenDetail("Type. Adult male.", Article.virtual(name="source.pdf")),
        TypeTag.ConflictingTypeData("Gender", "Sources disagree."),
    )
    (issue,) = derived_tags.check_specimen_detail_age(nam, CFG)
    assert issue.fix is not None
    assert "add Age" in issue.message
