from taxonomy.db.constants import NamingConvention, PersonType

from .name_matching import (
    external_identity_matches_person,
    public_identity_matches_person,
)
from .person import Person


def _person(family_name: str, given_names: str) -> Person:
    return Person.virtual(
        family_name=family_name,
        given_names=given_names,
        naming_convention=NamingConvention.unspecified,
        type=PersonType.unchecked,
    )


def test_external_identity_accepts_reversed_split_fields() -> None:
    assert external_identity_matches_person(
        given_names="KADJO",
        family_names="Blaise",
        credit_name=None,
        other_names=(),
        person=_person("Kadjo", "Blaise"),
    )


def test_external_identity_accepts_missing_family_name_apostrophe() -> None:
    assert external_identity_matches_person(
        given_names="Princia Margaret",
        family_names="Dsouza",
        credit_name=None,
        other_names=(),
        person=_person("D'Souza", "Princia Margaret"),
    )


def test_external_identity_rejects_conflicting_full_given_names() -> None:
    assert not external_identity_matches_person(
        given_names="Julia Elizabeth",
        family_names="Fa",
        credit_name=None,
        other_names=(),
        person=_person("Fa", "John E."),
    )


def test_external_identity_accepts_full_name_in_given_name_field() -> None:
    assert external_identity_matches_person(
        given_names="Dr Vinaya Kumar Singh",
        family_names=None,
        credit_name=None,
        other_names=(),
        person=_person("Singh", "Vinaya Kumar"),
    )


def test_external_identity_accepts_initial_punctuation_variation() -> None:
    assert external_identity_matches_person(
        given_names="Brie",
        family_names="Ilarde",
        credit_name="Gabriele C. T. Ilarde",
        other_names=(),
        person=_person("Ilarde", "Gabriele C.T."),
    )


def test_external_identity_accepts_complete_name_with_extra_components() -> None:
    assert external_identity_matches_person(
        given_names="Paul Martinet Taku Bisong",
        family_names=None,
        credit_name=None,
        other_names=(),
        person=_person("Bisong", "Paul Taku"),
    )


def test_external_identity_accepts_reordered_complete_name_with_middle_name() -> None:
    person = _person("Nguyen", "Sang")
    person.tussenvoegsel = "Ngoc"
    person.naming_convention = NamingConvention.vietnamese
    assert external_identity_matches_person(
        given_names="Sang",
        family_names="Nguyen",
        credit_name=None,
        other_names=(),
        person=person,
    )


def test_external_identity_accepts_native_order_vietnamese_name() -> None:
    person = _person("Nguyen", "Son")
    person.tussenvoegsel = "Truong"
    person.naming_convention = NamingConvention.vietnamese
    assert external_identity_matches_person(
        given_names="Nguyen Truong",
        family_names="Son",
        credit_name="Nguyen Truong Son",
        other_names=(),
        person=person,
    )


def test_public_identity_accepts_reordered_name_with_middle_initial() -> None:
    assert public_identity_matches_person(
        given_names="Denton Robert",
        family_names=None,
        credit_name=None,
        other_names=(),
        person=_person("Denton", "Robert K."),
    )


def test_public_identity_accepts_concatenated_pinyin_given_name() -> None:
    person = _person("Ge", "De-yan")
    person.naming_convention = NamingConvention.pinyin
    assert public_identity_matches_person(
        given_names="Deyan",
        family_names="Ge",
        credit_name=None,
        other_names=(),
        person=person,
    )


def test_public_identity_accepts_reversed_concatenated_pinyin_name() -> None:
    person = _person("Tang", "Ke-yi")
    person.naming_convention = NamingConvention.pinyin
    assert public_identity_matches_person(
        given_names="Tang",
        family_names="Keyi",
        credit_name=None,
        other_names=(),
        person=person,
    )


def test_public_identity_accepts_minor_spelling_difference() -> None:
    assert public_identity_matches_person(
        given_names="Prem",
        family_names="Chhetri",
        credit_name=None,
        other_names=(),
        person=_person("Chettri", "Prem Kumar"),
    )


def test_public_identity_still_rejects_conflicting_given_name() -> None:
    assert not public_identity_matches_person(
        given_names="Nestor",
        family_names="Abdala",
        credit_name=None,
        other_names=(),
        person=_person("Abdala", "Fernando"),
    )


def test_public_identity_accepts_incomplete_single_given_name() -> None:
    assert public_identity_matches_person(
        given_names="Brad",
        family_names=None,
        credit_name=None,
        other_names=(),
        person=_person("Maryan", "Brad"),
    )


def test_public_identity_rejects_unrelated_single_name() -> None:
    assert not public_identity_matches_person(
        given_names="Brad",
        family_names=None,
        credit_name=None,
        other_names=(),
        person=_person("Maryan", "Peter"),
    )
