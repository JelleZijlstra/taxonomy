from unittest.mock import Mock

import pytest

from taxonomy import parsing
from taxonomy.db.constants import NamingConvention, PersonType
from taxonomy.db.models.article.citations import format_authors

from .person import Person


def _vietnamese_person() -> Person:
    return Person.virtual(
        family_name="Nguyen",
        given_names="Son",
        tussenvoegsel="Truong",
        naming_convention=NamingConvention.vietnamese,
        type=PersonType.checked,
        tags=(),
    )


def test_vietnamese_name_components() -> None:
    person = _vietnamese_person()

    assert person.get_full_name() == "Nguyen Truong Son"
    assert person.get_full_name(family_first=True) == "Nguyen, Truong Son"
    assert person.get_initials() == "T.S."
    assert person.taxonomic_authority() == "Nguyen"


def test_vietnamese_citation_uses_middle_initial() -> None:
    article = Mock()
    article.get_authors.return_value = [_vietnamese_person()]

    assert format_authors(article) == "Nguyen, T.S."


def test_vietnamese_publication_alias_controls_citation_name() -> None:
    canonical = Person.virtual(
        family_name="Vuong",
        given_names="Tu",
        tussenvoegsel="Tan",
        naming_convention=NamingConvention.vietnamese,
        type=PersonType.checked,
        tags=(),
    )
    publication_alias = Person.virtual(
        family_name="Tu",
        given_names="Vuong Tan",
        naming_convention=NamingConvention.general,
        type=PersonType.alias,
        target=canonical,
        tags=(),
    )
    article = Mock()
    article.get_authors.return_value = [publication_alias]

    assert (
        canonical.get_full_name() == publication_alias.get_full_name() == "Vuong Tan Tu"
    )
    assert publication_alias.taxonomic_authority() == "Tu"
    assert format_authors(article) == "Tu, V.T."


def test_vietnamese_compound_family_name_is_valid() -> None:
    assert parsing.matches_grammar(
        "Trương Quan", parsing.vietnamese_family_name_pattern
    )


@pytest.mark.parametrize(
    "naming_convention",
    [
        NamingConvention.pinyin,
        NamingConvention.chinese,
        NamingConvention.japanese,
        NamingConvention.korean,
        NamingConvention.hungarian,
    ],
)
def test_family_name_first_conventions(naming_convention: NamingConvention) -> None:
    person = Person.virtual(
        family_name="Yang",
        given_names="Zhong-jian",
        naming_convention=naming_convention,
        type=PersonType.checked,
        tags=(),
    )

    assert person.get_full_name() == "Yang Zhong-jian"
    assert person.get_full_name(family_first=True) == "Yang, Zhong-jian"


def test_pinyin_hyphen_preserves_initials() -> None:
    person = Person.virtual(
        family_name="Yang",
        given_names="Zhong-jian",
        naming_convention=NamingConvention.pinyin,
        type=PersonType.checked,
        tags=(),
    )

    assert person.get_initials() == "Z.-j."


def test_ukrainian_uses_ukrainian_romanization() -> None:
    person = Person.virtual(
        family_name="Загороднюк",
        given_names="Ігор Володимирович",
        naming_convention=NamingConvention.ukrainian,
        type=PersonType.checked,
        tags=(),
    )
    article = Mock()
    article.get_authors.return_value = [person]

    assert person.get_transliterated_family_name() == "Zahorodniuk"
    assert person.get_transliterated_initials() == "I.V."
    assert format_authors(article, romanize=True) == "Zahorodniuk, I.V."
