from unittest.mock import Mock

import pytest

from taxonomy.apis import orcid
from taxonomy.db import models
from taxonomy.db.constants import NamingConvention, PersonType
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.person import (
    Person,
    is_more_specific_than,
    is_valid_orcid,
    lint,
    normalize_orcid,
)
from taxonomy.db.models.person.lint import multiple_orcids


def _person_with_tags(*tags: object) -> Mock:
    person = Mock(spec=Person)
    person.tags = tags
    person.get_tags.side_effect = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )
    return person


def _profile_person(*orcids: str) -> Mock:
    person = _person_with_tags(
        *(models.tags.PersonTag.ORCID(value) for value in orcids)
    )
    person.id = 1
    person.type = PersonType.checked
    person.target = None
    person.family_name = "Smith"
    person.given_names = "Jane"
    person.initials = None
    person.tussenvoegsel = None
    person.suffix = None
    person.naming_convention = NamingConvention.english
    person.get_full_name.return_value = "Jane Smith"
    return person


def test_orcid_normalization_and_validation() -> None:
    assert (
        normalize_orcid("https://orcid.org/0000-0002-1694-233x")
        == "0000-0002-1694-233X"
    )
    assert is_valid_orcid("0000-0002-1694-233X")
    assert not is_valid_orcid("0000-0002-1694-2330")


def test_multiple_orcids() -> None:
    person = _person_with_tags(
        models.tags.PersonTag.ORCID("0000-0002-1694-233X"),
        models.tags.PersonTag.ORCID("0000-0003-1577-6568"),
    )

    issues = list(
        multiple_orcids.linter(person, LintConfig(autofix=False, interactive=False))
    )

    assert len(issues) == 1
    assert "multiple ORCID tags" in str(issues[0])


def test_unchecked_person_may_have_orcid_work_ignore() -> None:
    person = Person.virtual(
        family_name="Smith",
        given_names="Jane",
        initials=None,
        suffix=None,
        tussenvoegsel=None,
        birth=None,
        death=None,
        type=PersonType.unchecked,
        target=None,
        bio=None,
        ol_id=None,
        naming_convention=NamingConvention.unspecified,
        tags=(
            models.tags.PersonTag.ORCID("0000-0002-1694-233X"),
            models.tags.PersonTag.IgnoreORCIDWork(
                orcid="0000-0002-1694-233X",
                doi="10.1234/example",
                comment="Crossref attributes this work to other authors.",
            ),
        ),
    )

    assert "unchecked but tags set" not in {
        str(issue)
        for issue in person.lint(
            LintConfig(
                autofix=False, interactive=False, available_resources=frozenset()
            )
        )
    }


def test_orcid_profile_accepts_matching_public_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _profile_person("0000-0002-1694-233X")
    monkeypatch.setattr(
        orcid,
        "get_orcid_profile",
        Mock(
            return_value=orcid.OrcidProfile(
                orcid="0000-0002-1694-233X",
                given_names="Jane",
                family_names="Smith",
                credit_name=None,
                other_names=(),
                works=(),
            )
        ),
    )

    assert (
        list(
            lint.orcid_profile.linter(
                person, LintConfig(autofix=False, interactive=False)
            )
        )
        == []
    )


def test_orcid_profile_warns_on_missing_record(monkeypatch: pytest.MonkeyPatch) -> None:
    person = _profile_person("0000-0002-1694-233X")
    monkeypatch.setattr(orcid, "get_orcid_profile", Mock(return_value=None))

    assert list(
        lint.orcid_profile.linter(person, LintConfig(autofix=False, interactive=False))
    ) == ["ORCID 0000-0002-1694-233X has no public record"]


def test_orcid_profile_warns_on_public_name_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _profile_person("0000-0002-1694-233X")
    monkeypatch.setattr(
        orcid,
        "get_orcid_profile",
        Mock(
            return_value=orcid.OrcidProfile(
                orcid="0000-0002-1694-233X",
                given_names="John",
                family_names="Jones",
                credit_name=None,
                other_names=(),
                works=(),
            )
        ),
    )

    assert list(
        lint.orcid_profile.linter(person, LintConfig(autofix=False, interactive=False))
    ) == [
        (
            "ORCID 0000-0002-1694-233X public name 'John Jones' does not match "
            "Person 'Jane Smith'"
        )
    ]


def test_orcid_works_warns_when_known_article_does_not_list_person(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _profile_person("0000-0002-1694-233X")
    article = Mock(spec=models.Article)
    article.id = 10
    article.name = "example-paper"
    article.get_authors.return_value = []
    profile = orcid.OrcidProfile(
        orcid="0000-0002-1694-233X",
        given_names="Jane",
        family_names="Smith",
        credit_name=None,
        other_names=(),
        works=(
            orcid.OrcidWork(
                doi="10.1234/example",
                title="Example",
                journal_title=None,
                publication_year=2024,
                source_name=None,
            ),
        ),
    )
    monkeypatch.setattr(orcid, "get_orcid_profile", Mock(return_value=profile))
    monkeypatch.setattr(
        lint, "_articles_by_doi", Mock(return_value={"10.1234/example": (article,)})
    )

    assert list(
        lint.orcid_works.linter(person, LintConfig(autofix=False, interactive=False))
    ) == [
        (
            "ORCID 0000-0002-1694-233X lists DOI 10.1234/example, but 'Jane Smith' "
            "is not an author of Article 'example-paper'"
        )
    ]


def test_orcid_works_respects_doi_specific_ignore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _profile_person("0000-0002-1694-233X")
    person.tags = (
        *person.tags,
        models.tags.PersonTag.IgnoreORCIDWork(
            orcid="0000-0002-1694-233X",
            doi="https://doi.org/10.1234/EXAMPLE",
            comment="Crossref attributes this work to other authors.",
        ),
    )
    article = Mock(spec=models.Article)
    article.id = 10
    article.name = "example-paper"
    article.get_authors.return_value = []
    profile = orcid.OrcidProfile(
        orcid="0000-0002-1694-233X",
        given_names="Jane",
        family_names="Smith",
        credit_name=None,
        other_names=(),
        works=(
            orcid.OrcidWork(
                doi="10.1234/example",
                title="Example",
                journal_title=None,
                publication_year=2024,
                source_name=None,
            ),
        ),
    )
    monkeypatch.setattr(orcid, "get_orcid_profile", Mock(return_value=profile))
    monkeypatch.setattr(
        lint, "_articles_by_doi", Mock(return_value={"10.1234/example": (article,)})
    )

    assert (
        list(
            lint.orcid_works.linter(
                person, LintConfig(autofix=False, interactive=False)
            )
        )
        == []
    )


def test_orcid_works_suggests_article_author_with_same_orcid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _profile_person("0000-0001-9748-2947")
    person.family_name = "Bennett"
    person.given_names = None
    person.initials = "N.C."
    person.get_full_name.return_value = "N.C. Bennett"
    article_author = _profile_person("0000-0001-9748-2947")
    article_author.id = 2
    article_author.family_name = "Bennett"
    article_author.given_names = "Nigel C."
    article_author.get_full_name.return_value = "Nigel C. Bennett"
    article = Mock(spec=models.Article)
    article.id = 10
    article.name = "example-paper"
    article.get_authors.return_value = [article_author]
    profile = orcid.OrcidProfile(
        orcid="0000-0001-9748-2947",
        given_names="Nigel",
        family_names="Bennett",
        credit_name=None,
        other_names=(),
        works=(
            orcid.OrcidWork(
                doi="10.1234/example",
                title="Example",
                journal_title=None,
                publication_year=2024,
                source_name=None,
            ),
        ),
    )
    monkeypatch.setattr(orcid, "get_orcid_profile", Mock(return_value=profile))
    monkeypatch.setattr(
        lint, "_articles_by_doi", Mock(return_value={"10.1234/example": (article,)})
    )

    assert list(
        lint.orcid_works.linter(person, LintConfig(autofix=False, interactive=False))
    ) == [
        (
            "ORCID 0000-0001-9748-2947 lists DOI 10.1234/example, but "
            "'N.C. Bennett' is not an author of Article 'example-paper'; article "
            "author with the same ORCID: 'Nigel C. Bennett'"
        )
    ]


def test_orcid_works_suggests_article_author_matching_public_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _profile_person("0000-0001-9748-2947")
    person.family_name = "Bennett"
    person.given_names = None
    person.initials = "N.C."
    person.get_full_name.return_value = "N.C. Bennett"
    article_author = _profile_person()
    article_author.id = 2
    article_author.family_name = "Bennett"
    article_author.given_names = "Nigel C."
    article_author.get_full_name.return_value = "Nigel C. Bennett"
    article = Mock(spec=models.Article)
    article.id = 10
    article.name = "example-paper"
    article.get_authors.return_value = [article_author]
    profile = orcid.OrcidProfile(
        orcid="0000-0001-9748-2947",
        given_names="Nigel",
        family_names="Bennett",
        credit_name=None,
        other_names=(),
        works=(
            orcid.OrcidWork(
                doi="10.1234/example",
                title="Example",
                journal_title=None,
                publication_year=2024,
                source_name=None,
            ),
        ),
    )
    monkeypatch.setattr(orcid, "get_orcid_profile", Mock(return_value=profile))
    monkeypatch.setattr(
        lint, "_articles_by_doi", Mock(return_value={"10.1234/example": (article,)})
    )

    assert list(
        lint.orcid_works.linter(person, LintConfig(autofix=False, interactive=False))
    ) == [
        (
            "ORCID 0000-0001-9748-2947 lists DOI 10.1234/example, but "
            "'N.C. Bennett' is not an author of Article 'example-paper'; ORCID "
            "public name matches article author: 'Nigel C. Bennett'"
        )
    ]


def test_redirect_is_never_more_specific_than_canonical_person() -> None:
    canonical = Person.virtual(
        family_name="Esteban", given_names="Graciela", type=PersonType.unchecked
    )
    redirect = Person.virtual(
        family_name="Esteban",
        given_names="Graciela I.",
        type=PersonType.hard_redirect,
        target=canonical,
    )

    assert not is_more_specific_than(redirect, canonical)
    assert is_more_specific_than(canonical, redirect)


def test_redirect_cycle_is_renderable_and_not_autofixed() -> None:
    first = Person.virtual(
        family_name="Lacerda",
        initials="M.",
        type=PersonType.soft_redirect,
        naming_convention=NamingConvention.general,
    )
    second = Person.virtual(
        family_name="Lacerda",
        given_names="Marcel Baêta",
        type=PersonType.hard_redirect,
        naming_convention=NamingConvention.general,
        target=first,
    )
    first.target = second

    assert "target: ..." in repr(first)
    issues = list(first.check_all_fields(LintConfig(autofix=True, interactive=False)))

    assert any(
        "field target points into a redirect cycle" in str(issue) for issue in issues
    )
    assert all(isinstance(issue, str) for issue in issues)
    assert first.target is second
