from unittest.mock import Mock

import pytest

from taxonomy.apis import orcid
from taxonomy.db import models
from taxonomy.db.constants import NamingConvention, PersonType
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.person import (
    Person,
    is_more_specific_than,
    is_valid_orcid,
    lint,
    normalize_orcid,
)
from taxonomy.db.models.person.lint import (
    multiple_orcids,
    redirect_to_less_specific,
    soft_redirect,
)


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


@pytest.mark.parametrize(
    "convention",
    [
        NamingConvention.vietnamese,
        NamingConvention.pinyin,
        NamingConvention.chinese,
        NamingConvention.korean,
    ],
)
def test_naming_convention_warns_without_autofixing(
    monkeypatch: pytest.MonkeyPatch, convention: NamingConvention
) -> None:
    person = Person.virtual(
        family_name="Li",
        given_names="Ming",
        type=PersonType.unchecked,
        naming_convention=NamingConvention.unspecified,
        tags=(),
    )
    other = Person.virtual(
        family_name="Li", type=PersonType.checked, naming_convention=convention
    )
    query = Mock()
    query.filter.return_value = [other]
    monkeypatch.setattr(Person, "select_valid", Mock(return_value=query))
    Person.clear_lint_caches()
    try:
        issues = list(
            lint.naming_convention(person, LintConfig(autofix=True, interactive=False))
        )

        assert len(issues) == 1
        assert issues[0].code == "naming_convention"
        assert (
            "naming convention is unspecified, but family name 'Li' is also used "
            f"with naming conventions: {convention.name}"
        ) in str(issues[0])
        assert issues[0].fix is None
        assert person.naming_convention is NamingConvention.unspecified
    finally:
        Person.clear_lint_caches()


@pytest.mark.parametrize(
    "convention",
    [value for value in NamingConvention if value is not NamingConvention.unspecified],
)
def test_naming_convention_preserves_explicit_conventions(
    monkeypatch: pytest.MonkeyPatch, convention: NamingConvention
) -> None:
    person = Person.virtual(
        family_name="Li",
        given_names="Ming",
        type=PersonType.unchecked,
        naming_convention=convention,
    )
    lookup = Mock(
        side_effect=AssertionError("should not look up an explicit convention")
    )
    monkeypatch.setattr(lint, "_family_name_to_naming_conventions", lookup)

    assert not list(
        lint.naming_convention.linter(
            person, LintConfig(autofix=False, interactive=False)
        )
    )


def test_naming_convention_exact_matches_and_ignored_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        lint,
        "_family_name_to_naming_conventions",
        Mock(return_value={"Li": {NamingConvention.pinyin, NamingConvention.chinese}}),
    )
    person = Person.virtual(
        family_name="Li",
        given_names="Ming",
        type=PersonType.unchecked,
        naming_convention=NamingConvention.unspecified,
        tags=(
            models.tags.PersonTag.IgnoreLint("naming_convention", comment="Reviewed"),
        ),
    )
    cfg = LintConfig(autofix=False, interactive=False)

    assert list(lint.naming_convention.linter(person, cfg)) == [
        (
            "naming convention is unspecified, but family name 'Li' is also used "
            "with naming conventions: chinese, pinyin"
        )
    ]
    assert not list(lint.naming_convention(person, cfg))
    for family_name in ("Liu", "li", "Lí", "Smith"):
        person.family_name = family_name
        assert not list(lint.naming_convention.linter(person, cfg))


@pytest.mark.parametrize("initials", [None, "M."])
def test_naming_convention_skips_names_without_given_names(
    monkeypatch: pytest.MonkeyPatch, initials: str | None
) -> None:
    person = Person.virtual(
        family_name="Li",
        initials=initials,
        type=PersonType.unchecked,
        naming_convention=NamingConvention.unspecified,
    )
    monkeypatch.setattr(
        lint,
        "_family_name_to_naming_conventions",
        Mock(side_effect=AssertionError("should not look up an initials-only name")),
    )

    assert not list(
        lint.naming_convention.linter(
            person, LintConfig(autofix=False, interactive=False)
        )
    )


@pytest.mark.parametrize(
    "person_type",
    [PersonType.deleted, PersonType.hard_redirect, PersonType.soft_redirect],
)
def test_naming_convention_skips_invalid_persons(
    monkeypatch: pytest.MonkeyPatch, person_type: PersonType
) -> None:
    invalid = Person.virtual(
        family_name="Li",
        given_names="Ming",
        type=person_type,
        naming_convention=NamingConvention.pinyin,
    )
    person = Person.virtual(
        family_name="Li",
        given_names="Ming",
        type=PersonType.unchecked,
        naming_convention=NamingConvention.unspecified,
    )
    query = Mock()
    query.filter.return_value = [invalid]
    monkeypatch.setattr(Person, "select_valid", Mock(return_value=query))
    Person.clear_lint_caches()
    cfg = LintConfig(autofix=False, interactive=False)
    try:
        assert not list(lint.naming_convention.linter(person, cfg))
        invalid.naming_convention = NamingConvention.unspecified
        assert not list(lint.naming_convention.linter(invalid, cfg))
    finally:
        Person.clear_lint_caches()


def test_naming_convention_cache_is_cleared_between_lint_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Person.virtual(
        family_name="Li",
        type=PersonType.checked,
        naming_convention=NamingConvention.pinyin,
    )
    query = Mock()
    query.filter.return_value = [source]
    select = Mock(return_value=query)
    monkeypatch.setattr(Person, "select_valid", select)
    Person.clear_lint_caches()
    try:
        assert lint._family_name_to_naming_conventions() == {
            "Li": {NamingConvention.pinyin}
        }
        source.naming_convention = NamingConvention.chinese
        assert lint._family_name_to_naming_conventions() == {
            "Li": {NamingConvention.pinyin}
        }
        select.assert_called_once_with()

        Person.clear_lint_caches()
        assert lint._family_name_to_naming_conventions() == {
            "Li": {NamingConvention.chinese}
        }
        assert select.call_count == 2
    finally:
        Person.clear_lint_caches()


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


def test_orcid_profile_reviewed_redirect_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored_orcid = "0000-0003-1648-6773"
    person = Person.virtual(
        family_name="Le",
        given_names="Son",
        tussenvoegsel="Xuan",
        naming_convention=NamingConvention.vietnamese,
        type=PersonType.unchecked,
        tags=(models.tags.PersonTag.ORCID(stored_orcid),),
    )
    profile = Mock(
        return_value=orcid.OrcidProfile(
            orcid="0000-0001-9273-0040",
            given_names="Anh",
            family_names="Nguyen",
            credit_name="Anh D. Nguyen",
            other_names=(),
            works=(),
        )
    )
    monkeypatch.setattr(orcid, "get_orcid_profile", profile)
    monkeypatch.setattr(lint.LINT, "linters", [lint.orcid_profile])
    cfg = LintConfig(
        autofix=False,
        structured_autofix=True,
        interactive=False,
        available_resources=frozenset(LintResource),
    )

    issues = list(lint.LINT.run(person, cfg))
    assert len(issues) == 2
    assert "redirects to public record 0000-0001-9273-0040" in str(issues[0])
    assert "does not match Person 'Le Xuan Son'" in str(issues[1])

    lint.add_ignore(
        person, "orcid_profile", "Published identifier redirects incorrectly."
    )
    assert not list(lint.LINT.run(person, cfg))
    assert lint.LINT.get_ignored_lints(person) == {"orcid_profile"}

    # If ORCID resolves the conflict, normal lint cleanup removes the exception.
    profile.return_value = orcid.OrcidProfile(
        orcid=stored_orcid,
        given_names="Son Xuan",
        family_names="Le",
        credit_name=None,
        other_names=(),
        works=(),
    )
    assert not list(lint.LINT.run(person, cfg))
    assert person.tags == (models.tags.PersonTag.ORCID(stored_orcid),)


@pytest.mark.parametrize(
    "resources",
    [frozenset(), frozenset({LintResource.NETWORK}), frozenset({LintResource.SLOW})],
)
def test_orcid_profile_exception_survives_unavailable_resources(
    monkeypatch: pytest.MonkeyPatch, resources: frozenset[LintResource]
) -> None:
    person = Person.virtual(
        family_name="Le",
        given_names="Son",
        tussenvoegsel="Xuan",
        naming_convention=NamingConvention.vietnamese,
        type=PersonType.unchecked,
        tags=(
            models.tags.PersonTag.ORCID("0000-0003-1648-6773"),
            models.tags.PersonTag.IgnoreLint(
                "orcid_profile", comment="Reviewed redirect."
            ),
        ),
    )
    lookup = Mock(
        side_effect=AssertionError("profile lookup requires network and slow")
    )
    monkeypatch.setattr(orcid, "get_orcid_profile", lookup)
    monkeypatch.setattr(lint.LINT, "linters", [lint.orcid_profile])

    assert not list(
        lint.LINT.run(
            person,
            LintConfig(
                autofix=False,
                structured_autofix=True,
                interactive=False,
                available_resources=resources,
            ),
        )
    )
    lookup.assert_not_called()
    assert lint.LINT.get_ignored_lints(person) == {"orcid_profile"}


@pytest.mark.parametrize("redirected", [False, True])
def test_orcid_works_warns_when_known_article_does_not_list_person(
    monkeypatch: pytest.MonkeyPatch, *, redirected: bool
) -> None:
    person = _profile_person("0000-0002-1694-233X")
    article = Mock(spec=models.Article)
    article.id = 10
    article.name = "example-paper"
    article.get_authors.return_value = []
    profile = orcid.OrcidProfile(
        orcid="0000-0003-1577-6568" if redirected else "0000-0002-1694-233X",
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


def test_orcid_works_skips_redirect_to_different_person(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _profile_person("0000-0002-1694-233X")
    profile = orcid.OrcidProfile(
        orcid="0000-0003-1577-6568",
        given_names="John",
        family_names="Jones",
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
    articles = Mock(
        side_effect=AssertionError("cannot attribute another person's works")
    )
    monkeypatch.setattr(lint, "_articles_by_doi", articles)

    assert not list(
        lint.orcid_works.linter(person, LintConfig(autofix=False, interactive=False))
    )
    articles.assert_not_called()


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
        family_name="Esteban",
        given_names="Graciela",
        naming_convention=NamingConvention.unspecified,
        type=PersonType.unchecked,
    )
    redirect = Person.virtual(
        family_name="Esteban",
        given_names="Graciela I.",
        naming_convention=NamingConvention.unspecified,
        type=PersonType.hard_redirect,
        target=canonical,
    )

    assert not is_more_specific_than(redirect, canonical)
    assert is_more_specific_than(canonical, redirect)


def test_soft_redirect_is_migrated_to_hard_redirect() -> None:
    canonical = Person.virtual(
        family_name="Esteban",
        given_names="Graciela",
        naming_convention=NamingConvention.unspecified,
        tags=(),
        type=PersonType.unchecked,
    )
    redirect = Person.virtual(
        family_name="Esteban",
        given_names="Graciela I.",
        naming_convention=NamingConvention.unspecified,
        tags=(),
        type=PersonType.soft_redirect,
        target=canonical,
    )
    cfg = LintConfig(autofix=False, structured_autofix=True, interactive=False)

    assert list(soft_redirect(redirect, cfg)) == []
    assert redirect.type is PersonType.hard_redirect


def test_redirect_to_less_specific_name_is_diagnostic() -> None:
    canonical = Person.virtual(
        family_name="Esteban",
        given_names="Graciela",
        naming_convention=NamingConvention.unspecified,
        type=PersonType.unchecked,
    )
    redirect = Person.virtual(
        family_name="Esteban",
        given_names="Graciela I.",
        naming_convention=NamingConvention.unspecified,
        type=PersonType.hard_redirect,
        target=canonical,
    )

    expected = (
        "redirect name 'Graciela I. Esteban' is more specific than target "
        "'Graciela Esteban'"
    )
    assert list(
        redirect_to_less_specific.linter(
            redirect, LintConfig(autofix=True, interactive=False)
        )
    ) == [expected]
    assert redirect.target is canonical


@pytest.mark.parametrize(
    "person_type", [PersonType.hard_redirect, PersonType.soft_redirect]
)
def test_redirect_to_equivalent_form_is_not_reported(person_type: PersonType) -> None:
    canonical = Person.virtual(
        family_name="Dasmahapatra",
        initials="K.K.",
        naming_convention=NamingConvention.unspecified,
        type=PersonType.unchecked,
    )
    redirect = Person.virtual(
        family_name="Dasmahapatra",
        initials="K K.",
        naming_convention=NamingConvention.unspecified,
        type=person_type,
        target=canonical,
    )

    assert not list(
        redirect_to_less_specific.linter(
            redirect, LintConfig(autofix=False, interactive=False)
        )
    )


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
