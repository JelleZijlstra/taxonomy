from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from taxonomy.apis import orcid
from taxonomy.db import models
from taxonomy.db.constants import (
    ArticleKind,
    ArticleType,
    Calendar,
    DateSource,
    NamingConvention,
    PersonType,
)
from taxonomy.db.models.article import Article, ArticleTag, api_data, lint
from taxonomy.db.models.article.publication_date import PublicationDateEvidence
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint_types import LintIssue
from taxonomy.db.models.person import Person, VirtualPerson


def test_publication_date_calendar_serialization() -> None:
    legacy = ArticleTag.unserialize(
        [14, DateSource.internal.value, "1910-09", "source"]
    )
    assert isinstance(legacy, ArticleTag.PublicationDate)
    assert legacy.calendar is None
    assert legacy.serialize() == [14, DateSource.internal.value, "1910-09", "source"]
    tag = ArticleTag.PublicationDate(
        DateSource.internal, "1910-09", comment="source", calendar=Calendar.julian
    )
    assert ArticleTag.unserialize(tag.serialize()) == tag
    assert tag.serialize()[-1] == Calendar.julian.value


@pytest.mark.parametrize(
    ("date", "calendar", "expected"),
    [
        ("1913-12", Calendar.julian, "1914-01-13"),
        ("12-Brumaire", Calendar.french_republican, "1803-11-22"),
        ("1805-1807", None, "1805-1807"),
        ("1910-09", Calendar.gregorian, "1910-09"),
    ],
)
def test_infer_publication_date_calendar(
    date: str, calendar: Calendar | None, expected: str
) -> None:
    tag = ArticleTag.PublicationDate(DateSource.internal, date, calendar=calendar)
    assert lint.infer_publication_date_from_tags([tag]) == (expected, [])


def test_publication_date_conflicts_use_gregorian() -> None:
    julian = ArticleTag.PublicationDate(
        DateSource.internal, "1910-09", calendar=Calendar.julian
    )
    gregorian = ArticleTag.PublicationDate(DateSource.internal, "1910-10-13")
    assert lint.infer_publication_date_from_tags([julian, gregorian]) == (
        "1910-10-13",
        [],
    )
    different = ArticleTag.PublicationDate(DateSource.internal, "1910-09-01")
    result, errors = lint.infer_publication_date_from_tags([julian, different])
    assert result is None
    assert "multiple tags" in errors[0]


def test_dual_dated_imprint_intersects_calendars() -> None:
    tags = [
        ArticleTag.PublicationDate(
            DateSource.internal, "14", calendar=Calendar.french_republican
        ),
        ArticleTag.PublicationDate(DateSource.internal, "1805"),
    ]
    assert lint.infer_publication_date_from_tags(tags) == ("1805-12-31", [])


def test_invalid_publication_date_calendar() -> None:
    tag = ArticleTag.PublicationDate(
        DateSource.internal, "2-Complémentaires-6", calendar=Calendar.french_republican
    )
    result, errors = lint.infer_publication_date_from_tags([tag])
    assert result is None
    assert "Invalid date" in errors[0]


@pytest.mark.parametrize(
    ("day", "calendar", "expected"),
    [
        ("1910-09-01", Calendar.julian, "1910-09-14"),
        ("1910-10-01", Calendar.gregorian, "1910-10-01"),
    ],
)
def test_more_precise_calendar_date_refines_month(
    day: str, calendar: Calendar, expected: str
) -> None:
    tags = [
        ArticleTag.PublicationDate(
            DateSource.internal, "1910-09", calendar=Calendar.julian
        ),
        ArticleTag.PublicationDate(DateSource.internal, day, calendar=calendar),
    ]
    assert lint.infer_publication_date_from_tags(tags) == (expected, [])


def test_article_inference_returns_gregorian_without_mutating_source() -> None:
    tag = ArticleTag.PublicationDate(
        DateSource.internal, "12-Brumaire", calendar=Calendar.french_republican
    )
    article = Article.virtual(
        name="Test calendar", type=ArticleType.BOOK, year="1803", tags=(tag,)
    )
    assert lint.infer_publication_date(article) == ("1803-11-22", None, [])
    assert article.year == "1803"
    assert article.tags == (tag,)


def test_custom_calendar_tag_validation() -> None:
    tag = ArticleTag.PublicationDate(
        DateSource.internal, "2-Complémentaires-6", calendar=Calendar.french_republican
    )
    art = Article.virtual(name="Invalid calendar", tags=(tag,))
    issues = list(lint.check_tags.linter(art, LintConfig(autofix=False)))
    assert any("invalid PublicationDate" in str(issue) for issue in issues)


def test_pdf_date_comparison_requires_calendar_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tag = ArticleTag.PublicationDate(
        DateSource.internal, "1910-09", calendar=Calendar.julian
    )
    article = Article.virtual(name="Test calendar", tags=(tag,))
    monkeypatch.setattr(
        lint,
        "get_pdf_publication_date_evidence",
        lambda art: (_pdf_date("1910-09-30"),),
    )
    assert (
        list(
            lint.internal_publication_date_matches_pdf.linter(
                article, LintConfig(autofix=False)
            )
        )
        == []
    )


def _pdf_date(date: str) -> PublicationDateEvidence:
    return PublicationDateEvidence(
        date=date,
        page_number=1,
        matched_text=f"Published {date}",
        context=f"Publication date: {date}",
        kind="explicit publication statement",
    )


def test_data_from_doi_adds_publication_dates_to_alternative_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publication_date = ArticleTag.PublicationDate(
        DateSource.doi_published_online, "2024-05-06"
    )
    article = Mock(doi="10.1234/example", kind=ArticleKind.alternative_version, tags=())
    article.has_tag.return_value = False
    monkeypatch.setattr(
        api_data, "expand_doi_json", Mock(return_value={"tags": [publication_date]})
    )

    issues = list(
        lint.data_from_doi.linter(article, LintConfig(autofix=False, interactive=False))
    )

    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert str(publication_date) in issues[0].message
    assert issues[0].fix is not None


def test_data_from_doi_still_skips_general_doi(monkeypatch: pytest.MonkeyPatch) -> None:
    article = Mock(
        doi="10.1234/container",
        kind=ArticleKind.alternative_version,
        tags=(ArticleTag.GeneralDOI(),),
    )
    article.has_tag.side_effect = lambda tag_type: tag_type is ArticleTag.GeneralDOI
    expand = Mock()
    monkeypatch.setattr(api_data, "expand_doi_json", expand)

    issues = list(
        lint.data_from_doi.linter(article, LintConfig(autofix=False, interactive=False))
    )

    assert issues == []
    expand.assert_not_called()


def _person_with_orcids(*orcids: str) -> Mock:
    person = Mock(spec=Person)
    person.family_name = "Smith"
    person.given_names = "Jane"
    person.initials = None
    person.tussenvoegsel = None
    person.suffix = None
    person.naming_convention = NamingConvention.english
    person.tags = tuple(models.tags.PersonTag.ORCID(orcid) for orcid in orcids)
    person.get_tags.side_effect = lambda tags, tag_type: (
        tag for tag in tags if isinstance(tag, tag_type)
    )
    person.get_full_name.return_value = "Jane Smith"
    person.resolve_redirect.return_value = person
    return person


def _article_with_author(person: Person) -> Mock:
    article = Mock(spec=Article)
    article.doi = "10.1234/example"
    article.tags = ()
    article.get_authors.return_value = [person]
    article.has_tag.return_value = False
    return article


def test_infer_author_orcids_adds_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    person = _person_with_orcids()
    article = _article_with_author(person)
    monkeypatch.setattr(
        api_data,
        "get_doi_authors",
        Mock(
            return_value=[
                api_data.DoiAuthor(
                    person=VirtualPerson(family_name="Smith", given_names="Jane"),
                    orcid="0000-0002-1694-233X",
                    authenticated_orcid=True,
                )
            ]
        ),
    )

    issues = list(
        lint.infer_author_orcids.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert "authenticated ORCID" in issues[0].message
    assert issues[0].fix is not None


def test_infer_author_orcids_accepts_existing_matching_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orcid = "0000-0002-1694-233X"
    person = _person_with_orcids(orcid)
    article = _article_with_author(person)
    monkeypatch.setattr(
        api_data,
        "get_doi_authors",
        Mock(
            return_value=[
                api_data.DoiAuthor(
                    person=VirtualPerson(family_name="Smith", given_names="Jane"),
                    orcid=orcid,
                    authenticated_orcid=True,
                )
            ]
        ),
    )

    assert (
        list(
            lint.infer_author_orcids.linter(
                article, LintConfig(autofix=False, interactive=False)
            )
        )
        == []
    )


def test_infer_author_orcids_warns_on_conflicting_existing_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _person_with_orcids("0000-0003-1577-6568")
    article = _article_with_author(person)
    monkeypatch.setattr(
        api_data,
        "get_doi_authors",
        Mock(
            return_value=[
                api_data.DoiAuthor(
                    person=VirtualPerson(family_name="Smith", given_names="Jane"),
                    orcid="0000-0002-1694-233X",
                    authenticated_orcid=False,
                )
            ]
        ),
    )

    issues = list(
        lint.infer_author_orcids.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert isinstance(issues[0], str)
    assert "already has ORCID tags" in issues[0]


def test_infer_author_orcids_from_orcid_adds_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _person_with_orcids()
    article = _article_with_author(person)
    monkeypatch.setattr(
        orcid,
        "search_orcids_by_doi",
        Mock(
            return_value=[
                orcid.OrcidSearchResult(
                    orcid="0000-0002-1694-233X",
                    given_names="Jane",
                    family_names="Smith",
                    credit_name=None,
                    other_names=(),
                    institution_names=(),
                )
            ]
        ),
    )

    issues = list(
        lint.infer_author_orcids_from_orcid.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert "public ORCID work DOI" in issues[0].message
    assert issues[0].fix is not None


def test_infer_author_orcids_from_orcid_warns_on_unmatched_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = _article_with_author(_person_with_orcids())
    monkeypatch.setattr(
        orcid,
        "search_orcids_by_doi",
        Mock(
            return_value=[
                orcid.OrcidSearchResult(
                    orcid="0000-0002-1694-233X",
                    given_names="John",
                    family_names="Jones",
                    credit_name=None,
                    other_names=(),
                    institution_names=(),
                )
            ]
        ),
    )

    issues = list(
        lint.infer_author_orcids_from_orcid.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == [
        (
            "ORCID DOI author does not match an article author: "
            "John Jones (0000-0002-1694-233X; given names 'John'; family name "
            "'Jones'); closest article author: 'Jane Smith' (given names 'Jane'; "
            "family name 'Smith')"
        )
    ]


def test_infer_author_orcids_from_orcid_skips_reviewed_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = _article_with_author(_person_with_orcids())
    article.tags = (
        ArticleTag.IgnoreORCIDProfile(
            "0000-0002-1694-233X", comment="The profile is not an author."
        ),
    )
    monkeypatch.setattr(
        orcid,
        "search_orcids_by_doi",
        Mock(
            return_value=[
                orcid.OrcidSearchResult(
                    orcid="0000-0002-1694-233X",
                    given_names="John",
                    family_names="Jones",
                    credit_name=None,
                    other_names=(),
                    institution_names=(),
                )
            ]
        ),
    )

    assert (
        list(
            lint.infer_author_orcids_from_orcid.linter(
                article, LintConfig(autofix=False, interactive=False)
            )
        )
        == []
    )


def test_infer_author_orcids_from_orcid_accepts_all_stored_duplicate_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orcid_ids = ("0000-0002-1694-233X", "0000-0001-5109-3700")
    article = _article_with_author(_person_with_orcids(*orcid_ids))
    monkeypatch.setattr(
        orcid,
        "search_orcids_by_doi",
        Mock(
            return_value=[
                orcid.OrcidSearchResult(
                    orcid=orcid_id,
                    given_names="Jane",
                    family_names="Smith",
                    credit_name=None,
                    other_names=(),
                    institution_names=(),
                )
                for orcid_id in orcid_ids
            ]
        ),
    )

    assert (
        list(
            lint.infer_author_orcids_from_orcid.linter(
                article, LintConfig(autofix=False, interactive=False)
            )
        )
        == []
    )


def test_infer_author_orcids_from_orcid_shows_closest_reversed_author(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    likely_author = _person_with_orcids()
    likely_author.family_name = "de los Reyes"
    likely_author.given_names = "Martín L."
    likely_author.naming_convention = NamingConvention.spanish
    likely_author.get_full_name.return_value = "Martín L. de los Reyes"
    other_author = _person_with_orcids()
    other_author.family_name = "Poiré"
    other_author.given_names = "Daniel"
    other_author.get_full_name.return_value = "Daniel Poiré"
    article = _article_with_author(likely_author)
    article.get_authors.return_value = [likely_author, other_author]
    monkeypatch.setattr(
        orcid,
        "search_orcids_by_doi",
        Mock(
            return_value=[
                orcid.OrcidSearchResult(
                    orcid="0000-0002-2438-7161",
                    given_names="De los Reyes",
                    family_names="Martin",
                    credit_name=None,
                    other_names=(),
                    institution_names=(),
                )
            ]
        ),
    )

    issues = list(
        lint.infer_author_orcids_from_orcid.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert "closest article author: 'Martín L. de los Reyes'" in str(issues[0])
    assert "given names 'Martín L.'; family name 'de los Reyes'" in str(issues[0])


def test_infer_author_orcids_from_orcid_warns_on_existing_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _person_with_orcids("0000-0003-1577-6568")
    article = _article_with_author(person)
    monkeypatch.setattr(
        orcid,
        "search_orcids_by_doi",
        Mock(
            return_value=[
                orcid.OrcidSearchResult(
                    orcid="0000-0002-1694-233X",
                    given_names="Jane",
                    family_names="Smith",
                    credit_name=None,
                    other_names=(),
                    institution_names=(),
                )
            ]
        ),
    )

    issues = list(
        lint.infer_author_orcids_from_orcid.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert isinstance(issues[0], str)
    assert "already has ORCID tags" in issues[0]


def test_infer_author_orcids_from_orcid_prefers_stored_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identified = _person_with_orcids("0000-0002-1694-233X")
    identified.family_name = "Szczygielski"
    identified.given_names = "Tomasz"
    identified.get_full_name.return_value = "Tomasz Szczygielski"
    similar = _person_with_orcids()
    similar.family_name = "Sulej"
    similar.given_names = "Tomasz"
    similar.get_full_name.return_value = "Tomasz Sulej"
    article = _article_with_author(identified)
    article.get_authors.return_value = [identified, similar]
    monkeypatch.setattr(
        orcid,
        "search_orcids_by_doi",
        Mock(
            return_value=[
                orcid.OrcidSearchResult(
                    orcid="0000-0002-1694-233X",
                    given_names="Tomasz",
                    family_names="Szczygielski",
                    credit_name=None,
                    other_names=("Tomasz Sulej",),
                    institution_names=(),
                )
            ]
        ),
    )

    assert (
        list(
            lint.infer_author_orcids_from_orcid.linter(
                article, LintConfig(autofix=False, interactive=False)
            )
        )
        == []
    )


def test_data_from_orcid_warns_on_consistent_metadata_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = _article_with_author(_person_with_orcids("0000-0002-1694-233X"))
    article.title = "Article title"
    article.kind = ArticleKind.electronic
    article.valid_numeric_year.return_value = 2023
    result = orcid.OrcidSearchResult(
        orcid="0000-0002-1694-233X",
        given_names="Jane",
        family_names="Smith",
        credit_name=None,
        other_names=(),
        institution_names=(),
    )
    profile = orcid.OrcidProfile(
        orcid=result.orcid,
        given_names="Jane",
        family_names="Smith",
        credit_name=None,
        other_names=(),
        works=(
            orcid.OrcidWork(
                doi="10.1234/example",
                title="Different title",
                journal_title="Journal",
                publication_year=2024,
                source_name="Crossref",
            ),
        ),
    )
    monkeypatch.setattr(orcid, "get_orcid_profile", Mock(return_value=profile))

    issues = list(
        lint.data_from_orcid.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == [
        (
            "title mismatch: ['Different title'] (ORCID records "
            "['0000-0002-1694-233X'], sources ['Crossref']) vs. Article title "
            "(article)"
        ),
        (
            "year mismatch: 2024 (ORCID records ['0000-0002-1694-233X'], "
            "sources ['Crossref']) vs. 2023 (article)"
        ),
    ]


def test_data_from_orcid_accepts_translated_title(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = _article_with_author(_person_with_orcids("0000-0002-1694-233X"))
    article.title = "Status of *Ndamathaia* and carnivores"
    profile = orcid.OrcidProfile(
        orcid="0000-0002-1694-233X",
        given_names="Jane",
        family_names="Smith",
        credit_name=None,
        other_names=(),
        works=(
            orcid.OrcidWork(
                doi="10.1234/example",
                title="Status of Nadamathaia and canivores",
                journal_title="Journal",
                publication_year=None,
                source_name="Scopus",
                translated_title="Status of Ndamathaia and carnivores",
            ),
        ),
    )
    monkeypatch.setattr(orcid, "get_orcid_profile", Mock(return_value=profile))

    issues = list(
        lint.data_from_orcid.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == []


def test_infer_author_orcids_warns_on_author_sequence_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _person_with_orcids()
    article = _article_with_author(person)
    monkeypatch.setattr(
        api_data,
        "get_doi_authors",
        Mock(
            return_value=[
                api_data.DoiAuthor(
                    person=VirtualPerson(family_name="Jones", given_names="Jane"),
                    orcid="0000-0002-1694-233X",
                    authenticated_orcid=True,
                )
            ]
        ),
    )

    issues = list(
        lint.infer_author_orcids.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == [
        (
            "DOI author mismatch blocks ORCID inference: position 1: "
            "DOI 'Jane Jones', article 'Jane Smith'"
        )
    ]


def test_infer_author_orcids_accepts_name_particles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _person_with_orcids()
    person.family_name = "Carvalho"
    person.given_names = "Ismar de Souza"
    person.get_full_name.return_value = "Ismar de Souza Carvalho"
    article = _article_with_author(person)
    monkeypatch.setattr(
        api_data,
        "get_doi_authors",
        Mock(
            return_value=[
                api_data.DoiAuthor(
                    person=VirtualPerson(
                        family_name="Carvalho", given_names="Ismar De Souza"
                    ),
                    orcid="0000-0002-1694-233X",
                    authenticated_orcid=True,
                )
            ]
        ),
    )

    issues = list(
        lint.infer_author_orcids.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)


def test_doi_author_match_accepts_tussenvoegsel() -> None:
    person = _person_with_orcids()
    person.family_name = "Geer"
    person.given_names = "Alexandra"
    person.tussenvoegsel = "van der"

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="van der Geer", given_names="Alexandra"), person
    )


def test_doi_author_match_accepts_transliterated_family_name() -> None:
    person = _person_with_orcids()
    person.family_name = "Аверьянов"
    person.given_names = "Александр Олегович"
    person.naming_convention = NamingConvention.russian
    person.tags = (models.tags.PersonTag.TransliteratedFamilyName("Averianov"),)
    person.get_transliterated_family_name.return_value = "Averianov"

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Averianov", given_names="Alexander O."), person
    )


def test_doi_author_match_accepts_omitted_second_family_name() -> None:
    person = _person_with_orcids()
    person.family_name = "Cadena Rueda"
    person.given_names = "Edwin Alberto"
    person.naming_convention = NamingConvention.spanish

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Cadena", given_names="Edwin-Alberto"), person
    )


def test_doi_author_match_accepts_suffix_in_family_name() -> None:
    person = _person_with_orcids()
    person.family_name = "Wood"
    person.given_names = "Perry L."
    person.suffix = "Jr."

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Wood Jr.", given_names="Perry L."), person
    )


def test_doi_author_match_accepts_vietnamese_field_partition() -> None:
    person = _person_with_orcids()
    person.family_name = "Nguyen"
    person.given_names = "Truong"
    person.tussenvoegsel = "Quang"
    person.naming_convention = NamingConvention.vietnamese

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Nguyen", given_names="Truong Quang"), person
    )


def test_doi_author_match_accepts_native_order_vietnamese_partition() -> None:
    person = _person_with_orcids()
    person.family_name = "Nguyen"
    person.given_names = "Son"
    person.tussenvoegsel = "Truong"
    person.naming_convention = NamingConvention.vietnamese

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Son", given_names="Nguyen Truong"), person
    )


def test_doi_author_match_accepts_adjacent_initial_and_name() -> None:
    person = _person_with_orcids()
    person.family_name = "Gilbert"
    person.given_names = "M. Thomas P."

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Gilbert", given_names="M.Thomas P."), person
    )


def test_doi_author_match_accepts_omitted_leading_given_name() -> None:
    person = _person_with_orcids()
    person.family_name = "Feijó"
    person.given_names = "José Anderson"

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Feijó", given_names="Anderson"), person
    )


def test_doi_author_match_accepts_pinyin_lyu_variant() -> None:
    person = _person_with_orcids()
    person.family_name = "Lü"
    person.given_names = "Zhi-tong"
    person.naming_convention = NamingConvention.pinyin

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Lyu", given_names="Zhi-Tong"), person
    )


def test_doi_author_match_accepts_pinyin_field_reversal() -> None:
    person = _person_with_orcids()
    person.family_name = "Li"
    person.given_names = "Chun"
    person.naming_convention = NamingConvention.pinyin

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Chun", given_names="Li"), person
    )


def test_doi_author_match_accepts_suffix_parsed_as_family_name() -> None:
    person = _person_with_orcids()
    person.family_name = "Persons"
    person.given_names = "W. Scott"
    person.suffix = "IV"

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Iv", given_names="W. Scott Persons"), person
    )


def test_doi_author_match_accepts_indivisible_full_name() -> None:
    person = _person_with_orcids()
    person.family_name = "Du Sar No"
    person.given_names = None

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="No", given_names="Du Sar"), person
    )


def test_doi_author_match_accepts_burmese_initials() -> None:
    person = _person_with_orcids()
    person.family_name = "Aung Naing Soe"
    person.given_names = None
    person.naming_convention = NamingConvention.burmese

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Soe", initials="A.N."), person
    )


def test_doi_author_match_ignores_spanish_given_name_particles() -> None:
    person = _person_with_orcids()
    person.family_name = "Álvarez-Sierra"
    person.given_names = "María de los Ángeles"
    person.naming_convention = NamingConvention.spanish

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Álvarez-Sierra", given_names="María A."), person
    )


def test_doi_author_match_accepts_abbreviated_repartitioned_name() -> None:
    person = _person_with_orcids()
    person.family_name = "Veerappan"
    person.given_names = "Deepak"

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Deepak", initials="V."), person
    )


def test_doi_author_match_accepts_compacted_repartitioned_initials() -> None:
    person = _person_with_orcids()
    person.family_name = "Srikanthan"
    person.given_names = "Achyuthan Needamangalam"

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Achyuthan", initials="N.S."), person
    )


def test_doi_author_match_accepts_dropped_family_name_particle() -> None:
    person = _person_with_orcids()
    person.family_name = "Macedo de Farias"
    person.given_names = "Brodsky Dantas"

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Macedo Farias", given_names="Brodsky Dantas"), person
    )


def test_doi_author_match_accepts_reversal_with_reviewed_convention() -> None:
    person = _person_with_orcids()
    person.family_name = "Antoine"
    person.given_names = "Pierre-Olivier"
    person.naming_convention = NamingConvention.general

    assert lint._doi_author_matches_person(
        VirtualPerson(family_name="Pierre-Olivier", given_names="Antoine"), person
    )


def test_doi_author_match_rejects_reversed_unchecked_name() -> None:
    person = _person_with_orcids()
    person.family_name = "Igor"
    person.given_names = "Berkunsky"
    person.naming_convention = NamingConvention.unspecified

    assert not lint._doi_author_matches_person(
        VirtualPerson(family_name="Berkunsky", given_names="Igor"), person
    )


def test_doi_author_identity_accepts_existing_matching_orcid() -> None:
    person = _person_with_orcids("0000-0002-1694-233X")
    doi_author = api_data.DoiAuthor(
        person=VirtualPerson(family_name="Typo", given_names="Different"),
        orcid="0000-0002-1694-233X",
        authenticated_orcid=True,
    )

    assert lint._doi_author_identity_matches(doi_author, person)


def test_resolve_doi_author_person_uses_redirect_target() -> None:
    canonical = Person.virtual(
        family_name="Esteban", given_names="Graciela", type=PersonType.unchecked
    )
    redirect = Person.virtual(
        family_name="Esteban",
        given_names="Graciela I.",
        type=PersonType.hard_redirect,
        target=canonical,
    )
    doi_author = Mock(spec=VirtualPerson)
    doi_author.create_person.return_value = redirect

    assert lint._resolve_doi_author_person(doi_author, canonical) is canonical


def test_infer_author_orcids_warns_on_author_count_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = _person_with_orcids()
    article = _article_with_author(person)
    monkeypatch.setattr(
        api_data,
        "get_doi_authors",
        Mock(
            return_value=[
                api_data.DoiAuthor(
                    person=VirtualPerson(family_name="Smith", given_names="Jane"),
                    orcid="0000-0002-1694-233X",
                    authenticated_orcid=True,
                ),
                api_data.DoiAuthor(
                    person=VirtualPerson(family_name="Jones", given_names="John"),
                    orcid=None,
                    authenticated_orcid=False,
                ),
            ]
        ),
    )

    issues = list(
        lint.infer_author_orcids.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == [
        "DOI author mismatch blocks ORCID inference: DOI has 2 authors, article has 1"
    ]


def test_infer_internal_publication_date_adds_tag_without_reading_year(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = Mock(spec=Article, tags=(), title="Example")
    article.get_tags.return_value = ()
    monkeypatch.setattr(
        lint,
        "get_pdf_publication_date_evidence",
        Mock(return_value=(_pdf_date("1994-06-17"),)),
    )

    issues = list(
        lint.infer_internal_publication_date.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert "1994-06-17" in issues[0].message
    assert issues[0].fix is not None


def test_infer_internal_publication_date_ignores_external_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    external = ArticleTag.PublicationDate(DateSource.external, "1994-06")
    article = Mock(spec=Article, tags=(external,), title="Example")
    article.get_tags.return_value = (external,)
    monkeypatch.setattr(
        lint,
        "get_pdf_publication_date_evidence",
        Mock(return_value=(_pdf_date("1994-06-17"),)),
    )

    issues = list(
        lint.infer_internal_publication_date.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert "1994-06-17" in str(issues[0])


def test_infer_internal_publication_date_does_not_choose_between_pdf_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = Mock(spec=Article, tags=(), title="Example")
    article.get_tags.return_value = ()
    monkeypatch.setattr(
        lint,
        "get_pdf_publication_date_evidence",
        Mock(return_value=(_pdf_date("1994-06-17"), _pdf_date("1994-07-01"))),
    )

    issues = list(
        lint.infer_internal_publication_date.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == []


def test_internal_publication_date_requires_exact_pdf_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    internal = ArticleTag.PublicationDate(DateSource.internal, "1994-06")
    article = Mock(spec=Article, tags=(internal,), title="Example")
    article.get_tags.return_value = (internal,)
    monkeypatch.setattr(
        lint,
        "get_pdf_publication_date_evidence",
        Mock(return_value=(_pdf_date("1994-06-17"),)),
    )

    issues = list(
        lint.internal_publication_date_matches_pdf.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert isinstance(issues[0], str)
    assert "does not exactly match" in issues[0]
    assert "1994-06-17" in issues[0]


def test_internal_publication_date_accepts_exact_pdf_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    internal = ArticleTag.PublicationDate(DateSource.internal, "1994-06-17")
    article = Mock(spec=Article, tags=(internal,), title="Example")
    article.get_tags.return_value = (internal,)
    monkeypatch.setattr(
        lint,
        "get_pdf_publication_date_evidence",
        Mock(return_value=(_pdf_date("1994-06-17"),)),
    )

    issues = list(
        lint.internal_publication_date_matches_pdf.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == []


def test_internal_publication_date_ignores_noninternal_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    external = ArticleTag.PublicationDate(DateSource.external, "1994-06")
    article = Mock(spec=Article, tags=(external,), title="Example")
    article.get_tags.return_value = (external,)
    extract = Mock(return_value=(_pdf_date("1994-06-17"),))
    monkeypatch.setattr(lint, "get_pdf_publication_date_evidence", extract)

    issues = list(
        lint.internal_publication_date_matches_pdf.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == []
    extract.assert_not_called()


def test_supported_parent_date_supports_chapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        lint, "get_inferred_date_from_position", Mock(return_value=None)
    )
    monkeypatch.setattr(
        lint, "infer_publication_date_from_issue_date", Mock(return_value=None)
    )
    parent = SimpleNamespace(
        year="2024-05-06",
        type=ArticleType.BOOK,
        kind=ArticleKind.electronic,
        parent=None,
        has_tag=lambda tag_type: tag_type is ArticleTag.PublicationDate,
    )
    chapter = SimpleNamespace(
        year="2024-05-06",
        type=ArticleType.CHAPTER,
        kind=ArticleKind.electronic,
        parent=parent,
        has_tag=lambda tag_type: False,
    )

    assert not lint.has_unsupported_publication_date(cast(Article, chapter))


def test_unsupported_parent_date_does_not_support_chapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        lint, "get_inferred_date_from_position", Mock(return_value=None)
    )
    monkeypatch.setattr(
        lint, "infer_publication_date_from_issue_date", Mock(return_value=None)
    )
    parent = SimpleNamespace(
        year="2024-05-06",
        type=ArticleType.BOOK,
        kind=ArticleKind.electronic,
        parent=None,
        has_tag=lambda tag_type: False,
    )
    chapter = SimpleNamespace(
        year="2024-05-06",
        type=ArticleType.CHAPTER,
        kind=ArticleKind.electronic,
        parent=parent,
        has_tag=lambda tag_type: False,
    )

    assert lint.has_unsupported_publication_date(cast(Article, chapter))


def test_supported_parent_date_supports_alternative_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        lint, "get_inferred_date_from_position", Mock(return_value=None)
    )
    monkeypatch.setattr(
        lint, "infer_publication_date_from_issue_date", Mock(return_value=None)
    )
    parent = SimpleNamespace(
        year="2024-05-06",
        type=ArticleType.JOURNAL,
        kind=ArticleKind.electronic,
        parent=None,
        has_tag=lambda tag_type: tag_type is ArticleTag.PublicationDate,
    )
    alternative = SimpleNamespace(
        year="2024-05-06",
        type=ArticleType.JOURNAL,
        kind=ArticleKind.alternative_version,
        parent=parent,
        has_tag=lambda tag_type: False,
    )

    assert not lint.has_unsupported_publication_date(cast(Article, alternative))


def test_supported_parent_date_does_not_support_part(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        lint, "infer_publication_date_from_issue_date", Mock(return_value=None)
    )
    parent = SimpleNamespace(
        year="2024-05-06",
        type=ArticleType.JOURNAL,
        kind=ArticleKind.electronic,
        parent=None,
        has_tag=lambda tag_type: tag_type is ArticleTag.PublicationDate,
    )
    part = SimpleNamespace(
        year="2024-05-06",
        type=ArticleType.JOURNAL,
        kind=ArticleKind.part,
        parent=parent,
        has_tag=lambda tag_type: False,
    )

    assert lint.has_unsupported_publication_date(cast(Article, part))


def test_infer_lsid_virtual_update_reads_origin_pdf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    origin = Mock(spec=Article)
    origin.get_all_pdf_pages.return_value = []
    article = Mock(
        spec=Article,
        is_virtual=True,
        virtual_origin=origin,
        tags=(),
        title="An Article",
    )
    article.numeric_year.return_value = 2024
    article.get_tags.return_value = ()
    article.get_new_names.return_value = ()
    article.get_all_pdf_pages.side_effect = AssertionError(
        "virtual Article PDF should not be read directly"
    )
    extract = Mock(return_value=None)
    monkeypatch.setattr(lint, "extract_safe_publication_lsid", extract)

    issues = list(
        lint.infer_lsid_from_names.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == []
    origin.get_all_pdf_pages.assert_called_once_with()
    extract.assert_called_once_with([], "An Article")
