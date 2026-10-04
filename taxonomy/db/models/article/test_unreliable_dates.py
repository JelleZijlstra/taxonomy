from unittest.mock import Mock

import pytest

from taxonomy.db.constants import ArticleType, DateSource
from taxonomy.db.models.article import Article, ArticleTag, add_data, api_data, lint
from taxonomy.db.models.article.article import PresenceStatus
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.citation_group import CitationGroup, CitationGroupTag
from taxonomy.db.models.citation_group import lint as cg_lint
from taxonomy.db.models.citation_group.date_sources import get_unreliable_date_sources


@pytest.fixture(autouse=True)
def prevent_saves(monkeypatch: pytest.MonkeyPatch) -> None:
    def save(art: Article) -> None:
        assert art.is_virtual

    monkeypatch.setattr(Article, "save", save)


def group(*rules: CitationGroupTag) -> CitationGroup:
    return CitationGroup.virtual(
        name="Example journal", type=ArticleType.JOURNAL, tags=rules
    )


def rule(
    source: DateSource = DateSource.doi_published_print, **kwargs: str
) -> CitationGroupTag:
    return CitationGroupTag.UnreliablePublicationDate(
        source, "Publisher supplies a volume placeholder.", **kwargs
    )


def date(source: DateSource, value: str) -> ArticleTag:
    return ArticleTag.PublicationDate(source, value)


def article(cg: CitationGroup, *tags: ArticleTag, **kwargs: str) -> Article:
    return Article.virtual(
        name="Example paper",
        type=ArticleType.JOURNAL,
        citation_group=cg,
        tags=tags,
        **kwargs,
    )


def test_filter_keeps_raw_tags_and_trusted_date() -> None:
    art = article(
        group(rule()),
        date(DateSource.doi_published_print, "2015-12"),
        date(DateSource.doi_published, "2015-03-04"),
        year="2015-12",
    )
    before = art.tags
    assert lint.infer_publication_date(art) == ("2015-03-04", None, [])
    assert art.tags == before
    assert art.year == "2015-12"


@pytest.mark.parametrize(
    "source", [DateSource.decision, DateSource.internal, DateSource.external]
)
def test_rejected_bounds_do_not_constrain_trusted_evidence(source: DateSource) -> None:
    art = article(
        group(rule()),
        date(DateSource.doi_published_print, ">2099"),
        date(source, "1971"),
    )
    assert lint.infer_publication_date(art) == ("1971", None, [])


def test_no_supported_date_preserves_existing_value_and_reports_reason() -> None:
    art = article(
        group(rule()), date(DateSource.doi_published_print, "2015-12"), year="2015-12"
    )
    assert lint.infer_publication_date(art) == (None, None, [])
    issues = list(lint.check_unreliable_publication_date.linter(art, LintConfig()))
    assert len(issues) == 1 and "doi_published_print" in str(issues[0])
    assert art.year == "2015-12"


def test_online_eligibility_is_not_relaxed() -> None:
    art = article(
        group(rule(), rule(DateSource.doi_published)),
        date(DateSource.doi_published_print, "2023-01"),
        date(DateSource.doi_published_online, "2023-11-27"),
    )
    assert lint.infer_publication_date(art) == (None, None, [])
    art.tags = (*art.tags, ArticleTag.LSID("test", PresenceStatus.present))  # type: ignore[assignment]
    assert lint.infer_publication_date(art) == ("2023-11-27", None, [])


@pytest.mark.parametrize(
    ("series", "volume", "issue", "blocked"),
    [
        ("7", "21", "3", True),
        ("7", "121", "3", False),
        ("6", None, None, False),
        ("7", None, "3", True),
        (None, "21", "3", True),
        ("7", None, "4", False),
    ],
)
def test_scope_and_missing_metadata(
    series: str | None, volume: str | None, issue: str | None, *, blocked: bool
) -> None:
    cg = group(rule(series="7", volume_regex="2[0-9]", issue_regex="3"))
    result = get_unreliable_date_sources(cg, series=series, volume=volume, issue=issue)
    assert bool(result) == blocked
    if blocked and (series is None or volume is None):
        assert "scope unresolved" in result[DateSource.doi_published_print]


def test_empty_series_matches_absence() -> None:
    cg = group(rule(series=""))
    assert get_unreliable_date_sources(cg)
    assert not get_unreliable_date_sources(cg, series="7")


def test_issue_date_and_parent_cannot_reintroduce_rejected_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = article(
        group(rule()), date(DateSource.doi_published_print, "1900"), year="1900"
    )
    child = Article.virtual(
        name="Chapter", type=ArticleType.CHAPTER, parent=parent, tags=()
    )
    assert lint.infer_publication_date(child) == (None, None, [])
    monkeypatch.setattr(
        lint, "infer_publication_date_from_issue_date", lambda art: ("1901-03", "2")
    )
    assert lint.infer_publication_date(parent) == ("1901-03", "2", [])


def test_citation_group_validation_and_repeatability() -> None:
    invalid = group(
        rule(DateSource.internal),
        rule(volume_regex="["),
        CitationGroupTag.UnreliablePublicationDate(DateSource.jstor, ""),
    )
    issues = list(cg_lint.check_tags.linter(invalid, LintConfig()))
    assert len(issues) == 3
    assert not get_unreliable_date_sources(group(rule(DateSource.internal)))
    valid = group(rule(), rule(DateSource.jstor))
    assert not list(cg_lint.check_tags.linter(valid, LintConfig()))
    assert not any(
        "multiple" in str(i) for i in cg_lint.format_tags.linter(valid, LintConfig())
    )
    tag = valid.tags[0]
    assert CitationGroupTag.unserialize(tag.serialize()) == tag


def test_doi_import_filters_resolved_group_and_defers_unknown_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_data,
        "get_doi_json",
        lambda doi: {
            "message": {
                "type": "journal-article",
                "container-title": ["Example journal"],
                "volume": "21",
                "published-print": {"date-parts": [[2023, 1]]},
                "published": {"date-parts": [[2023, 5, 7]]},
            }
        },
    )
    monkeypatch.setattr(CitationGroup, "select_one", Mock(return_value=group(rule())))
    data = api_data.expand_doi_json("10.example/test")
    assert data["year"] == "2023-05-07"
    assert date(DateSource.doi_published_print, "2023-01") in data["tags"]
    monkeypatch.setattr(CitationGroup, "select_one", Mock(return_value=None))
    assert "year" not in api_data.expand_doi_json("10.example/test")


@pytest.mark.parametrize("method", ["add_data", "article"])
def test_import_rechecks_actual_group_after_resolving_it(
    method: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    cg = group(rule())
    monkeypatch.setattr(CitationGroup, "get_or_prompt", Mock(return_value=cg))
    art = Article.virtual(name="Import", type=ArticleType.JOURNAL, tags=())
    data = {
        "year": "2015-12",
        "journal": "Example journal",
        "tags": [
            date(DateSource.doi_published_print, "2015-12"),
            date(DateSource.doi_published, "2015-03-04"),
        ],
    }
    if method == "add_data":
        add_data.set_multi(art, data, only_new=False)
    else:
        art.set_multi(data)
    assert art.year == "2015-03-04"
    assert set(art.tags) == set(data["tags"])


def test_import_does_not_clear_existing_date_when_all_sources_rejected() -> None:
    art = article(group(rule()), year="1971")
    add_data.set_multi(
        art,
        {"year": "2008", "tags": [date(DateSource.doi_published_print, "2008")]},
        only_new=False,
    )
    assert art.year == "1971"


@pytest.mark.parametrize("method", ["add_data", "article"])
def test_import_preserves_explicit_decisions(method: str) -> None:
    art = article(
        group(rule()), date(DateSource.decision, "2015-06-01"), year="2015-06-01"
    )
    data = {
        "year": "2015-12",
        "tags": [
            date(DateSource.doi_published_print, "2015-12"),
            date(DateSource.doi_published, "2015-03-04"),
        ],
    }
    if method == "add_data":
        add_data.set_multi(art, data, only_new=False)
    else:
        art.set_multi(data)
    assert art.year == "2015-06-01"
    assert date(DateSource.decision, "2015-06-01") in art.tags


def test_import_without_a_resolved_journal_does_not_adopt_date() -> None:
    art = Article.virtual(name="Unresolved import", type=ArticleType.JOURNAL, tags=())
    add_data.set_multi(
        art,
        {"year": "2015-12", "tags": [date(DateSource.doi_published_print, "2015-12")]},
        only_new=False,
    )
    assert art.year is None
    assert art.tags == (date(DateSource.doi_published_print, "2015-12"),)


def test_neighbor_inference_rejects_untrusted_siblings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cg = group(rule(DateSource.jstor))
    target = article(cg, year="1930", volume="69", start_page="100")
    before = article(
        cg,
        date(DateSource.jstor, "1930-01"),
        year="1930-01",
        volume="69",
        start_page="90",
    )
    after = article(
        cg,
        date(DateSource.jstor, "1930-01"),
        year="1930-01",
        volume="69",
        start_page="110",
    )
    query = Mock()
    query.filter.return_value = [before, after]
    monkeypatch.setattr(Article, "select_valid", Mock(return_value=query))
    assert lint.get_inferred_date_from_position(target) is None
    for sibling in [before, after]:
        sibling.tags = (*sibling.tags, date(DateSource.decision, "1930-01"))  # type: ignore[assignment]
    assert lint.get_inferred_date_from_position(target) == (before, after)
