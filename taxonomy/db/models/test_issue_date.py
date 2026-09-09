from collections.abc import Iterator
from typing import Any

import pytest

from taxonomy.db.models import issue_date as issue_date_module
from taxonomy.db.models.citation_group import CitationGroup
from taxonomy.db.models.issue_date import (
    IssueDate,
    page_range_contains,
    parse_page_number,
)


class _FakeQuery:
    def __init__(self, values: list[Any]) -> None:
        self.values = values

    def filter(self, *args: object, **kwargs: object) -> _FakeQuery:
        return self

    def __iter__(self) -> Iterator[Any]:
        return iter(self.values)


def _issue(*, issue: str, start: str, end: str, date: str) -> IssueDate:
    return IssueDate.virtual(
        citation_group=CitationGroup.virtual(name="Example journal"),
        volume="1",
        issue=issue,
        start_page=start,
        end_page=end,
        date=date,
    )


def _install_issues(
    monkeypatch: pytest.MonkeyPatch,
    citation_group: CitationGroup,
    issues: list[IssueDate],
) -> None:
    monkeypatch.setattr(
        IssueDate, "select_valid", classmethod(lambda cls: _FakeQuery(issues))
    )
    monkeypatch.setattr(
        issue_date_module, "_get_cgs_with_issue_dates", lambda: {citation_group.id}
    )


def test_parse_page_number() -> None:
    assert parse_page_number("160") == (160, ("", "arabic", False))
    assert parse_page_number("160bis") == (160, ("", "arabic", True))
    assert parse_page_number("160 bis") is None
    assert parse_page_number("bis160") is None


def test_page_range_contains_requires_same_variant() -> None:
    assert page_range_contains("91", "190", "160", "165")
    assert page_range_contains("91bis", "190bis", "160bis", "165bis")
    assert not page_range_contains("91", "190", "160bis", "165bis")
    assert not page_range_contains("91bis", "190bis", "160", "165")


def test_find_matching_issue_uses_article_issue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    citation_group = CitationGroup.virtual(name="Example journal")
    first = _issue(issue="1", start="1", end="50", date="1900-01-01")
    second = _issue(issue="2", start="40", end="90", date="1900-02-01")
    _install_issues(monkeypatch, citation_group, [first, second])

    result = IssueDate.find_matching_issue(
        citation_group, None, "1", "45", "48", issue="2"
    )

    assert result is second


def test_find_matching_issue_accepts_equivalent_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    citation_group = CitationGroup.virtual(name="Example journal")
    first = _issue(issue="1", start="1", end="50", date="1900-01-01")
    duplicate = _issue(issue="1", start="1", end="50", date="1900-01-01")
    _install_issues(monkeypatch, citation_group, [first, duplicate])

    result = IssueDate.find_matching_issue(citation_group, None, "1", "10", "20")

    assert result is first


@pytest.mark.parametrize(
    ("outer_start", "outer_end", "inner_start", "inner_end", "matches"),
    [
        ("01", "0100", "02", "010", True),
        ("01", "0100", "2", "10", False),
        ("1", "100", "02", "010", False),
        ("I", "XVI", "ii", "x", True),
        ("I", "XVI", "2", "10", False),
        (
            "compte-rendu:1",
            "compte-rendu:100",
            "compte-rendu:2",
            "compte-rendu:10",
            True,
        ),
        ("compte-rendu:1", "compte-rendu:100", "2", "10", False),
        ("compte-rendu:1", "compte-rendu:100", "index:2", "index:10", False),
        ("1", "100", "10", "2", False),
        ("01", "0100", "02", "10", False),
    ],
)
def test_pagination_sequences(
    outer_start: str, outer_end: str, inner_start: str, inner_end: str, *, matches: bool
) -> None:
    assert (
        page_range_contains(outer_start, outer_end, inner_start, inner_end) is matches
    )


@pytest.mark.parametrize(
    "page",
    [
        "",
        "iiii",
        "IC",
        "iiix",
        "1:1",
        ":1",
        "section:",
        "section:other:1",
        "1-2",
        "1 bis",
    ],
)
def test_invalid_page_number(page: str) -> None:
    assert parse_page_number(page) is None


def test_calendar_tag_and_inference(monkeypatch: pytest.MonkeyPatch) -> None:
    from taxonomy.db.constants import ArticleType, Calendar
    from taxonomy.db.models.article import Article
    from taxonomy.db.models.article.lint import infer_publication_date_from_issue_date
    from taxonomy.db.models.issue_date import IssueDateTag

    cg = CitationGroup.virtual(name="Example journal")
    issue = _issue(issue="3", start="267", end="349", date="1910-09")
    issue.citation_group = cg
    issue.volume = "15"
    issue.tags = (IssueDateTag.Calendar(Calendar.julian),)  # type: ignore[assignment]
    _install_issues(monkeypatch, cg, [issue])
    art = Article.virtual(
        type=ArticleType.JOURNAL,
        citation_group=cg,
        volume="15",
        start_page="318",
        end_page="333",
    )
    assert infer_publication_date_from_issue_date(art) == ("1910-10-13", "3")
    assert issue.date == "1910-09"
    assert "julian; adopted Gregorian 1910-10-13" in str(issue)
    assert IssueDateTag.unserialize(
        IssueDateTag.Calendar(Calendar.julian).serialize()
    ) == IssueDateTag.Calendar(Calendar.julian)


def test_conflicting_calendars_are_not_equivalent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taxonomy.db.constants import Calendar
    from taxonomy.db.models.issue_date import IssueDateTag

    cg = CitationGroup.virtual(name="Example journal")
    first = _issue(issue="1", start="1", end="50", date="1910-09")
    second = _issue(issue="1", start="1", end="50", date="1910-09")
    second.tags = (IssueDateTag.Calendar(Calendar.julian),)  # type: ignore[assignment]
    _install_issues(monkeypatch, cg, [first, second])
    assert isinstance(IssueDate.find_matching_issue(cg, None, "1", "10", "20"), str)
    second.tags += (IssueDateTag.Calendar(Calendar.gregorian),)
    with pytest.raises(ValueError, match="Conflicting Calendar"):
        second.get_gregorian_date()


def test_calendar_validation_and_pagination_lint() -> None:
    from taxonomy.db.constants import Calendar
    from taxonomy.db.models.base import LintConfig
    from taxonomy.db.models.issue_date import IssueDateTag

    issue = _issue(issue="1", start="01", end="010", date="1900-02-29")
    cfg = LintConfig(autofix=False, interactive=False)
    assert any("Invalid date" in str(result) for result in issue.lint(cfg))
    issue.tags = (IssueDateTag.Calendar(Calendar.julian),)  # type: ignore[assignment]
    assert list(issue.lint(cfg)) == []
    issue.end_page = "10"
    assert any(
        "inconsistent page-number variants" in str(result) for result in issue.lint(cfg)
    )


def test_inference_distinguishes_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    from taxonomy.db.constants import ArticleType
    from taxonomy.db.models.article import Article
    from taxonomy.db.models.article.lint import infer_publication_date_from_issue_date

    cg = CitationGroup.virtual(name="Example journal")
    ordinary = _issue(issue="1", start="1", end="20", date="1910-01")
    report = _issue(issue="2", start="01", end="020", date="1910-02")
    roman = _issue(issue="3", start="i", end="xx", date="1910-03")
    qualified = _issue(issue="4", start="report:1", end="report:20", date="1910-04")
    _install_issues(monkeypatch, cg, [ordinary, report, roman, qualified])
    for page, expected in [
        ("10", ("1910-01", "1")),
        ("010", ("1910-02", "2")),
        ("X", ("1910-03", "3")),
        ("report:10", ("1910-04", "4")),
    ]:
        art = Article.virtual(
            type=ArticleType.JOURNAL,
            citation_group=cg,
            volume="1",
            start_page=page,
            end_page=page,
        )
        assert infer_publication_date_from_issue_date(art) == expected
