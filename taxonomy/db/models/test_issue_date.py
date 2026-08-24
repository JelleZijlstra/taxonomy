from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any, cast

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
    return cast(
        IssueDate,
        SimpleNamespace(issue=issue, start_page=start, end_page=end, date=date),
    )


def _install_issues(monkeypatch: pytest.MonkeyPatch, issues: list[IssueDate]) -> None:
    monkeypatch.setattr(
        IssueDate, "select_valid", classmethod(lambda cls: _FakeQuery(issues))
    )
    monkeypatch.setattr(issue_date_module, "_get_cgs_with_issue_dates", lambda: {1})


def test_parse_page_number() -> None:
    assert parse_page_number("160") == (160, False)
    assert parse_page_number("160bis") == (160, True)
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
    first = _issue(issue="1", start="1", end="50", date="1900-01-01")
    second = _issue(issue="2", start="40", end="90", date="1900-02-01")
    _install_issues(monkeypatch, [first, second])

    result = IssueDate.find_matching_issue(
        cast(CitationGroup, SimpleNamespace(id=1)), None, "1", "45", "48", issue="2"
    )

    assert result is second


def test_find_matching_issue_accepts_equivalent_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _issue(issue="1", start="1", end="50", date="1900-01-01")
    duplicate = _issue(issue="1", start="1", end="50", date="1900-01-01")
    _install_issues(monkeypatch, [first, duplicate])

    result = IssueDate.find_matching_issue(
        cast(CitationGroup, SimpleNamespace(id=1)), None, "1", "10", "20"
    )

    assert result is first
