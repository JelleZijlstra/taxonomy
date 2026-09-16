import pytest

from taxonomy.db.constants import ArticleKind, ArticleType, Calendar, DateSource
from taxonomy.db.models.article import Article, ArticleTag, lint
from taxonomy.db.models.article.date_bounds import parse_date
from taxonomy.db.models.base import LintConfig


def _date(date: str, source: DateSource = DateSource.external) -> ArticleTag:
    return ArticleTag.PublicationDate(source, date)


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (["<1799-04-14"], "1799-04-14"),
        (["<1799"], "1799"),
        (["<1799-04"], "1799-04"),
        (["<1799-04-14", "<1799-10-06"], "1799-04-14"),
        (["<1799-10-06", "<1799-04-14"], "1799-04-14"),
        ([">1798-07", "<1799-04-14"], "1799-04-14"),
        ([">1798-07"], None),
        ([">1797", ">1798"], None),
        (["1799", "<1799-04-14"], "1799-04-14"),
        (["1799-01-01", "<1799-04-14"], "1799-01-01"),
        (["1797-1800", "<1799-04-14"], "1799-04-14"),
        (["1799", ">1799-04-14"], "1799"),
        # Same-day order is unknown; do not fabricate a preceding day.
        (["1799-04-14", "<1799-04-14"], "1799-04-14"),
    ],
)
def test_infer_bounded_dates(evidence: list[str], expected: str | None) -> None:
    assert lint.infer_publication_date_from_tags([_date(d) for d in evidence]) == (
        expected,
        [],
    )


@pytest.mark.parametrize(
    "evidence", [[">1800", "<1799"], ["1799-05", "<1799-04-14"], ["1799", ">1800"]]
)
def test_contradictory_bounds(evidence: list[str]) -> None:
    date, errors = lint.infer_publication_date_from_tags([_date(d) for d in evidence])
    assert date is None
    assert len(errors) == 1
    assert "contradictory publication dates" in errors[0]


def test_bounds_across_sources_and_existing_priority() -> None:
    assert lint.infer_publication_date_from_tags(
        [_date("<1799-10-06"), _date("<1799-04-14", DateSource.internal)]
    ) == ("1799-04-14", [])
    # An external cutoff overrides an unreliable later title-page date.
    assert lint.infer_publication_date_from_tags(
        [_date("<1799-04-14"), _date("1800", DateSource.internal)]
    ) == ("1799-04-14", [])
    date, errors = lint.infer_publication_date_from_tags(
        [_date("<1799"), _date(">1800", DateSource.internal)]
    )
    assert date is None and errors


def test_bounds_convert_source_calendar() -> None:
    tag = ArticleTag.PublicationDate(
        DateSource.external, "<1910-09-01", calendar=Calendar.julian
    )
    assert lint.infer_publication_date_from_tags([tag, _date(">1910-09-10")]) == (
        "1910-09-14",
        [],
    )
    republican = ArticleTag.PublicationDate(
        DateSource.external, "<7", calendar=Calendar.french_republican
    )
    assert lint.infer_publication_date_from_tags([republican]) == ("1799-09-22", [])
    assert tag.date == "<1910-09-01"


@pytest.mark.parametrize(
    "date",
    ["<", ">", "<<1799", ">=1799", "<1799-02-30", "<1800-1799", "<undated", "<1799-13"],
)
def test_invalid_bound(date: str) -> None:
    with pytest.raises(ValueError, match="Invalid date"):
        parse_date(date)
    adopted, errors = lint.infer_publication_date_from_tags([_date(date)])
    assert adopted is None and errors


def _article(
    name: str, year: str | None = None, tags: tuple[ArticleTag, ...] = ()
) -> Article:
    return Article.virtual(
        name=name,
        year=year,
        tags=tags,
        type=ArticleType.BOOK,
        kind=ArticleKind.no_copy,
        parent=None,
        doi=None,
        issue=None,
    )


def test_published_before_uses_inference_and_updates_transitively() -> None:
    witness = _article("Bechstein", "1800", (_date("1799-04-14"),))
    middle = _article(
        "Audebert 2",
        "1799-12-02",
        (ArticleTag.PublishedBefore(witness, "Cited on p. 190"),),
    )
    earliest = _article(
        "Earlier work", tags=(ArticleTag.PublishedBefore(middle, "Cited"),)
    )
    assert lint.infer_publication_date(earliest) == ("1799-04-14", None, [])
    witness.tags = (_date("1798-07"),)  # type: ignore[assignment]
    assert lint.infer_publication_date(earliest) == ("1798-07", None, [])
    assert middle.year == "1799-12-02"


def test_published_before_falls_back_to_stored_year() -> None:
    witness = _article("Witness", "1799")
    article = _article("Earlier", tags=(ArticleTag.PublishedBefore(witness, "Cited"),))
    assert lint.infer_publication_date(article) == ("1799", None, [])


def test_published_before_multiple_witnesses_and_literal_bound() -> None:
    early, late = _article("Early", "1798-07"), _article("Late", "1799")
    article = _article(
        "Earlier",
        tags=(
            ArticleTag.PublishedBefore(late, "Cited"),
            ArticleTag.PublishedBefore(early, "Cited"),
            _date("<1798-06"),
        ),
    )
    assert lint.infer_publication_date(article) == ("1798-06", None, [])


def test_published_before_conflict_reaches_year_lint() -> None:
    witness = _article("Witness", "1799")
    article = _article(
        "Later", "1800", (_date(">1800"), ArticleTag.PublishedBefore(witness, "Cited"))
    )
    issues = list(
        lint.check_year(article, LintConfig(autofix=False, interactive=False))
    )
    assert any("contradictory publication dates" in str(issue) for issue in issues)


@pytest.mark.parametrize("self_reference", [True, False])
def test_published_before_cycles(*, self_reference: bool) -> None:
    first, second = _article("First", "1798"), _article("Second", "1799")
    first.tags = (  # type: ignore[assignment]
        ArticleTag.PublishedBefore(first if self_reference else second, "Cited"),
    )
    second.tags = (ArticleTag.PublishedBefore(first, "Cited"),)  # type: ignore[assignment]
    date, _, errors = lint.infer_publication_date(first)
    assert date is None
    assert any("circular publication-date reference" in error for error in errors)


def test_cycle_through_parent_inheritance() -> None:
    parent = _article("Parent", "1799")
    child = _article("Chapter", "1799")
    child.type, child.parent = ArticleType.CHAPTER, parent
    parent.tags = (ArticleTag.PublishedBefore(child, "Circular"),)  # type: ignore[assignment]
    assert (
        "circular publication-date reference"
        in lint.infer_publication_date(parent)[2][0]
    )


@pytest.mark.parametrize("year", [None, "undated"])
def test_published_before_undated_witness(year: str | None) -> None:
    witness = _article("Undated", year)
    article = _article("Earlier", tags=(ArticleTag.PublishedBefore(witness, "Cited"),))
    date, _, errors = lint.infer_publication_date(article)
    assert date is None and errors


def test_published_before_does_not_use_lower_bound_as_upper() -> None:
    witness = _article("After 1799", "1799", (_date(">1799"),))
    article = _article("Earlier", tags=(ArticleTag.PublishedBefore(witness, "Cited"),))
    assert "no upper publication date" in lint.infer_publication_date(article)[2][0]


def test_inherited_date_must_satisfy_bounds() -> None:
    parent = _article("Parent", "1800")
    child = _article("Chapter", tags=(_date("<1799"),))
    child.type, child.parent = ArticleType.CHAPTER, parent
    assert "contradictory publication dates" in lint.infer_publication_date(child)[2][0]


def test_published_before_tag_serialization_and_supported_year() -> None:
    tag = ArticleTag.PublishedBefore(Article(38388), "Cited on p. 190")
    assert tag.serialize() == [35, 38388, "Cited on p. 190"]
    assert ArticleTag.unserialize(tag.serialize()) == tag
    article = _article("Earlier", "1799-04-14", (tag,))
    assert not lint.has_unsupported_publication_date(article)


def test_lower_bound_reports_inconsistent_stored_year() -> None:
    article = _article("Too early", "1798", (_date(">1799"),))
    issues = list(
        lint.check_year(article, LintConfig(autofix=False, interactive=False))
    )
    assert any("precedes publication lower bound" in str(issue) for issue in issues)


def test_month_is_more_precise_than_range_with_same_upper_day() -> None:
    assert lint.infer_publication_date_from_tags(
        [_date("1798-1799"), _date("1799-12"), _date("<1800")]
    ) == ("1799-12", [])


def test_issue_date_is_checked_against_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    article = _article("Journal article", tags=(_date("<1799"),))
    monkeypatch.setattr(
        lint, "infer_publication_date_from_issue_date", lambda _: ("1800", "2")
    )
    date, issue, errors = lint.infer_publication_date(article)
    assert date is None and issue == "2"
    assert "contradictory publication dates" in errors[0]


def test_inherited_upper_bound_can_be_tightened() -> None:
    parent = _article("Parent", "1800", (_date("<1800"),))
    child = _article("Chapter", tags=(_date("<1799"),))
    child.type, child.parent = ArticleType.CHAPTER, parent
    assert lint.infer_publication_date(child) == ("1799", None, [])


def test_inherited_lower_bound_remains_unbounded() -> None:
    parent = _article("Parent", "1800", (_date(">1799"),))
    child = _article("Chapter")
    child.type, child.parent = ArticleType.CHAPTER, parent
    assert lint.infer_publication_date(child) == (None, None, [])
    child.year = "1798"
    issues = list(lint.check_year(child, LintConfig(autofix=False, interactive=False)))
    assert any("precedes publication lower bound" in str(issue) for issue in issues)
    child.tags = (_date("<1798"),)  # type: ignore[assignment]
    assert "contradictory publication dates" in lint.infer_publication_date(child)[2][0]


def test_lower_only_source_does_not_hide_next_source_upper_date() -> None:
    assert lint.infer_publication_date_from_tags(
        [_date(">1798"), _date("1799", DateSource.internal)]
    ) == ("1799", [])


def test_published_before_overrides_later_internal_imprint() -> None:
    witness = _article("Witness", "1799")
    article = _article(
        "Earlier",
        tags=(
            _date("1800", DateSource.internal),
            ArticleTag.PublishedBefore(witness, "Cited"),
        ),
    )
    assert lint.infer_publication_date(article) == ("1799", None, [])


def test_invalid_ordinary_date_is_a_lint_error() -> None:
    date, errors = lint.infer_publication_date_from_tags([_date("1799-02-30")])
    assert date is None and errors
