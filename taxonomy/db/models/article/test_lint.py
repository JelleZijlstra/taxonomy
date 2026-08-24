from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from taxonomy.db.constants import ArticleKind, ArticleType, DateSource
from taxonomy.db.models.article import Article, ArticleTag, api_data, lint
from taxonomy.db.models.article.publication_date import PublicationDateEvidence
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint_types import LintIssue


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
