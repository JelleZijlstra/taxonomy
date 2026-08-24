from unittest.mock import Mock

import pytest

from taxonomy.db.constants import ArticleType
from taxonomy.db.models.article import Article, lint
from taxonomy.db.models.base import LintConfig


def test_check_start_end_page_allows_bis() -> None:
    citation_group = Mock()
    citation_group.get_tag.return_value = None
    article = Mock(
        type=ArticleType.JOURNAL,
        citation_group=citation_group,
        start_page="91bis",
        end_page="190bis",
    )
    article.is_full_issue.return_value = False
    article.is_in_press.return_value = False

    issues = list(
        lint.check_start_end_page.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == []


def test_internal_date_virtual_update_reads_origin_pdf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    origin = Mock(spec=Article)
    article = Mock(spec=Article, is_virtual=True, virtual_origin=origin, tags=())
    article.get_tags.return_value = ()
    extract = Mock(return_value=())
    monkeypatch.setattr(lint, "get_pdf_publication_date_evidence", extract)

    issues = list(
        lint.infer_internal_publication_date.linter(
            article, LintConfig(autofix=False, interactive=False)
        )
    )

    assert issues == []
    extract.assert_called_once_with(article)
