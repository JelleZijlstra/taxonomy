from __future__ import annotations

from collections.abc import Iterable
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from taxonomy.apis import zoobank
from taxonomy.db.constants import ArticleKind
from taxonomy.db.models.article import Article, ArticleTag, PresenceStatus
from taxonomy.db.models.article import lint as article_lint
from taxonomy.db.models.base import LintConfig


def _get_tags(tags: tuple[object, ...], tag_cls: type[object]) -> Iterable[object]:
    return (tag for tag in tags if isinstance(tag, tag_cls))


def test_clear_zoobank_caches_is_adt_callback() -> None:
    article = object.__new__(Article)

    callbacks = article.get_adt_callbacks()

    assert callbacks["clear_zoobank_caches"] == article.clear_zoobank_caches


def test_clear_zoobank_caches_clears_article_lsids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_cache = Mock()
    monkeypatch.setattr(zoobank, "clear_zoobank_publication_cache", clear_cache)
    article = cast(
        Article,
        SimpleNamespace(
            tags=(
                ArticleTag.LSIDArticle(
                    "FC07ACBE-03F7-414A-BB64-1BB0711766BF", PresenceStatus.present
                ),
                ArticleTag.AlternativeURL("https://doi.org/10.5252/g2016n3a3"),
            ),
            get_tags=_get_tags,
        ),
    )

    Article.clear_zoobank_caches(article)

    clear_cache.assert_called_once_with("FC07ACBE-03F7-414A-BB64-1BB0711766BF")


def test_data_from_zoobank_skips_unavailable_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        zoobank,
        "get_zoobank_data_for_article",
        Mock(side_effect=zoobank.ZooBankUnavailableError("unavailable")),
    )
    article = cast(
        Article,
        SimpleNamespace(
            kind=ArticleKind.electronic,
            tags=(
                ArticleTag.LSIDArticle(
                    "FC07ACBE-03F7-414A-BB64-1BB0711766BF", PresenceStatus.present
                ),
            ),
            get_tags=_get_tags,
        ),
    )

    assert (
        list(
            article_lint.data_from_zoobank.linter(
                article, LintConfig(autofix=False, interactive=False)
            )
        )
        == []
    )
