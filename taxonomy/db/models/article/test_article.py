from __future__ import annotations

from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from taxonomy.apis import zoobank
from taxonomy.db.constants import ArticleKind, ArticleType
from taxonomy.db.models.article import Article, ArticleTag, PresenceStatus, api_data
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


@pytest.mark.parametrize(
    ("linter", "expand", "tag", "source"),
    [
        (article_lint.data_from_pmc.linter, "expand_pmc_json", ArticleTag.PMC, "PMC"),
        (
            article_lint.data_from_pubmed.linter,
            "expand_pubmed_json",
            ArticleTag.PMID,
            "PubMed",
        ),
    ],
)
def test_identifier_data_stops_at_doi_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    linter: Callable[[Article, LintConfig], Iterable[str]],
    expand: str,
    tag: type[object],
    source: str,
) -> None:
    monkeypatch.setattr(
        api_data,
        expand,
        Mock(
            return_value={
                "doi": "10.1093/jmammal/gyy181",
                "title": (
                    "Erratum: Gray wolf mortality patterns in Wisconsin from 1979 to 2012."
                ),
                "tags": [ArticleTag.PMID("30837778")],
            }
        ),
    )
    article = cast(
        Article,
        SimpleNamespace(
            doi="10.1093/jmammal/gyv133",
            kind=ArticleKind.electronic,
            get_identifier=lambda tag_cls: "PMC6394112" if tag_cls is tag else None,
        ),
    )

    assert list(linter(article, LintConfig())) == [
        f"DOI mismatch: 10.1093/jmammal/gyy181 ({source}) vs. "
        "10.1093/jmammal/gyv133 (article)"
    ]


def test_pmc_inference_does_not_fall_back_from_doi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idconv = Mock(return_value=None)
    europe_pmc_doi = Mock(return_value=None)
    metadata = Mock(return_value="PMC6394112")
    monkeypatch.setattr(api_data, "get_pmcid_from_idconv", idconv)
    monkeypatch.setattr(api_data, "get_pmcid_from_doi_via_europe_pmc", europe_pmc_doi)
    monkeypatch.setattr(api_data, "get_pmcid_from_metadata", metadata)
    article = cast(
        Article,
        SimpleNamespace(
            id=26_476,
            title="Erratum",
            year="2015-09-29",
            doi="10.1093/jmammal/gyv133",
            kind=ArticleKind.electronic,
            type=ArticleType.JOURNAL,
            citation_group=SimpleNamespace(
                name="Journal of Mammalogy",
                may_have_article_identifier=Mock(return_value=True),
            ),
            get_identifier=lambda tag_cls: (
                "30837778" if tag_cls is ArticleTag.PMID else None
            ),
            numeric_year=lambda: 2015,
        ),
    )

    assert list(article_lint.infer_pmc.linter(article, LintConfig())) == []
    idconv.assert_called_once_with("10.1093/jmammal/gyv133")
    europe_pmc_doi.assert_called_once_with("10.1093/jmammal/gyv133")
    metadata.assert_not_called()
