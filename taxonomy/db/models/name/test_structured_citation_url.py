from types import SimpleNamespace
from typing import Any, cast

import pytest

from taxonomy.db.constants import ArticleType
from taxonomy.db.models import Name
from taxonomy.db.models.citation_group import lint as cg_lint
from taxonomy.db.models.name import TypeTag

from . import lint, parse_citations


def test_parsed_citation_merge_preserves_url(monkeypatch: pytest.MonkeyPatch) -> None:
    existing = TypeTag.StructuredVerbatimCitation(
        volume="1", url="https://example.com/work"
    )
    name = cast(
        Name,
        SimpleNamespace(
            verbatim_citation="Journal 1: 2-3",
            citation_group=None,
            get_type_tag=lambda _tag_type: existing,
        ),
    )
    monkeypatch.setattr(
        parse_citations,
        "parse_citation",
        lambda _text: SimpleNamespace(
            series=None, volume="1", issue=None, start_page="2", end_page="3"
        ),
    )
    monkeypatch.setattr(
        lint, "replace_tag_issue", lambda _message, _name, _old, new, **_kwargs: new
    )

    assert list(
        lint.add_structured_verbatim_citation.linter(name, cast(Any, None))
    ) == [
        TypeTag.StructuredVerbatimCitation(
            volume="1", start_page="2", end_page="3", url="https://example.com/work"
        )
    ]


def test_field_normalization_preserves_url(monkeypatch: pytest.MonkeyPatch) -> None:
    tag = TypeTag.StructuredVerbatimCitation(
        volume="1–2", url="https://example.com/work"
    )
    citation_group = SimpleNamespace(type=ArticleType.JOURNAL)
    name = cast(
        Name,
        SimpleNamespace(
            get_citation_group=lambda: citation_group,
            get_tags=lambda _tags, _tag_type: iter((tag,)),
            type_tags=(tag,),
        ),
    )
    monkeypatch.setattr(cg_lint, "get_series_regex", lambda _cg: None)
    monkeypatch.setattr(cg_lint, "get_volume_regex", lambda _cg: r"\d+-\d+")
    monkeypatch.setattr(cg_lint, "describe_volume_regex", lambda _cg: "numeric range")
    monkeypatch.setattr(
        lint, "replace_tag_issue", lambda _message, _name, _old, new, **_kwargs: new
    )

    assert list(
        lint.check_structured_verbatim_citation_fields.linter(name, cast(Any, None))
    ) == [
        TypeTag.StructuredVerbatimCitation(volume="1-2", url="https://example.com/work")
    ]
