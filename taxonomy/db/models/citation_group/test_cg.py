from types import SimpleNamespace
from typing import cast

import pytest

from taxonomy.db.models.citation_group.cg import CitationGroup, CitationGroupStatus


def test_delete_rejects_any_valid_direct_backreference() -> None:
    field = SimpleNamespace(
        model_cls=SimpleNamespace(__name__="Book"), name="citation_group_id"
    )
    book = SimpleNamespace(id=1)
    group = cast(
        CitationGroup,
        SimpleNamespace(get_direct_backrefs=lambda: iter(((field, book),))),
    )

    with pytest.raises(AssertionError, match=r"Book\.citation_group_id"):
        CitationGroup.delete(group)


def test_delete_without_direct_backreferences() -> None:
    group = cast(
        CitationGroup,
        SimpleNamespace(
            status=CitationGroupStatus.normal, get_direct_backrefs=lambda: iter(())
        ),
    )

    CitationGroup.delete(group)

    assert group.status is CitationGroupStatus.deleted
