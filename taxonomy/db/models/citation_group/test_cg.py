from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, call

import pytest

from taxonomy.db import constants
from taxonomy.db.models.citation_group.cg import CitationGroup, CitationGroupStatus


def test_create_interactively_prompts_for_type_and_opens_editor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    getter = Mock()
    getter.get_one_key.return_value = "Journal of Examples"
    get_value_for_field = Mock(side_effect=[None, constants.ArticleType.JOURNAL])
    created = Mock()
    create = Mock(return_value=created)
    lifecycle = Mock()
    lifecycle.attach_mock(created.fill_required_fields, "fill_required_fields")
    lifecycle.attach_mock(created.edit, "edit")
    monkeypatch.setattr(CitationGroup, "getter", Mock(return_value=getter))
    monkeypatch.setattr(
        CitationGroup, "get_value_for_field_on_class", get_value_for_field
    )
    monkeypatch.setattr(CitationGroup, "create", create)

    result = CitationGroup.create_interactively()

    getter.get_one_key.assert_called_once_with("name> ", allow_empty=False)
    assert get_value_for_field.call_args_list == [call("type"), call("type")]
    create.assert_called_once_with(
        name="Journal of Examples", type=constants.ArticleType.JOURNAL
    )
    assert lifecycle.mock_calls == [call.fill_required_fields(), call.edit()]
    assert result is created


def test_create_interactively_uses_provided_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_value_for_field = Mock()
    created = Mock()
    create = Mock(return_value=created)
    monkeypatch.setattr(
        CitationGroup, "get_value_for_field_on_class", get_value_for_field
    )
    monkeypatch.setattr(CitationGroup, "create", create)

    result = CitationGroup.create_interactively(
        "Example City", type=constants.ArticleType.BOOK
    )

    get_value_for_field.assert_not_called()
    create.assert_called_once_with(name="Example City", type=constants.ArticleType.BOOK)
    created.fill_required_fields.assert_called_once_with()
    created.edit.assert_called_once_with()
    assert result is created


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
