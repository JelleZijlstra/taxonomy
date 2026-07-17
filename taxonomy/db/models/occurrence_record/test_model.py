from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from taxonomy.db import models
from taxonomy.db.models.classification_entry import ClassificationEntry
from taxonomy.db.models.location import Location

from .model import OccurrenceRecord


def test_edit_opens_tags_only() -> None:
    record = SimpleNamespace(fill_field=Mock())

    OccurrenceRecord.edit(record)  # type: ignore[arg-type]

    record.fill_field.assert_called_once_with("tags")


def test_call_sign_getter_uses_configured_field() -> None:
    assert OccurrenceRecord.get_call_sign_getter() is OccurrenceRecord.getter(None)
    assert Location.get_call_sign_getter() is Location.getter(Location.label_field)


def test_classification_entry_add_occurrence_record_opens_editor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = Mock()
    create_interactively = Mock(return_value=record)
    monkeypatch.setattr(
        models.OccurrenceRecord, "create_interactively", create_interactively
    )
    entry = cast(ClassificationEntry, object())

    result = ClassificationEntry.add_occurrence_record(entry)

    assert result is record
    create_interactively.assert_called_once_with(classification_entry=entry)
    record.edit.assert_called_once_with()  # type: ignore[attr-defined]
