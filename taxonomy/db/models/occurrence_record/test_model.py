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


def test_open_coordinates_falls_back_to_location() -> None:
    location = Mock()
    record = SimpleNamespace(
        tags=(), location=location, get_tags=lambda values, tag_type: ()
    )

    OccurrenceRecord.open_coordinates(record)  # type: ignore[arg-type]

    location.open_coordinates.assert_called_once_with()


def test_occurrence_record_callbacks_include_article_and_related_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article_callback = Mock()
    article = SimpleNamespace(
        get_shareable_adt_callbacks=Mock(
            return_value={"article_callback": article_callback}
        )
    )
    monkeypatch.setattr(
        OccurrenceRecord, "classification_entry", SimpleNamespace(article=article)
    )
    record = object.__new__(OccurrenceRecord)

    callbacks = record.get_adt_callbacks()

    assert callbacks["article_callback"] is article_callback
    assert callbacks["open_coordinates"] == record.open_coordinates
    assert callbacks["display_location"] == record.display_location
    assert callbacks["edit_classification_entry"] == record.edit_classification_entry


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
