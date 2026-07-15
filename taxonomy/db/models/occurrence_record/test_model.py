from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from .model import OccurrenceRecord


def test_edit_opens_tags_only() -> None:
    record = SimpleNamespace(fill_field=Mock())

    OccurrenceRecord.edit(record)  # type: ignore[arg-type]

    record.fill_field.assert_called_once_with("tags")
