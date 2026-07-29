from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from data_import import lib, location_file, occurrence_import
from scripts import import_ce_file


def test_import_and_lint_all_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    article = SimpleNamespace(id=1, name="source.pdf")
    entries: list[lib.CEDict] = [{"article": article}]  # type: ignore[typeddict-item]
    add_entries = MagicMock(return_value=iter(entries))
    format_ces = MagicMock()
    location_plan = MagicMock()
    locations = {"locality": object()}
    apply_locations = MagicMock(return_value=locations)
    add_occurrences = MagicMock()
    monkeypatch.setattr(lib, "add_classification_entries", add_entries)
    monkeypatch.setattr(lib, "format_ces_in_article", format_ces)
    monkeypatch.setattr(location_file, "apply_plan", apply_locations)
    monkeypatch.setattr(occurrence_import, "add_occurrence_records", add_occurrences)

    import_ce_file.import_and_lint(entries, verbose=True, location_plan=location_plan)

    add_entries.assert_called_once_with(
        entries, dry_run=False, strict=True, verbose=True
    )
    format_ces.assert_called_once_with(article)
    apply_locations.assert_called_once_with(location_plan)
    add_occurrences.assert_called_once_with(entries, locations)


def test_import_and_lint_groups_entries_by_article(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = SimpleNamespace(id=1, name="first.pdf")
    second = SimpleNamespace(id=2, name="second.pdf")
    first_entry_1: lib.CEDict = {"article": first}  # type: ignore[typeddict-item]
    second_entry: lib.CEDict = {"article": second}  # type: ignore[typeddict-item]
    first_entry_2: lib.CEDict = {"article": first}  # type: ignore[typeddict-item]
    entries = [first_entry_1, second_entry, first_entry_2]
    add_entries = MagicMock(side_effect=lambda rows, **kwargs: iter(rows))
    format_ces = MagicMock()
    location_plan = MagicMock()
    locations = {"locality": object()}
    monkeypatch.setattr(lib, "add_classification_entries", add_entries)
    monkeypatch.setattr(lib, "format_ces_in_article", format_ces)
    monkeypatch.setattr(location_file, "apply_plan", MagicMock(return_value=locations))
    add_occurrences = MagicMock()
    monkeypatch.setattr(occurrence_import, "add_occurrence_records", add_occurrences)

    import_ce_file.import_and_lint(entries, verbose=False, location_plan=location_plan)

    assert add_entries.call_count == 2
    assert add_entries.call_args_list[0].args == ([first_entry_1, first_entry_2],)
    assert add_entries.call_args_list[1].args == ([second_entry],)
    assert all(
        call.kwargs == {"dry_run": False, "strict": True, "verbose": False}
        for call in add_entries.call_args_list
    )
    assert format_ces.call_args_list == [call(first), call(second)]
    add_occurrences.assert_called_once_with(entries, locations)
