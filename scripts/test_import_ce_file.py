from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from data_import import lib
from scripts import import_ce_file


def test_import_and_lint_all_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    article = object()
    entries: list[lib.CEDict] = [{"article": article}]  # type: ignore[typeddict-item]
    add_entries = MagicMock(return_value=iter(entries))
    format_ces = MagicMock()
    location_plan = MagicMock()
    locations = {"locality": object()}
    apply_locations = MagicMock(return_value=locations)
    add_occurrences = MagicMock()
    monkeypatch.setattr(import_ce_file.lib, "add_classification_entries", add_entries)  # type: ignore[attr-defined]
    monkeypatch.setattr(import_ce_file.lib, "format_ces_in_article", format_ces)  # type: ignore[attr-defined]
    monkeypatch.setattr(import_ce_file.location_file, "apply_plan", apply_locations)  # type: ignore[attr-defined]
    monkeypatch.setattr(
        import_ce_file.occurrence_import, "add_occurrence_records", add_occurrences  # type: ignore[attr-defined]
    )

    import_ce_file.import_and_lint(entries, verbose=True, location_plan=location_plan)

    add_entries.assert_called_once_with(
        entries, dry_run=False, strict=True, verbose=True
    )
    format_ces.assert_called_once_with(article)
    apply_locations.assert_called_once_with(location_plan)
    add_occurrences.assert_called_once_with(entries, locations)
