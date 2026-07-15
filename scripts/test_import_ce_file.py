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
    monkeypatch.setattr(import_ce_file.lib, "add_classification_entries", add_entries)  # type: ignore[attr-defined]
    monkeypatch.setattr(import_ce_file.lib, "format_ces_in_article", format_ces)  # type: ignore[attr-defined]

    import_ce_file.import_and_lint(entries, verbose=True)

    add_entries.assert_called_once_with(
        entries, dry_run=False, strict=True, verbose=True
    )
    format_ces.assert_called_once_with(article)
