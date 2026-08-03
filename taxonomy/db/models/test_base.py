from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from taxonomy.db import models
from taxonomy.db.models.base import BaseModel, LintConfig
from taxonomy.db.models.tags import LocationTag


def test_lint_all_uses_read_only_context_without_autofix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    @contextmanager
    def readonly() -> Iterator[None]:
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    def linter(_obj: object, cfg: LintConfig) -> tuple[()]:
        assert events == ["enter"]
        assert not cfg.autofix
        return ()

    monkeypatch.setattr(BaseModel.clirm, "readonly", readonly)
    monkeypatch.setattr(
        models.Location, "clear_lint_caches", classmethod(lambda cls: None)
    )

    assert models.Location.lint_all(linter, autofix=False, query=[]) == []
    assert events == ["enter", "exit"]


def test_read_only_lint_reports_invalid_model_reference_in_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = models.OccurrenceRecord(2)
    location = models.Location(1)
    tag = LocationTag.CoordinatesFromOccurrenceRecord(record)
    location._clirm_data["tags"] = models.Location.tags.serialize((tag,))

    monkeypatch.setattr(models.Location, "fields", classmethod(lambda cls: ["tags"]))
    monkeypatch.setattr(models.Location, "is_invalid", lambda self: False)
    monkeypatch.setattr(models.Location, "__str__", lambda self: f"Location #{self.id}")
    monkeypatch.setattr(models.OccurrenceRecord, "is_invalid", lambda self: True)
    monkeypatch.setattr(
        models.OccurrenceRecord, "get_redirect_target", lambda self: None
    )
    monkeypatch.setattr(
        models.OccurrenceRecord, "__repr__", lambda self: f"OccurrenceRecord #{self.id}"
    )
    monkeypatch.setattr(
        models.OccurrenceRecord, "__str__", lambda self: f"OccurrenceRecord #{self.id}"
    )

    messages = list(
        location.check_all_fields(LintConfig(autofix=False, interactive=False))
    )

    expected = "Location #1: references invalid object OccurrenceRecord #2 in tags tag CoordinatesFromOccurrenceRecord(OccurrenceRecord #2)"
    assert messages == [expected]


def test_get_direct_backrefs_excludes_invalid_objects() -> None:
    valid = SimpleNamespace(is_invalid=lambda: False)
    invalid = SimpleNamespace(is_invalid=lambda: True)
    field = SimpleNamespace(related_name="references")
    obj = SimpleNamespace(clirm_backrefs=(field,), references=(valid, invalid))

    assert list(BaseModel.get_direct_backrefs(obj)) == [(field, valid)]  # type: ignore[arg-type]
    assert list(
        BaseModel.get_direct_backrefs(obj, include_invalid=True)  # type: ignore[arg-type]
    ) == [(field, valid), (field, invalid)]
