import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from taxonomy.db import models
from taxonomy.db.models.base import BaseModel, LazyClirm, LintConfig, LintResource
from taxonomy.db.models.tags import LocationTag


def test_lazy_clirm_does_not_connect_on_initialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_connection(self: LazyClirm) -> sqlite3.Connection:
        raise AssertionError("LazyClirm opened a connection before it was needed")

    monkeypatch.setenv("CLIRM_READONLY", "1")
    monkeypatch.setattr(LazyClirm, "make_connection", unexpected_connection)
    database = LazyClirm()
    with database.readonly():
        assert database.is_read_only
    assert database._conn is None


def test_lazy_clirm_protects_new_and_replacement_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIRM_READONLY", "1")
    monkeypatch.setattr(
        LazyClirm, "make_connection", lambda self: sqlite3.connect(":memory:")
    )
    database = LazyClirm()
    try:
        first = database.conn
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            first.execute("CREATE TABLE test (id INTEGER)")
        database.reconnect()
        assert database.conn is not first
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            first.execute("SELECT 1")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            database.conn.execute("CREATE TABLE test (id INTEGER)")

        database.conn.close()
        database.conn = sqlite3.connect(":memory:")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            database.conn.execute("CREATE TABLE test (id INTEGER)")
    finally:
        database.conn.close()


@pytest.mark.parametrize("initial_query_only", [False, True])
def test_lazy_clirm_restores_connection_state_after_readonly(
    monkeypatch: pytest.MonkeyPatch, *, initial_query_only: bool
) -> None:
    # All connections in this test are isolated in-memory databases.
    monkeypatch.setenv("CLIRM_READONLY", "0")

    def make_connection(self: LazyClirm) -> sqlite3.Connection:
        connection = sqlite3.connect(":memory:")
        if initial_query_only:
            connection.execute("PRAGMA query_only = ON")
        return connection

    monkeypatch.setattr(LazyClirm, "make_connection", make_connection)
    database = LazyClirm()
    try:
        with database.readonly():
            assert database.conn.execute("PRAGMA query_only").fetchone() == (1,)
            database.reconnect()
            # Reassigning the same connection must preserve its original state.
            database.conn = database.conn
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                database.conn.execute("CREATE TABLE test (id INTEGER)")
        assert database.conn.execute("PRAGMA query_only").fetchone() == (
            int(initial_query_only),
        )
    finally:
        database.conn.close()


class _IntegrityErrorOnEdit:
    _run_field_edit = BaseModel._run_field_edit

    def __init__(self) -> None:
        self._name = "original"
        self.reload_count = 0

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        # Match clirm's behavior: its cache is changed before SQLite rejects
        # the UPDATE.
        self._name = value
        raise sqlite3.IntegrityError("UNIQUE constraint failed: location.name")

    def get_value_for_field(self, field: str) -> str:
        assert field == "name"
        return "duplicate"

    def reload(self) -> _IntegrityErrorOnEdit:
        self._name = "original"
        self.reload_count += 1
        return self


def test_fill_field_rejects_integrity_error_and_restores_state(
    capsys: pytest.CaptureFixture[str],
) -> None:
    obj = _IntegrityErrorOnEdit()

    BaseModel.fill_field(obj, "name")  # type: ignore[arg-type]

    assert obj.name == "original"
    assert obj.reload_count == 1
    assert (
        capsys.readouterr().out
        == "Edit rejected: UNIQUE constraint failed: location.name\n"
    )


def test_lint_all_uses_read_only_context_without_autofix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    @contextmanager
    def readonly() -> Generator[None]:
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


def test_lint_all_passes_explicit_available_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[LintConfig] = []

    def linter(_obj: object, cfg: LintConfig) -> tuple[()]:
        seen.append(cfg)
        return ()

    monkeypatch.setattr(
        models.Location, "clear_lint_caches", classmethod(lambda cls: None)
    )

    assert (
        models.Location.lint_all(
            linter,
            autofix=False,
            query=[object()],  # type: ignore[list-item]
            available_resources=frozenset({LintResource.SLOW}),
        )
        == []
    )
    assert seen[0].available_resources == frozenset({LintResource.SLOW})


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
