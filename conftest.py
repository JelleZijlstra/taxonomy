"""Keep unit tests independent of private configuration and database contents."""

import sqlite3
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

_TEST_DIRECTORY = tempfile.TemporaryDirectory(prefix="taxonomy-tests-")
_TEST_ROOT = Path(_TEST_DIRECTORY.name)
_CONFIG = _TEST_ROOT / "taxonomy.ini"
_CONFIG.write_text(
    "[taxonomy]\n"
    "use_sqlite = true\n"
    "db_filename = unused.sqlite\n"
    "derived_data_filename = derived.pkl\n"
    "urlcache_filename = urlcache.sqlite\n"
    f"parserdata_path = {Path(__file__).parent / 'taxonomy/db/models/article/parserdata'}\n"
)
_ENVIRONMENT = pytest.MonkeyPatch()
_ENVIRONMENT.setenv("TAXONOMY_CONFIG_FILE", str(_CONFIG))
_ENVIRONMENT.setenv("CLIRM_READONLY", "1")


@pytest.fixture(autouse=True)
def _isolated_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    from taxonomy.db import derived_data
    from taxonomy.db.models.base import BaseModel

    # Most tests use virtual models. Empty tables support incidental lookups without
    # consulting the user's database. Tests of constraints supply their own schema.
    connection = sqlite3.connect(":memory:")
    for model in BaseModel.call_sign_to_model.values():
        if not model.clirm_fields:
            continue
        columns = ", ".join(
            [
                '"id" INTEGER PRIMARY KEY',
                *(f'"{field.name}"' for field in model.clirm_fields.values()),
            ]
        )
        connection.execute(f'CREATE TABLE "{model.clirm_table_name}" ({columns})')
    connection.execute("PRAGMA query_only = ON")
    monkeypatch.setattr(BaseModel.clirm, "_conn", connection)
    derived_data.load_derived_data.cache_clear()
    for model in BaseModel.call_sign_to_model.values():
        model.clear_lint_caches()
    try:
        yield
    finally:
        connection.close()


def pytest_unconfigure() -> None:
    _ENVIRONMENT.undo()
    _TEST_DIRECTORY.cleanup()
