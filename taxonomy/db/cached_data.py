"""Cache for storing arbitrary data."""

import functools
import sqlite3
from typing import Any

from taxonomy.config import get_options


@functools.cache
def get_database() -> sqlite3.Connection:
    option = get_options()
    # static analysis: ignore[internal_error]
    return sqlite3.connect(option.db_filename)


def run_query(sql: str, args: tuple[object, ...]) -> list[tuple[Any, ...]]:
    db = get_database()
    with db:
        cursor = db.execute(sql, args)
        return cursor.fetchall()


def get(key: str) -> bytes | None:
    cached_rows = run_query(
        """
        SELECT data
        FROM cached_data
        WHERE name = ?
        """,
        (key,),
    )
    if len(cached_rows) == 1:
        return cached_rows[0][0]
    return None


def set(key: str, value: bytes) -> None:
    run_query(
        """
        REPLACE INTO cached_data(name, data)
        VALUES(?, ?)
        """,
        (key, value),
    )


def clear(key: str) -> None:
    run_query(
        """
        DELETE FROM cached_data
        WHERE name = ?
        """,
        (key,),
    )
