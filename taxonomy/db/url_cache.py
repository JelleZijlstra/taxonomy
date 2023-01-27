"""

Cache for arbitrary external data.

Motivating use case: caching CrossRef API responses.

"""
import datetime
import enum
import functools
from typing import Any
from collections.abc import Callable

from . import models

CachedCallable = Callable[[str], str]


class CacheDomain(enum.Enum):
    test = 1  # test data
    doi = 2  # https://api.crossref.org/swagger-ui/index.html#/Works/get_works__doi_
    zoobank_act = (
        3  # e.g. https://zoobank.org/NomenclaturalActs.json/Pseudanthias_carlsoni
    )
    zoobank_publication = 4  # e.g. https://zoobank.org/References.json/427D7953-E8FC-41E8-BEA7-8AE644E6DE77


_LOCAL_CACHE: dict[tuple[CacheDomain, str], str] = {}


def run_query(sql: str, args: tuple[object, ...]) -> list[tuple[Any, ...]]:
    cursor = models.base.database.execute_sql(sql, args)
    return list(cursor)


def cached(domain: CacheDomain) -> Callable[[CachedCallable], CachedCallable]:
    def decorator(func: CachedCallable) -> CachedCallable:
        @functools.wraps(func)
        def wrapper(key: str) -> str:
            local_key = (domain, key)
            if local_key in _LOCAL_CACHE:
                return _LOCAL_CACHE[local_key]
            cached_rows = run_query(
                """
                SELECT content
                FROM url_cache
                WHERE domain = ? AND key = ?
                """,
                (domain.value, key),
            )
            if len(cached_rows) == 1:
                value = cached_rows[0][0]
                _LOCAL_CACHE[local_key] = value
                return value

            value = func(key)
            _LOCAL_CACHE[local_key] = value
            run_query(
                """
                INSERT INTO url_cache(domain, key, content)
                VALUES(?, ?, ?)
                """,
                (domain.value, key, value),
            )
            return value

        return wrapper

    return decorator


def dirty_cache(domain: CacheDomain, key: str) -> None:
    run_query(
        """
        DELETE FROM url_cache
        WHERE domain = ? AND key = ?
        """,
        (domain.value, key),
    )


@cached(CacheDomain.test)
def example_cached(key: str) -> str:
    print("Called with key:", key)
    return f"{key} at {datetime.datetime.now().isoformat()}"
