"""

Cache for arbitrary external data.

Motivating use case: caching CrossRef API responses.

Table created with:

CREATE TABLE `url_cache` (
    `domain` INT UNSIGNED NOT NULL,
    `key` VARCHAR(128),
    `content` TEXT
);
CREATE UNIQUE INDEX `full_key` on `url_cache` (`domain`, `key`);

"""

import datetime
import enum
import functools
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from peewee import SqliteDatabase

from taxonomy.config import get_options

CachedCallable = Callable[[str], str]


class CacheDomain(enum.Enum):
    test = 1  # test data
    doi = 2  # https://api.crossref.org/swagger-ui/index.html#/Works/get_works__doi_
    zoobank_act = (
        3  # e.g. https://zoobank.org/NomenclaturalActs.json/Pseudanthias_carlsoni
    )
    zoobank_publication = 4  # e.g. https://zoobank.org/References.json/427D7953-E8FC-41E8-BEA7-8AE644E6DE77
    crossref_openurl = 5
    bhl_title = 6  # BHL GetTitleMetadata
    bhl_item = 7  # BHL GetItemMetadata
    bhl_page = 8  # BHL GetPageMetadata
    bhl_part = 9  # BHL GetPartMetadata


KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")


@dataclass
class LRU(Generic[KeyT, ValueT]):
    max_size: int
    _cache: dict[KeyT, ValueT] = field(default_factory=dict, init=False)

    def __getitem__(self, key: KeyT) -> ValueT:
        value = self._cache.pop(key)
        self._cache[key] = value
        return value

    def __contains__(self, key: KeyT) -> bool:
        return key in self._cache

    def __setitem__(self, key: KeyT, value: ValueT) -> None:
        if len(self._cache) >= self.max_size:
            key_to_remove = next(iter(self._cache))
            del self._cache[key_to_remove]
        self._cache[key] = value

    def __delitem__(self, key: KeyT) -> None:
        del self._cache[key]

    def dirty(self, key: KeyT) -> None:
        self._cache.pop(key, None)


_LOCAL_CACHE: LRU[tuple[CacheDomain, str], str] = LRU(2048)


@functools.cache
def get_database() -> SqliteDatabase:
    option = get_options()
    return SqliteDatabase(option.urlcache_filename)


def run_query(sql: str, args: tuple[object, ...]) -> list[tuple[Any, ...]]:
    db = get_database()
    cursor = db.execute_sql(sql, args)
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
    _LOCAL_CACHE.dirty((domain, key))


@cached(CacheDomain.test)
def example_cached(key: str) -> str:
    print("Called with key:", key)
    return f"{key} at {datetime.datetime.now().isoformat()}"
