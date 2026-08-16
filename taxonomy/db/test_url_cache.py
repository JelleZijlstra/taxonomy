import datetime
import json
import sqlite3
from unittest.mock import Mock

import pytest

from taxonomy.db import url_cache


def test_cached_value_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript("""
            CREATE TABLE url_cache (domain INT NOT NULL, key TEXT, content TEXT);
            CREATE UNIQUE INDEX full_key ON url_cache (domain, key);
            """)
        monkeypatch.setattr(url_cache, "get_database", Mock(return_value=connection))
        url_cache.clear_memory_cache()

        url_cache.set_cached_value(url_cache.CacheDomain.test, "key", "first")
        url_cache.set_cached_value(url_cache.CacheDomain.test, "key", "updated")
        url_cache.clear_memory_cache()

        assert (
            url_cache.get_cached_value(url_cache.CacheDomain.test, "key") == "updated"
        )
    finally:
        url_cache.clear_memory_cache()
        connection.close()


def test_set_expiring_cached_value(monkeypatch: pytest.MonkeyPatch) -> None:
    set_value = Mock()
    monkeypatch.setattr(url_cache, "set_cached_value", set_value)
    now = datetime.datetime(2026, 8, 15, tzinfo=datetime.UTC)

    url_cache.set_expiring_cached_value(
        url_cache.CacheDomain.test,
        "key",
        "not found",
        ttl=datetime.timedelta(days=30),
        now=now,
    )

    domain, key, raw_value = set_value.call_args.args
    assert domain is url_cache.CacheDomain.test
    assert key == "key"
    assert json.loads(raw_value) == {
        "content": "not found",
        "expires_at": "2026-09-14T00:00:00+00:00",
    }


def test_get_expiring_cached_value(monkeypatch: pytest.MonkeyPatch) -> None:
    get_value = Mock(
        return_value=json.dumps(
            {"content": "not found", "expires_at": "2026-09-14T00:00:00+00:00"}
        )
    )
    monkeypatch.setattr(url_cache, "get_cached_value", get_value)

    assert (
        url_cache.get_expiring_cached_value(
            url_cache.CacheDomain.test,
            "key",
            now=datetime.datetime(2026, 8, 15, tzinfo=datetime.UTC),
        )
        == "not found"
    )


@pytest.mark.parametrize("raw_value", ["not JSON", '{"content": "missing expiry"}'])
def test_get_expiring_cached_value_discards_invalid_or_expired_entries(
    monkeypatch: pytest.MonkeyPatch, raw_value: str
) -> None:
    monkeypatch.setattr(url_cache, "get_cached_value", Mock(return_value=raw_value))
    dirty_cache = Mock()
    monkeypatch.setattr(url_cache, "dirty_cache", dirty_cache)

    assert (
        url_cache.get_expiring_cached_value(
            url_cache.CacheDomain.test,
            "key",
            now=datetime.datetime(2026, 8, 15, tzinfo=datetime.UTC),
        )
        is None
    )
    dirty_cache.assert_called_once_with(url_cache.CacheDomain.test, "key")


def test_get_expiring_cached_value_discards_expired_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        url_cache,
        "get_cached_value",
        Mock(
            return_value=json.dumps(
                {"content": "not found", "expires_at": "2026-08-14T00:00:00+00:00"}
            )
        ),
    )
    dirty_cache = Mock()
    monkeypatch.setattr(url_cache, "dirty_cache", dirty_cache)

    assert (
        url_cache.get_expiring_cached_value(
            url_cache.CacheDomain.test,
            "key",
            now=datetime.datetime(2026, 8, 15, tzinfo=datetime.UTC),
        )
        is None
    )
    dirty_cache.assert_called_once_with(url_cache.CacheDomain.test, "key")
