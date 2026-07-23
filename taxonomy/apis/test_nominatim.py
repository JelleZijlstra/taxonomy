from __future__ import annotations

import json
import time
from unittest.mock import Mock
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from taxonomy.apis import nominatim


def test_search_requests_address_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    get_data = Mock(
        return_value=json.dumps(
            [
                {
                    "lat": "38.0615885",
                    "lon": "-122.6985975",
                    "name": "Nicasio",
                    "display_name": "Nicasio, Marin County, California, United States",
                    "category": "place",
                    "type": "hamlet",
                    "address": {
                        "hamlet": "Nicasio",
                        "county": "Marin County",
                        "state": "California",
                        "country": "United States",
                    },
                }
            ]
        )
    )
    monkeypatch.setattr(nominatim, "get_nominatim_data", get_data)

    results = nominatim.search("Nicasio, Marin County", limit=3)

    assert results == [
        nominatim.SearchResult(
            latitude="38.0615885",
            longitude="-122.6985975",
            name="Nicasio",
            display_name="Nicasio, Marin County, California, United States",
            category="place",
            feature_type="hamlet",
            address={
                "hamlet": "Nicasio",
                "county": "Marin County",
                "state": "California",
                "country": "United States",
            },
        )
    ]
    url = get_data.call_args.args[0]
    assert urlparse(url).path == "/search"
    assert parse_qs(urlparse(url).query) == {
        "q": ["Nicasio, Marin County"],
        "format": ["jsonv2"],
        "addressdetails": ["1"],
        "limit": ["3"],
        "accept-language": ["en"],
    }


def test_uncached_requests_are_rate_limited(monkeypatch: pytest.MonkeyPatch) -> None:
    response = Mock(text="response")
    get = Mock(return_value=response)
    sleep = Mock()
    monkeypatch.setattr(nominatim, "_last_request_started", 10.0)
    monkeypatch.setattr(time, "monotonic", Mock(side_effect=[10.25, 11.0]))
    monkeypatch.setattr(time, "sleep", sleep)
    monkeypatch.setattr(httpx, "get", get)

    raw_get = nominatim.get_nominatim_data.__wrapped__  # type: ignore[attr-defined]
    assert raw_get("https://nominatim.example/search") == "response"

    sleep.assert_called_once_with(0.75)
    get.assert_called_once_with(
        "https://nominatim.example/search", headers={"User-Agent": nominatim.UA}
    )
    response.raise_for_status.assert_called_once_with()
    assert nominatim._last_request_started == 11.0
