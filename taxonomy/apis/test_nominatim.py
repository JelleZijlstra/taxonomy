import json
import time
from unittest.mock import Mock
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from taxonomy import coordinates
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
                    "osm_type": "relation",
                    "osm_id": 1234,
                    "boundingbox": [
                        "38.0415885",
                        "38.0815885",
                        "-122.7185975",
                        "-122.6785975",
                    ],
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
            osm_type="relation",
            osm_id=1234,
            bounding_box=("38.0415885", "38.0815885", "-122.7185975", "-122.6785975"),
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


def test_lookup_uses_stable_osm_object_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_data = Mock(
        return_value=json.dumps(
            [
                {
                    "lat": "38.0615885",
                    "lon": "-122.6985975",
                    "name": "Nicasio",
                    "display_name": "Nicasio, California, United States",
                    "category": "place",
                    "type": "hamlet",
                    "osm_type": "relation",
                    "osm_id": 1234,
                    "address": {"country": "United States"},
                }
            ]
        )
    )
    monkeypatch.setattr(nominatim, "get_nominatim_data", get_data)

    result = nominatim.lookup("relation", 1234)

    assert result is not None
    assert (result.osm_type, result.osm_id) == ("relation", 1234)
    url = get_data.call_args.args[0]
    assert urlparse(url).path == "/lookup"
    assert parse_qs(urlparse(url).query)["osm_ids"] == ["R1234"]


def test_reverse_requests_administrative_address(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_data = Mock(
        return_value=json.dumps(
            {
                "display_name": "Carson City, Nevada, United States",
                "address": {
                    "city": "Carson City",
                    "state": "Nevada",
                    "country": "United States",
                    "country_code": "us",
                },
            }
        )
    )
    monkeypatch.setattr(nominatim, "get_nominatim_data", get_data)

    result = nominatim.reverse(coordinates.Point(-119.77, 39.16), zoom=5)

    assert result == nominatim.ReverseResult(
        display_name="Carson City, Nevada, United States",
        address={
            "city": "Carson City",
            "state": "Nevada",
            "country": "United States",
            "country_code": "us",
        },
    )
    url = get_data.call_args.args[0]
    assert urlparse(url).path == "/reverse"
    assert parse_qs(urlparse(url).query) == {
        "lat": ["39.16"],
        "lon": ["-119.77"],
        "format": ["jsonv2"],
        "addressdetails": ["1"],
        "layer": ["address"],
        "zoom": ["5"],
        "accept-language": ["en"],
    }


def test_reverse_returns_none_when_nominatim_cannot_geocode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        nominatim,
        "get_nominatim_data",
        Mock(return_value=json.dumps({"error": "Unable to geocode"})),
    )

    assert nominatim.reverse(coordinates.Point(0, 0)) is None


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
