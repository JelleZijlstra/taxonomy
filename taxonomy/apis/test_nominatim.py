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


def test_geocodejson_search_returns_normalized_administrative_hierarchy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_data = Mock(
        return_value=json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "geocoding": {
                                "osm_type": "relation",
                                "osm_id": 113722,
                                "osm_key": "boundary",
                                "osm_value": "administrative",
                                "type": "state",
                                "label": "Pichincha, Ecuador",
                                "name": "Pichincha",
                                "state": "Pichincha",
                                "country": "Ecuador",
                                "country_code": "ec",
                                "admin": {"level4": "Pichincha"},
                            }
                        },
                    }
                ],
            }
        )
    )
    monkeypatch.setattr(nominatim, "get_nominatim_data", get_data)

    results = nominatim.search_geocodejson("Pichincha, Ecuador", limit=10)

    assert results == [
        nominatim.GeocodeResult(
            name="Pichincha",
            display_name="Pichincha, Ecuador",
            category="boundary",
            feature_type="administrative",
            address_type="state",
            address={"state": "Pichincha", "country": "Ecuador", "country_code": "ec"},
            administrative={"level4": "Pichincha"},
            osm_type="relation",
            osm_id=113722,
        )
    ]
    url = get_data.call_args.args[0]
    assert urlparse(url).path == "/search"
    assert parse_qs(urlparse(url).query) == {
        "q": ["Pichincha, Ecuador"],
        "format": ["geocodejson"],
        "addressdetails": ["1"],
        "limit": ["10"],
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
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "geocoding": {
                                "label": "Carson City, Nevada, United States",
                                "name": "Carson City",
                                "type": "city",
                                "city": "Carson City",
                                "state": "Nevada",
                                "country": "United States",
                                "country_code": "us",
                                "admin": {"level6": "Carson City", "level4": "Nevada"},
                            }
                        },
                    }
                ],
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
        administrative={"level6": "Carson City", "level4": "Nevada"},
    )
    url = get_data.call_args.args[0]
    assert urlparse(url).path == "/reverse"
    assert parse_qs(urlparse(url).query) == {
        "lat": ["39.16"],
        "lon": ["-119.77"],
        "format": ["geocodejson"],
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


def test_reverse_returns_none_for_empty_geocodejson_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        nominatim,
        "get_nominatim_data",
        Mock(return_value=json.dumps({"type": "FeatureCollection", "features": []})),
    )

    assert nominatim.reverse(coordinates.Point(0, 0)) is None


def test_get_openstreetmap_country_uses_geocodejson_reverse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    point = coordinates.Point(-78.75, 0.11)
    reverse = Mock(
        return_value=nominatim.ReverseResult(
            display_name="Pichincha, Ecuador",
            address={"state": "Pichincha", "country": "Ecuador"},
        )
    )
    monkeypatch.setattr(nominatim, "reverse", reverse)

    assert nominatim.get_openstreetmap_country(point) == "Ecuador"
    reverse.assert_called_once_with(point)


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
