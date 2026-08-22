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
                    "addresstype": "village",
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
            address_type="village",
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


def test_geocodejson_structured_search_uses_separate_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_data = Mock(return_value=json.dumps({"features": []}))
    monkeypatch.setattr(nominatim, "get_nominatim_data", get_data)

    assert (
        nominatim.search_geocodejson_structured(
            state="Amazonas", country="Brazil", limit=10
        )
        == []
    )

    url = get_data.call_args.args[0]
    assert parse_qs(urlparse(url).query) == {
        "state": ["Amazonas"],
        "country": ["Brazil"],
        "format": ["geocodejson"],
        "addressdetails": ["1"],
        "limit": ["10"],
        "accept-language": ["en"],
    }


def test_geocodejson_structured_search_requires_a_field() -> None:
    with pytest.raises(ValueError, match="at least one"):
        nominatim.search_geocodejson_structured()


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


def test_lookup_many_batches_and_returns_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def get_data(url: str) -> str:
        calls.append(url)
        references = parse_qs(urlparse(url).query)["osm_ids"][0].split(",")
        return json.dumps(
            [
                {
                    "lat": "0",
                    "lon": "0",
                    "name": f"Region {reference[1:]}",
                    "display_name": f"Region {reference[1:]}, Example",
                    "category": "boundary",
                    "type": "administrative",
                    "osm_type": "relation",
                    "osm_id": int(reference[1:]),
                    "address": {"country": "Example"},
                    "namedetails": {
                        "name": f"Region {reference[1:]}",
                        "name:en": f"English {reference[1:]}",
                    },
                    "extratags": (
                        None
                        if reference == "R51"
                        else {"wikidata": f"Q{reference[1:]}"}
                    ),
                }
                for reference in references
            ]
        )

    monkeypatch.setattr(nominatim, "get_nominatim_data", get_data)
    references = [("relation", osm_id) for osm_id in range(1, 52)]

    results = nominatim.lookup_many(references)

    assert len(calls) == 2
    assert len(parse_qs(urlparse(calls[0]).query)["osm_ids"][0].split(",")) == 50
    assert parse_qs(urlparse(calls[0]).query) == {
        "osm_ids": [",".join(f"R{osm_id}" for osm_id in range(1, 51))],
        "format": ["jsonv2"],
        "addressdetails": ["1"],
        "extratags": ["1"],
        "namedetails": ["1"],
        "accept-language": ["en"],
    }
    assert results[("relation", 51)].names["name:en"] == "English 51"
    assert results[("relation", 50)].extra["wikidata"] == "Q50"
    assert results[("relation", 51)].extra == {}


def test_lookup_many_rejects_invalid_reference() -> None:
    with pytest.raises(ValueError, match="unsupported OSM object types"):
        nominatim.lookup_many([("area", 1)])
    with pytest.raises(ValueError, match="must be positive"):
        nominatim.lookup_many([("relation", 0)])


def test_lookup_boundary_uses_cached_hole_aware_geometry_offline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cached = Mock(
        return_value=json.dumps(
            [
                {
                    "name": "Example",
                    "display_name": "Example, Country",
                    "category": "boundary",
                    "type": "administrative",
                    "osm_type": "relation",
                    "osm_id": 123,
                    "namedetails": {"name": "Example", "name:en": "Example"},
                    "geojson": {
                        "type": "Polygon",
                        "coordinates": [
                            [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
                            [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]],
                        ],
                    },
                }
            ]
        )
    )
    fetch = Mock()
    monkeypatch.setattr(nominatim, "get_cached_value", cached)
    monkeypatch.setattr(nominatim, "get_nominatim_data", fetch)

    result = nominatim.lookup_boundary("relation", 123, allow_network=False)

    assert result is not None
    assert result.geometry is not None
    assert coordinates.is_in_geometry(coordinates.Point(2, 2), result.geometry)
    assert not coordinates.is_in_geometry(coordinates.Point(5, 5), result.geometry)
    fetch.assert_not_called()
    url = cached.call_args.args[1]
    assert parse_qs(urlparse(url).query) == {
        "osm_ids": ["R123"],
        "format": ["jsonv2"],
        "addressdetails": ["0"],
        "namedetails": ["1"],
        "polygon_geojson": ["1"],
        "polygon_threshold": ["0.001"],
        "accept-language": ["en"],
    }


def test_lookup_boundary_does_not_fetch_on_offline_cache_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = Mock()
    monkeypatch.setattr(nominatim, "get_cached_value", Mock(return_value=None))
    monkeypatch.setattr(nominatim, "get_nominatim_data", fetch)

    assert nominatim.lookup_boundary("relation", 123, allow_network=False) is None
    fetch.assert_not_called()


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
                                "osm_type": "relation",
                                "osm_id": "1234",
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
        osm_type="relation",
        osm_id=1234,
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
    response = Mock(text="response", status_code=200)
    get = Mock(return_value=response)
    sleep = Mock()
    monkeypatch.setattr(nominatim, "_last_request_started", 10.0)
    monkeypatch.setattr(time, "monotonic", Mock(side_effect=[10.25, 12.0]))
    monkeypatch.setattr(time, "sleep", sleep)
    monkeypatch.setattr(httpx, "get", get)

    raw_get = nominatim.get_nominatim_data.__wrapped__  # type: ignore[attr-defined]
    assert raw_get("https://nominatim.example/search") == "response"

    sleep.assert_called_once_with(1.75)
    get.assert_called_once_with(
        "https://nominatim.example/search",
        headers={"User-Agent": nominatim.UA},
        timeout=nominatim.REQUEST_TIMEOUT,
    )
    response.raise_for_status.assert_called_once_with()
    assert nominatim._last_request_started == 12.0


def test_uncached_request_retries_429_after_server_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limited = Mock(status_code=429, headers={"Retry-After": "12"})
    response = Mock(text="response", status_code=200)
    get = Mock(side_effect=[limited, response])
    sleep = Mock()
    monkeypatch.setattr(nominatim, "_last_request_started", None)
    monkeypatch.setattr(time, "monotonic", Mock(side_effect=[10.0, 22.0]))
    monkeypatch.setattr(time, "sleep", sleep)
    monkeypatch.setattr(httpx, "get", get)

    raw_get = nominatim.get_nominatim_data.__wrapped__  # type: ignore[attr-defined]
    assert raw_get("https://nominatim.example/lookup") == "response"

    sleep.assert_called_once_with(12.0)
    assert get.call_count == 2
    limited.raise_for_status.assert_not_called()
    response.raise_for_status.assert_called_once_with()
    assert nominatim._last_request_started == 22.0
