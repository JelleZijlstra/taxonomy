import json
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from taxonomy import coordinates
from taxonomy.db.url_cache import CacheDomain, cached

UA = "taxonomy (https://github.com/JelleZijlstra/taxonomy)"
BASE_URL = os.environ.get(
    "TAXONOMY_NOMINATIM_URL", "https://nominatim.openstreetmap.org"
).rstrip("/")
MIN_REQUEST_INTERVAL = 1.0

_request_lock = threading.Lock()
_last_request_started: float | None = None


@dataclass(frozen=True, slots=True)
class SearchResult:
    latitude: str
    longitude: str
    name: str
    display_name: str
    category: str
    feature_type: str
    address: dict[str, str]
    osm_type: str | None = None
    bounding_box: tuple[str, str, str, str] | None = None
    osm_id: int | None = None


@dataclass(frozen=True, slots=True)
class ReverseResult:
    display_name: str
    address: dict[str, str]
    administrative: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GeocodeResult:
    name: str
    display_name: str
    category: str
    feature_type: str
    address_type: str
    address: dict[str, str]
    administrative: dict[str, str]
    osm_type: str | None = None
    osm_id: int | None = None


_GEOCODEJSON_ADDRESS_FIELDS = frozenset(
    {
        "housenumber",
        "street",
        "locality",
        "district",
        "postcode",
        "city",
        "county",
        "state",
        "country",
        "country_code",
    }
)


def search(query: str, *, limit: int = 5) -> list[SearchResult]:
    """Search for named geographic features using Nominatim."""
    url = str(
        httpx.URL(f"{BASE_URL}/search").copy_with(
            params=httpx.QueryParams(
                {
                    "q": query,
                    "format": "jsonv2",
                    "addressdetails": "1",
                    "limit": str(limit),
                    "accept-language": "en",
                }
            )
        )
    )
    data = json.loads(get_nominatim_data(url))
    if not isinstance(data, list):
        raise TypeError(data)
    return [_parse_search_result(row) for row in data]


def search_geocodejson(query: str, *, limit: int = 5) -> list[GeocodeResult]:
    """Search for places using Nominatim's normalized GeocodeJSON schema."""
    url = str(
        httpx.URL(f"{BASE_URL}/search").copy_with(
            params=httpx.QueryParams(
                {
                    "q": query,
                    "format": "geocodejson",
                    "addressdetails": "1",
                    "limit": str(limit),
                    "accept-language": "en",
                }
            )
        )
    )
    data = json.loads(get_nominatim_data(url))
    if not isinstance(data, dict):
        raise TypeError(data)
    features = data.get("features")
    if not isinstance(features, list):
        raise TypeError(data)
    return [_parse_geocodejson_search_result(feature) for feature in features]


def lookup(osm_type: str, osm_id: int) -> SearchResult | None:
    """Look up a stable OpenStreetMap object reference through Nominatim."""
    type_code = {"node": "N", "way": "W", "relation": "R"}.get(osm_type)
    if type_code is None:
        return None
    url = str(
        httpx.URL(f"{BASE_URL}/lookup").copy_with(
            params=httpx.QueryParams(
                {
                    "osm_ids": f"{type_code}{osm_id}",
                    "format": "jsonv2",
                    "addressdetails": "1",
                    "accept-language": "en",
                }
            )
        )
    )
    data = json.loads(get_nominatim_data(url))
    if not isinstance(data, list):
        raise TypeError(data)
    results = [_parse_search_result(row) for row in data]
    return next(
        (
            result
            for result in results
            if result.osm_type == osm_type and result.osm_id == osm_id
        ),
        None,
    )


def reverse(point: coordinates.Point, *, zoom: int = 5) -> ReverseResult | None:
    """Return the nearest Nominatim address for a coordinate."""
    url = str(
        httpx.URL(f"{BASE_URL}/reverse").copy_with(
            params=httpx.QueryParams(
                {
                    "lat": str(point.latitude),
                    "lon": str(point.longitude),
                    "format": "geocodejson",
                    "addressdetails": "1",
                    "layer": "address",
                    "zoom": str(zoom),
                    "accept-language": "en",
                }
            )
        )
    )
    data = json.loads(get_nominatim_data(url))
    if not isinstance(data, dict):
        raise TypeError(data)
    if data.get("error") == "Unable to geocode":
        return None
    try:
        features = data["features"]
        if not isinstance(features, list) or len(features) > 1:
            raise TypeError
        if not features:
            return None
        geocoding = _get_geocodejson_properties(features[0])
        address, administrative = _parse_geocodejson_address(geocoding)
        return ReverseResult(
            display_name=str(geocoding["label"]),
            address=address,
            administrative=administrative,
        )
    except (KeyError, TypeError) as exc:
        raise ValueError(data) from exc


def _get_geocodejson_properties(feature: Any) -> dict[str, Any]:
    if not isinstance(feature, dict):
        raise TypeError
    properties = feature["properties"]
    if not isinstance(properties, dict):
        raise TypeError
    geocoding = properties["geocoding"]
    if not isinstance(geocoding, dict):
        raise TypeError
    return geocoding


def _parse_geocodejson_address(
    geocoding: dict[str, Any],
) -> tuple[dict[str, str], dict[str, str]]:
    address = {
        str(key): value
        for key, value in geocoding.items()
        if key in _GEOCODEJSON_ADDRESS_FIELDS and isinstance(value, str)
    }
    raw_administrative = geocoding.get("admin", {})
    if not isinstance(raw_administrative, dict):
        raise TypeError
    administrative = {
        str(key): value
        for key, value in raw_administrative.items()
        if isinstance(value, str)
    }
    return address, administrative


def _parse_geocodejson_search_result(feature: Any) -> GeocodeResult:
    try:
        geocoding = _get_geocodejson_properties(feature)
        address, administrative = _parse_geocodejson_address(geocoding)
        raw_osm_type = geocoding.get("osm_type")
        if raw_osm_type is not None and not isinstance(raw_osm_type, str):
            raise TypeError
        raw_osm_id = geocoding.get("osm_id")
        if raw_osm_id is None:
            osm_id = None
        elif isinstance(raw_osm_id, int) and not isinstance(raw_osm_id, bool):
            osm_id = raw_osm_id
        elif isinstance(raw_osm_id, str) and raw_osm_id.isdigit():
            osm_id = int(raw_osm_id)
        else:
            raise TypeError
        return GeocodeResult(
            name=str(geocoding["name"]),
            display_name=str(geocoding["label"]),
            category=str(geocoding["osm_key"]),
            feature_type=str(geocoding["osm_value"]),
            address_type=str(geocoding["type"]),
            address=address,
            administrative=administrative,
            osm_type=raw_osm_type,
            osm_id=osm_id,
        )
    except (KeyError, TypeError) as exc:
        raise ValueError(feature) from exc


def _parse_search_result(row: Any) -> SearchResult:
    if not isinstance(row, dict):
        raise TypeError(row)
    try:
        raw_address = row["address"]
        if not isinstance(raw_address, dict):
            raise TypeError
        address = {
            str(key): str(value)
            for key, value in raw_address.items()
            if isinstance(value, str)
        }
        raw_bounding_box = row.get("boundingbox")
        if raw_bounding_box is None:
            bounding_box = None
        elif (
            isinstance(raw_bounding_box, list)
            and len(raw_bounding_box) == 4
            and all(isinstance(value, (int, float, str)) for value in raw_bounding_box)
        ):
            bounding_box = (
                str(raw_bounding_box[0]),
                str(raw_bounding_box[1]),
                str(raw_bounding_box[2]),
                str(raw_bounding_box[3]),
            )
        else:
            raise TypeError
        raw_osm_type = row.get("osm_type")
        if raw_osm_type is not None and not isinstance(raw_osm_type, str):
            raise TypeError
        raw_osm_id = row.get("osm_id")
        if raw_osm_id is None:
            osm_id = None
        elif isinstance(raw_osm_id, int) and not isinstance(raw_osm_id, bool):
            osm_id = raw_osm_id
        elif isinstance(raw_osm_id, str) and raw_osm_id.isdigit():
            osm_id = int(raw_osm_id)
        else:
            raise TypeError
        return SearchResult(
            latitude=str(row["lat"]),
            longitude=str(row["lon"]),
            name=str(row["name"]),
            display_name=str(row["display_name"]),
            category=str(row["category"]),
            feature_type=str(row["type"]),
            address=address,
            osm_type=raw_osm_type,
            bounding_box=bounding_box,
            osm_id=osm_id,
        )
    except (KeyError, TypeError) as exc:
        raise ValueError(row) from exc


def get_openstreetmap_country(point: coordinates.Point) -> str | None:
    result = reverse(point)
    return None if result is None else result.address.get("country")


@cached(CacheDomain.nominatim)
def get_nominatim_data(url: str) -> str:
    # This function is only entered on a cache miss. The public Nominatim service
    # requires clients to stay at or below one request per second.
    global _last_request_started
    with _request_lock:
        now = time.monotonic()
        if _last_request_started is not None:
            delay = MIN_REQUEST_INTERVAL - (now - _last_request_started)
            if delay > 0:
                time.sleep(delay)
                now = time.monotonic()
        _last_request_started = now
        response = httpx.get(url, headers={"User-Agent": UA})
        response.raise_for_status()
        return response.text


HESP_COUNTRY_TO_OSM_COUNTRY = {
    "Cote d'Ivoire": "Côte d'Ivoire",
    "Curaçao": "Curacao",
    "Guadeloupe": "France",
    "Martinique": "France",
    "Republic of the Congo": "Congo-Brazzaville",
    "Réunion": "France",
    "New Caledonia": "France",
    "French Guiana": "France",
    "Czech Republic": "Czechia",
}
