import json
import os
import threading
import time
from dataclasses import dataclass
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
        return SearchResult(
            latitude=str(row["lat"]),
            longitude=str(row["lon"]),
            name=str(row["name"]),
            display_name=str(row["display_name"]),
            category=str(row["category"]),
            feature_type=str(row["type"]),
            address=address,
        )
    except (KeyError, TypeError) as exc:
        raise ValueError(row) from exc


def get_openstreetmap_country(point: coordinates.Point) -> str | None:
    url = f"{BASE_URL}/reverse?format=jsonv2&lat={point.latitude}&lon={point.longitude}&accept-language=en"
    data = json.loads(get_nominatim_data(url))
    # maybe in the ocean
    if data.get("error") == "Unable to geocode":
        return None
    try:
        return data["address"]["country"]
    except KeyError:
        raise ValueError(data) from None


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
    "Martinique": "France",
    "Republic of the Congo": "Congo-Brazzaville",
    "Réunion": "France",
    "New Caledonia": "France",
    "French Guiana": "France",
    "Czech Republic": "Czechia",
}
