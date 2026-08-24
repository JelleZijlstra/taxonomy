import json
import os
from dataclasses import dataclass

import httpx

from taxonomy.db.url_cache import CacheDomain, cached

UA = "taxonomy (https://github.com/JelleZijlstra/taxonomy)"
BASE_URL = os.environ.get(
    "TAXONOMY_OPENSTREETMAP_API_URL", "https://api.openstreetmap.org/api/0.6"
).rstrip("/")
REQUEST_TIMEOUT = 30.0


@dataclass(frozen=True, slots=True)
class Element:
    osm_type: str
    osm_id: int
    tags: dict[str, str]


def lookup_element(osm_type: str, osm_id: int) -> Element | None:
    """Look up an OSM element directly, independently of Nominatim's index."""
    if osm_type not in {"node", "way", "relation"} or osm_id <= 0:
        return None
    url = f"{BASE_URL}/{osm_type}/{osm_id}.json"
    data = json.loads(get_openstreetmap_data(url))
    if data is None:
        return None
    if not isinstance(data, dict):
        raise TypeError(data)
    elements = data.get("elements")
    if not isinstance(elements, list):
        raise TypeError(data)
    matches = [
        element
        for element in elements
        if isinstance(element, dict)
        and element.get("type") == osm_type
        and element.get("id") == osm_id
        and element.get("visible", True) is not False
    ]
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError(
            f"OpenStreetMap returned duplicate element {(osm_type, osm_id)}"
        )
    raw_tags = matches[0].get("tags", {})
    if not isinstance(raw_tags, dict):
        raise TypeError(raw_tags)
    tags = {
        str(key): value for key, value in raw_tags.items() if isinstance(value, str)
    }
    return Element(osm_type=osm_type, osm_id=osm_id, tags=tags)


@cached(CacheDomain.openstreetmap)
def get_openstreetmap_data(url: str) -> str:
    response = httpx.get(url, headers={"User-Agent": UA}, timeout=REQUEST_TIMEOUT)
    if response.status_code in {404, 410}:
        return "null"
    response.raise_for_status()
    return response.text
