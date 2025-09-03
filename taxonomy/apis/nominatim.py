import json

import httpx

from taxonomy import coordinates
from taxonomy.db.url_cache import CacheDomain, cached

UA = "taxonomy (https://github.com/JelleZijlstra/taxonomy)"


def get_openstreetmap_country(point: coordinates.Point) -> str | None:
    url = f"https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat={point.latitude}&lon={point.longitude}&accept-language=en"
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
