import json
import urllib.parse

import httpx

from taxonomy.db.url_cache import CacheDomain, cached


@cached(CacheDomain.is_hdl_valid)
def _is_hdl_valid_cached(hdl: str) -> str:
    # Use the Handle.net proxy server REST API
    # https://www.handle.net/proxy_servlet.html
    url = f"https://hdl.handle.net/api/handles/{urllib.parse.quote(hdl, safe='')}"
    response = httpx.get(url)
    if response.status_code == 404:
        return "false"
    response.raise_for_status()
    data = json.loads(response.text)
    # Per API: responseCode == 1 indicates success
    return "true" if data.get("responseCode") == 1 else "false"


def is_hdl_valid(hdl: str) -> bool:
    return _is_hdl_valid_cached(hdl) == "true"
