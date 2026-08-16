import json
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import requests

from taxonomy.db.url_cache import (
    CacheDomain,
    cached,
    dirty_cache,
    get_expiring_cached_value,
    set_expiring_cached_value,
)

from .util import RateLimiter


def clean_lsid(lsid: str) -> str:
    lsid = re.sub(r"\s+", "", lsid.lower())
    if lsid.startswith("urn:"):
        *_, lsid = lsid.split(":")
    return (
        lsid.upper()
        .replace("Ø", "0")
        .replace("\N{EN DASH}", "-")
        .replace("\N{MINUS SIGN}", "-")
        .replace("\N{EM DASH}", "-")
    )


def is_valid_lsid(lsid: str) -> bool:
    return bool(
        re.fullmatch(
            r"^[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}$", lsid
        )
    )


rate_limiter = RateLimiter(min_interval=0.5)
REQUEST_TIMEOUT = 10
NEGATIVE_CACHE_TTL = timedelta(days=30)


class ZooBankUnavailableError(RuntimeError):
    """Raised when ZooBank does not return usable API data."""


class ZooBankNotFoundError(ZooBankUnavailableError):
    """Raised when ZooBank returns 404 for a requested record."""


def _get_json_response(url: str, *, expected_count: int | None = None) -> str:
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    if response.status_code == 404:
        raise ZooBankNotFoundError(f"ZooBank returned 404 for {url}")
    response.raise_for_status()
    try:
        data = json.loads(response.text)
    except json.JSONDecodeError as exc:
        # In particular, do not cache the HTML human-verification page.
        raise ZooBankUnavailableError(
            f"ZooBank returned non-JSON data for {url}"
        ) from exc
    if not isinstance(data, list):
        raise ZooBankUnavailableError(
            f"ZooBank returned unexpected data for {url}: {data!r}"
        )
    if expected_count is not None and len(data) != expected_count:
        raise ZooBankUnavailableError(
            f"ZooBank returned {len(data)} records for {url}; expected {expected_count}"
        )
    return response.text


def _get_zoobank_data_with_negative_cache(
    query: str,
    *,
    negative_domain: CacheDomain,
    url: str,
    expected_count: int | None = None,
) -> str:
    cached_failure = get_expiring_cached_value(negative_domain, query)
    if cached_failure is not None:
        raise ZooBankNotFoundError(cached_failure)
    rate_limiter.wait()
    try:
        return _get_json_response(url, expected_count=expected_count)
    except ZooBankNotFoundError as exc:
        set_expiring_cached_value(
            negative_domain, query, str(exc), ttl=NEGATIVE_CACHE_TTL
        )
        raise


@cached(CacheDomain.zoobank_act)
def _get_zoobank_act_data(query: str) -> str:
    return _get_zoobank_data_with_negative_cache(
        query,
        negative_domain=CacheDomain.zoobank_act_negative,
        url=f"https://zoobank.org/NomenclaturalActs.json/{query}",
    )


@cached(CacheDomain.zoobank_publication)
def _get_zoobank_publication_data(query: str) -> str:
    return _get_zoobank_data_with_negative_cache(
        query,
        negative_domain=CacheDomain.zoobank_publication_negative,
        url=f"https://zoobank.org/References.json/{query}",
        expected_count=1,
    )


def clear_zoobank_act_cache(query: str) -> None:
    dirty_cache(CacheDomain.zoobank_act, query)
    dirty_cache(CacheDomain.zoobank_act_negative, query)


def clear_zoobank_publication_cache(lsid: str) -> None:
    lsid = clean_lsid(lsid)
    dirty_cache(CacheDomain.zoobank_publication, lsid)
    dirty_cache(CacheDomain.zoobank_publication_negative, lsid)


@dataclass(frozen=True)
class ZooBankData:
    name_lsid: str
    citation_lsid: str
    full_data: dict[str, Any]


def get_zoobank_data_for_act(act: str) -> list[ZooBankData]:
    act = clean_lsid(act)
    try:
        api_response = json.loads(_get_zoobank_act_data(act))
    except json.JSONDecodeError as exc:
        # This can occur for an old cached human-verification page.
        raise ZooBankUnavailableError(
            f"cached ZooBank data for act {act!r} is not JSON"
        ) from exc
    return [
        ZooBankData(
            clean_lsid(data["protonymuuid"]),
            clean_lsid(data["OriginalReferenceUUID"]),
            data,
        )
        for data in api_response
    ]


def get_zoobank_data(original_name: str) -> list[ZooBankData]:
    try:
        api_response = json.loads(
            _get_zoobank_act_data(original_name.replace(" ", "_"))
        )
    except requests.RequestException, ZooBankUnavailableError, json.JSONDecodeError:
        return []
    api_response = [
        entry
        for entry in api_response
        if entry["tnuuuid"] == entry["protonymuuid"]
        and entry["namestring"] == original_name.rsplit(maxsplit=1)[-1]
    ]
    return [
        ZooBankData(
            clean_lsid(data["protonymuuid"]),
            clean_lsid(data["OriginalReferenceUUID"]),
            data,
        )
        for data in api_response
    ]


def get_zoobank_data_for_article(lsid: str) -> dict[str, Any]:
    try:
        ref_data = json.loads(_get_zoobank_publication_data(clean_lsid(lsid)))
    except json.JSONDecodeError as exc:
        # This can occur for an old cached human-verification page.
        raise ZooBankUnavailableError(
            f"cached ZooBank data for reference {lsid!r} is not JSON"
        ) from exc
    if len(ref_data) != 1:
        # Old 404 responses were cached as empty lists. Treat them as an
        # unavailable network lookup rather than a malformed Article.
        raise ZooBankUnavailableError(
            f"unexpected data for reference {lsid}: {ref_data}"
        )
    return ref_data[0]


def article_lsid_has_valid_data(lsid: str) -> bool:
    try:
        data = get_zoobank_data_for_article(lsid)
    except requests.RequestException, ZooBankUnavailableError:
        return False
    ref_uuid = data.get("referenceuuid", "")
    return clean_lsid(lsid) == clean_lsid(ref_uuid)
