import json
import re
from dataclasses import dataclass
from typing import Any

import requests

from taxonomy.db.url_cache import CacheDomain, cached

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
            r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$", lsid
        )
    )


rate_limiter = RateLimiter(min_interval=0.5)


@cached(CacheDomain.zoobank_act)
def _get_zoobank_act_data(query: str) -> str:
    rate_limiter.wait()
    url = f"https://zoobank.org/NomenclaturalActs.json/{query}"
    response = requests.get(url, timeout=1)
    if response.status_code == 404:
        return "[]"
    response.raise_for_status()
    return response.text


@cached(CacheDomain.zoobank_publication)
def _get_zoobank_publication_data(query: str) -> str:
    rate_limiter.wait()
    url = f"https://zoobank.org/References.json/{query}"
    response = requests.get(url, timeout=1)
    if response.status_code == 404:
        return "[]"
    response.raise_for_status()
    return response.text


@dataclass(frozen=True)
class ZooBankData:
    name_lsid: str
    citation_lsid: str
    full_data: dict[str, Any]


def get_zoobank_data_for_act(act: str) -> list[ZooBankData]:
    api_response = json.loads(_get_zoobank_act_data(act))
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
    except requests.ConnectionError:
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
    ref_data = json.loads(_get_zoobank_publication_data(clean_lsid(lsid)))
    if len(ref_data) != 1:
        raise ValueError(f"unexpected data for reference {lsid}: {ref_data}")
    return ref_data[0]


def article_lsid_has_valid_data(lsid: str) -> bool:
    try:
        data = get_zoobank_data_for_article(lsid)
    except requests.ConnectionError:
        return False
    ref_uuid = data.get("referenceuuid", "")
    return clean_lsid(lsid) == clean_lsid(ref_uuid)
